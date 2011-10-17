// Copyright (C) 2011 CRS4.
// 
// This file is part of Seal.
// 
// Seal is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
// 
// Seal is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
// for more details.
// 
// You should have received a copy of the GNU General Public License along
// with Seal.  If not, see <http://www.gnu.org/licenses/>.

package it.crs4.seal.read_sort;
import it.crs4.seal.common.BwaRefAnnotation;
import it.crs4.seal.common.FormatException;
import it.crs4.seal.common.ClusterUtils;
import it.crs4.seal.common.SealToolRunner;

import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.cli.*;

import java.net.URI;
import java.net.InetSocketAddress;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReadSort extends Configured implements Tool {

	public static final String REF_ANN_PROP_NAME = "readsort.reference.ann";

	private static final Log LOG = LogFactory.getLog(ReadSort.class);

	public static Path getAnnotationPath(Configuration conf) throws IOException
	{
		String annotationName = conf.get(ReadSort.REF_ANN_PROP_NAME);
		if (annotationName == null)
			throw new RuntimeException("missing property " + REF_ANN_PROP_NAME);

		LOG.info("reading reference annotation from " + annotationName);

		Path annPath = new Path(annotationName);

		FileSystem srcFs;
		if (conf.get("mapred.cache.archives") != null)
		{
			// we're using the distributed cache for the reference,
			// so it's on the local file system
			srcFs = FileSystem.getLocal(conf);
		}
		else
			srcFs = annPath.getFileSystem(conf);

		return annPath.makeQualified(srcFs);
	}

	public static class ReadSortSamMapper extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		private static final String delim = "\t";
		private BwaRefAnnotation annotation;
		private LongWritable outputKey;

		@Override
		public void setup(Context context) throws IOException, FormatException
		{
			Configuration conf = context.getConfiguration();
			Path annPath = getAnnotationPath(conf);

			FSDataInputStream in = annPath.getFileSystem(conf).open(annPath);
			annotation = new BwaRefAnnotation(new InputStreamReader(in));
			LOG.info("ReadSortSamMapper successfully read reference annotations");
			in.close();

			outputKey = new LongWritable();
		}

		/**
		 * Map (xx, SAM record) to (absolute coordinates, SAM record).
		 */
		@Override
		public void map(LongWritable ignored, Text sam, Context context) throws IOException, InterruptedException
		{
			try
			{
				int pos = 0;
				for (int i = 1; i <= 2; ++i)
					pos = sam.find(delim, pos) + 1; // +1 since we get the position of the delimiter
				int seq_pos = pos;
				int coord_pos = sam.find(delim, pos) + 1;
				int coord_end = sam.find(delim, coord_pos); // pos of coordinate delimiter

				if (seq_pos <= 0 || coord_pos <= 0 || coord_end <= 0)
					throw new RuntimeException("Invalid SAM record: " + sam.toString());

				String seq_name = Text.decode(sam.getBytes(), seq_pos, coord_pos - seq_pos - 1);
				if (seq_name.equals("*"))
					outputKey.set(Long.MAX_VALUE); // unmapped read.  Send it to the end
				else
				{
					long coord = Long.parseLong( Text.decode(sam.getBytes(), coord_pos, coord_end - coord_pos) );
					outputKey.set(annotation.getAbsCoord(seq_name, coord));
				}

				context.write( outputKey, sam);
			}
			catch (java.nio.charset.CharacterCodingException e)
			{
				throw new RuntimeException("Character coding error in SAM: " + e.getMessage());
			}
		}
	}

	/**
	 * Partition the input reads assuming that they cover the entire reference uniformly.
	 * This partitioner needs to know the reference length, and then divides it into
	 * regions of equal length.
	 */
	public static class WholeReferencePartitioner extends Partitioner<LongWritable, Text> implements Configurable {
		private long partitionSize;
		private long referenceSize;
		Configuration conf;

		public WholeReferencePartitioner() 
		{
			partitionSize = 0;
			referenceSize = 0;
			conf = null;
		}

		@Override
		public void setConf(Configuration c)
		{
			conf = c;

			/* Read the reference annotation from the file provided in REF_ANN_PROP_NAME.
			 * The file can be on a mounted filesystem or HDFS, but it has to be accessible
			 * from every node.
			 */
			FSDataInputStream in = null;
			Path annPath = null;

			try {
				annPath = getAnnotationPath(conf);
				System.err.println("WholeReferencePartitioner: annotation path: " + annPath);
			}
			catch (IOException e) {
				throw new RuntimeException("WholeReferencePartitioner:  error getting annotation file path. " + e.getMessage());
			}

			try {
				in = annPath.getFileSystem(conf).open(annPath);

				BwaRefAnnotation annotation = new BwaRefAnnotation(new InputStreamReader(in));
				LOG.info("Partitioner successfully read reference annotations");

				referenceSize = annotation.getReferenceLength();
				if (referenceSize <= 0)
					throw new RuntimeException("WholeReferencePartitioner could not get reference length.");
				int nReducers = conf.getInt(ClusterUtils.NUM_RED_TASKS_PROPERTY, 1);
				if (nReducers == 1)
				{
					partitionSize = referenceSize;
				}
				else if (nReducers >= 2)
				{
					// leave one reducer for the unmapped reads
					partitionSize = (long)Math.ceil( referenceSize / ((double)nReducers - 1));
					if (LOG.isInfoEnabled())
						LOG.info("Reference size: " + referenceSize + "; n reducers: " + nReducers + ". Set partition size to " + partitionSize);
				}
				else
					throw new RuntimeException("Negative number of reducers (" + nReducers + ")");
			}
			catch (IOException e) {
				// We can't throw IOException since it's not in the setConf specification.
				String msg = "WholeReferencePartitioner: error reading BWA annotation. " + e.getMessage();
				if (annPath.toString().startsWith("hdfs://"))
					msg += " Maybe you forgot to specify 'file://' for a local path?";
				throw new RuntimeException(msg);
			}
			finally
			{
				if (in != null)
				{
					try {
						in.close();
					}
				 	catch (IOException e) {
						LOG.warn("Error closing annotations file. Message: " + e.getMessage());
					}
				}
			}
		}

		@Override
		public Configuration getConf()
		{
			return conf;
		}

		@Override
		public int getPartition(LongWritable key, Text value, int numPartitions) 
		{
			if (conf == null)
				throw new RuntimeException("WholeReferencePartitioner isn't configured!");
			if (partitionSize <= 0)
				throw new RuntimeException("WholeReferencePartitioner can't partition with partitionSize " + partitionSize);
			
			if (numPartitions == 1 || key.get() == Long.MAX_VALUE)
			{ 
				// If we only have one partition, obviously we return partition 0.
				// Otherwise, reserve the last partition for the unmapped reads.
				return numPartitions - 1;
			}
			else
			{
				int partition = (int)( (key.get() - 1) / partitionSize); // the key coordinate starts at 1
				if (partition == numPartitions - 1) // the last partition is reserved for unmapped reads. Something went wrong.
				{
					throw new RuntimeException("WholeReferencePartitioner: partition index too big! referenceSize: " + referenceSize + 
							"; key: " + key +
						 	"; partitionSize: " + partitionSize +
						 	"; numPartitions: " + numPartitions +
						 	"; partition: " + partition);
				}

				return partition;
			}
		}
	}

	public static class ReadSortSamReducer extends Reducer<LongWritable, Text, Text, Text>
	{
		private Text outputValue = new Text();

		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException 
		{
			// We can get more than one read per position.  We could sort them by name, but
			// it's probably not worth the effort.
			for (Text record: values)
			{
				int delim_pos = record.find("\t");
				// copy the part after the name field to outputValue
				outputValue.clear();
				outputValue.append(record.getBytes(), delim_pos+1, record.getLength() - delim_pos - 1);
				// XXX: now set record to itself, truncating the part from delim_pos onwards (make sure this works!)
				record.set(record.getBytes(), 0, delim_pos);

				// the default output formatter joins key and value with a tab.
				context.write(record, outputValue);
			}
		}
	}

	private String makeJobName(Path firstInputPath)
	{
		// TODO:  if the path is too long look at some smart way to trim the name
		return "ReadSort " + firstInputPath.toString();
	}

	public int run(String[] args) throws Exception {
		LOG.info("starting");

		Configuration conf = getConf();

		ReadSortOptionParser parser = new ReadSortOptionParser();
		parser.parse(conf, args);

		// Create a Job using the processed conf
		Job job = new Job(conf, makeJobName(parser.getInputPaths().get(0)));
		job.setJarByClass(ReadSort.class);

		// input paths
		for (Path p: parser.getInputPaths())
			FileInputFormat.addInputPath(job, p);

		job.setMapperClass(ReadSortSamMapper .class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(WholeReferencePartitioner.class);

		job.setReducerClass(ReadSortSamReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// output path
		FileOutputFormat.setOutputPath(job, parser.getOutputPath());

		// Submit the job, then poll for progress until the job is complete
		boolean result = job.waitForCompletion(true);
		if (result)
		{
			LOG.info("done");
			return 0;
		}
		else
		{
			LOG.fatal("ReadSort failed!");
			return 1;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = new SealToolRunner().run(new ReadSort(), args);
		System.exit(res);
	}
}
