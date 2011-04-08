// Copyright (C) 2011 CRS4.
// 
// This file is part of ReadSort.
// 
// ReadSort is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
// 
// ReadSort is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
// for more details.
// 
// You should have received a copy of the GNU General Public License along
// with ReadSort.  If not, see <http://www.gnu.org/licenses/>.

package it.crs4.mr.read_sort;
import it.crs4.mr.read_sort.BwaRefAnnotation;

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
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadSort extends Configured implements Tool {

	public static final String REF_ANN_PROP_NAME = "readsort.reference.ann";
	public static final String INPUT_PROP_NAME = "readsort.input.path";
	public static final String OUTPUT_PROP_NAME = "readsort.output.path";
	public static final String NUM_RED_TASKS_PROPERTY = "mapred.reduce.tasks"; // XXX: this changes depending on Hadoop version
	public static final int DEFAULT_RED_TASKS_PER_NODE = 3;

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

		@Override
		public void setup(Context context) throws IOException, BwaRefAnnotation.InvalidAnnotationFormatException
		{
			Configuration conf = context.getConfiguration();
			Path annPath = getAnnotationPath(conf);

			FSDataInputStream in = annPath.getFileSystem(conf).open(annPath);
			annotation = new BwaRefAnnotation(new InputStreamReader(in));
			LOG.info("ReadSortSamMapper successfully read reference annotations");
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
				long coord = Long.parseLong( Text.decode(sam.getBytes(), coord_pos, coord_end - coord_pos) ); 

				context.write( new LongWritable( annotation.getAbsCoord(seq_name, coord)), sam);
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

			try
			{
				Path annPath = getAnnotationPath(conf);
				System.err.println("WholeReferencePartitioner: annotation path: " + annPath);
				in = annPath.getFileSystem(conf).open(annPath);

				BwaRefAnnotation annotation = new BwaRefAnnotation(new InputStreamReader(in));
				LOG.info("Partitioner successfully read reference annotations");

				referenceSize = annotation.getReferenceLength();
				if (referenceSize <= 0)
					throw new RuntimeException("WholeReferencePartitioner could not get reference length.");
				int nReducers = conf.getInt(NUM_RED_TASKS_PROPERTY, 0);
				if (nReducers > 0)
				{
					partitionSize = (long)Math.ceil(referenceSize / (double)nReducers);
					if (LOG.isInfoEnabled())
						LOG.info("Reference size: " + referenceSize + "; n reducers: " + nReducers + ". Set partition size to " + partitionSize);
				}
			}
			catch (IOException e)
			{
				// We can't throw IOException since it's not in the setConf specification.
				throw new RuntimeException("WholeReferencePartitioner: error reading BWA annotation. " + e.getMessage());
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
			/* XXX: for debugging */
			if (conf == null)
				throw new RuntimeException("WholeReferencePartitioner isn't configured!");
			if (partitionSize <= 0)
				throw new RuntimeException("WholeReferencePartitioner can't partition with partitionSize " + partitionSize);
			
			int partition = (int)(key.get() / partitionSize);
			if (partition == numPartitions)
				partition = partition - 1;
			else if (partition > numPartitions)
				throw new RuntimeException("WholeReferencePartitioner: partition index too big! referenceSize: " + referenceSize + "; key: " + key + "; partitionSize: " + partitionSize);

			return partition;
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

	public int getDefaultNumberReduceTasks() throws IOException
	{
		/* XXX hack to get the ClusterStatus.  To use JobClient it seems I need to
		 * wrap the Configuration with the deprecated JobConf.
		 * Is there a better way?
		 */ 
		ClusterStatus status = (new JobClient(new JobConf(getConf()))).getClusterStatus();
		return status.getTaskTrackers();
	}

	/**
	 * Scan command line and set configuration values appropriately.
	 * Calls System.exit in case of a command line error.
	 */
	private void scanOptions(String[] args)
	{
		Options options = new Options();

		Option reducers = OptionBuilder
			              .withDescription("Number of reduce tasks to use.")
			              .hasArg()
			              .withArgName("INT")
										.withLongOpt("reducers")
			              .create("r");
		options.addOption(reducers);


		Option ann = OptionBuilder
			              .withDescription("annotation file (.ann) of the BWA reference used to create the SAM data")
			              .hasArg()
			              .withArgName("ref.ann")
										.withLongOpt("annotations")
			              .create("ann");
		options.addOption(ann);

		Option distReference = OptionBuilder
		                .withDescription("BWA reference on HDFS used to create the SAM data, to be distributed by DistributedCache")
			             .hasArg()
			             .withArgName("archive")
			             .withLongOpt("distributed-reference")
			             .create("distref");
		options.addOption(distReference);

		CommandLineParser parser = new GnuParser();
		Configuration conf = getConf();

		try {
			CommandLine line = parser.parse( options, args );

			/********* Number of reduce tasks *********/
			if (line.hasOption("r"))
			{
				String rString = line.getOptionValue(reducers.getOpt());
				try
				{
					int r = Integer.parseInt(rString);
					if (r > 0)
						conf.set(NUM_RED_TASKS_PROPERTY, String.valueOf(r));
					else
						throw new ParseException("Number of reducers must be greater than 0 (got " + rString + ")");
				}
				catch (NumberFormatException e)
				{
					throw new ParseException("Invalid number of reducers '" + rString + "'");
				}
			}
			else
				conf.set(NUM_RED_TASKS_PROPERTY, String.valueOf(DEFAULT_RED_TASKS_PER_NODE * getDefaultNumberReduceTasks()));

			/********* distributed reference and annotations *********/
			if (line.hasOption("distref"))
			{
				// Distribute the reference archive, and create a // symlink "reference" to the directory
				Path optPath = new Path(line.getOptionValue(distReference.getOpt()));
				optPath = optPath.makeQualified(optPath.getFileSystem(conf));
				Path cachePath = new Path(optPath.toString() + "#reference");
				conf.set("mapred.cache.archives", cachePath.toString());
				conf.set("mapred.create.symlink", "yes");

				if (line.hasOption(ann.getOpt()))
					conf.set(REF_ANN_PROP_NAME, "reference/" + line.getOptionValue(ann.getOpt()));
				else
					throw new ParseException("You must specify the name of the annotation file within the distributed reference archive with -" + ann.getOpt());
			}
			else if (line.hasOption(ann.getOpt()))
			{
				// direct access to the reference annotation
				conf.set(REF_ANN_PROP_NAME, line.getOptionValue(ann.getOpt()));
			}
			else
				throw new ParseException("You must provide the path the reference annotation file (<ref>.ann)");

			// remaining args
			String[] otherArgs = line.getArgs();
			if (otherArgs.length == 2) 
			{
				Path inputDir = new Path(otherArgs[0]);
				inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf));
				conf.set(INPUT_PROP_NAME, inputDir.toString());

				Path outputDir = new Path(otherArgs[1]);
				outputDir = outputDir.makeQualified(outputDir.getFileSystem(conf));
				conf.set(OUTPUT_PROP_NAME, outputDir.toString());
			}
			else
				throw new ParseException("You must provide HDFS input and output paths");
		}
		catch(IOException e)
		{
			LOG.fatal( e.getMessage() );
			System.exit(1);
		}
		catch( ParseException e ) 
		{
			LOG.fatal("Usage error");
			LOG.fatal( e.getMessage() );
			// XXX: redirect System.out to System.err since the simple version of 
			// HelpFormatter.printHelp prints to System.out, and we're on a way to
			// a fatal exit.
			System.setOut(System.err);
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "hadoop jar ReadSort [options] <in> <out>", options);
			System.exit(1);
		}
	}

	public int run(String[] otherArgs) throws Exception {
		LOG.info("starting");

		// Configuration processed by ToolRunner
		Configuration conf = getConf();

		scanOptions(otherArgs);

		// Create a Job using the processed conf
		Job job = new Job(conf, "ReadSort " + conf.get(INPUT_PROP_NAME));
		job.setJarByClass(ReadSort.class);

		// input path
		Path inputDir = new Path(conf.get(INPUT_PROP_NAME));
		FileInputFormat.setInputPaths(job, inputDir);

		job.setMapperClass(ReadSortSamMapper .class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(WholeReferencePartitioner.class);

		job.setReducerClass(ReadSortSamReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// output path
		Path outputDir = new Path(conf.get(OUTPUT_PROP_NAME));
		FileOutputFormat.setOutputPath(job, outputDir);

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
		int res = ToolRunner.run(new ReadSort(), args);
		System.exit(res);
	}
}
