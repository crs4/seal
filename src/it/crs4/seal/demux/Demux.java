// Copyright (C) 2011-2012 CRS4.
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

package it.crs4.seal.demux;

import it.crs4.seal.common.ClusterUtils;
import it.crs4.seal.common.ContextAdapter;
import it.crs4.seal.common.FormatNameMap;
import it.crs4.seal.common.GroupByLocationComparator;
import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.SequenceId;
import it.crs4.seal.common.SequenceIdLocationPartitioner;
import it.crs4.seal.common.SealToolRunner;
import it.crs4.seal.demux.TwoOneThreeSortComparator;

import fi.tkk.ics.hadoop.bam.QseqInputFormat;
import fi.tkk.ics.hadoop.bam.SequencedFragment;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;


import java.net.URI;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Collection;

public class Demux extends Configured implements Tool
{
	private static final Log LOG = LogFactory.getLog(Demux.class);
	private static final String LocalSampleSheetName = "sample_sheet.csv";

	public static final String DEFAULT_PROJECT     = "DefaultProject";
	public static final int DEFAULT_MAX_MISMATCHES = 0;
	public static final String CONF_MAX_MISMATCHES = "seal.demux.max-mismatches";
	public static final String CONF_NO_INDEX_READS = "seal.demux.no-index";

	public static class Map extends Mapper<Text, SequencedFragment, SequenceId, SequencedFragment>
	{
		private DemuxMapper impl;
		private IMRContext<SequenceId,SequencedFragment> contextAdapter;

		@Override
		public void setup(Context context)
		{
			impl = new DemuxMapper();
			contextAdapter = new ContextAdapter<SequenceId,SequencedFragment>(context);
		}

		@Override
		public void map(Text qseqKey, SequencedFragment seq, Context context) throws java.io.IOException, InterruptedException
		{
			impl.map(qseqKey, seq, contextAdapter);
		}
	}

	public static class Red extends Reducer<SequenceId, SequencedFragment, Text, SequencedFragment>
	{
		private DemuxReducer impl;
		private IMRContext<Text,SequencedFragment> contextAdapter;

		@Override
		public void setup(Context context) throws IOException
		{
			impl = new DemuxReducer();
			impl.setup(new java.io.File(".", LocalSampleSheetName).getCanonicalPath(), context.getConfiguration());

			contextAdapter = new ContextAdapter<Text,SequencedFragment>(context);
			LOG.info("DemuxReducer setup.  Sample sheet loaded");
		}

		@Override
		public void reduce(SequenceId key, Iterable<SequencedFragment> values, Context context) throws IOException, InterruptedException
		{
			impl.reduce(key, values, contextAdapter);
		}
	}

	private String makeJobName(Path firstInputPath)
	{
		// TODO:  if the path is too long look at some smart way to trim the name
		return "Demux " + firstInputPath.toString();
	}

	private void distributeSampleSheet(Path sampleSheetPath) throws java.net.URISyntaxException
	{
		Configuration conf = getConf();
		DistributedCache.createSymlink(conf); // create symlinks in each task's working directory for the distributed files
		String distPath = sampleSheetPath.toString() + "#" + LocalSampleSheetName;
		DistributedCache.addCacheFile(new URI(distPath), conf);
	}

	private Writer makeLaneContentWriter(Path outputPath, String sampleName) throws IOException
	{
		Path destPath = new Path(outputPath, "LaneContent." + sampleName);
		FileSystem destFs = destPath.getFileSystem(getConf());
		OutputStream rawOut = destFs.create(destPath, true); // create and overwrite if it exists
		Writer out =
					new BufferedWriter(
							new OutputStreamWriter(rawOut));
		return out;
	}

	private void createLaneContentFiles(Path outputPath, Path sampleSheetPath) throws IOException
	{
		StringBuilder builder = new StringBuilder(100);

		try
		{
			Path qualifiedPath = sampleSheetPath.makeQualified(sampleSheetPath.getFileSystem(getConf()));
			SampleSheet sheet = DemuxUtils.loadSampleSheet(qualifiedPath, getConf());
			Collection<String> samples = sheet.getSamples();
			// we have one output directory per sample, thus we need one LaneContent file per sample.
			for (String sample: samples)
			{
				Writer out = makeLaneContentWriter(outputPath, sample);
				try
				{
					for (int lane = 1; lane <= 8; ++lane)
					{
						builder.delete(0, builder.length());
						builder.append(lane-1).append(":");
						if (sheet.getSamplesInLane(lane).contains(sample))
							builder.append(sample);
						builder.append("\n");
						out.write(builder.toString());
					}
				}
				finally {
					out.close();
				}
			}
		}
		catch (SampleSheet.FormatException e) {
			throw new RuntimeException("Error in sample sheet.  " + e.getMessage());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		LOG.info("starting");

		Configuration conf = getConf();
		DemuxOptionParser parser = new DemuxOptionParser();
		parser.parse(conf, args);

		conf.setBoolean(CONF_NO_INDEX_READS, parser.getNoIndexReads());

		LOG.info("Using " + parser.getNReduceTasks() + " reduce tasks");
		if (parser.getNoIndexReads())
			LOG.info("Not expecting to find any index reads.  Will demultiplex based only on lane.");

		// load sample sheet to fail early in case of problems
		DemuxUtils.loadSampleSheet(parser.getSampleSheetPath(), conf);

		// must be called before creating the job, since the job
		// *copies* the Configuration.
		distributeSampleSheet(parser.getSampleSheetPath());

		// Create a Job using the processed conf
		Job job = new Job(getConf(), makeJobName(parser.getInputPaths().get(0)));

		job.setJarByClass(Demux.class);

		// input paths
		for (Path p: parser.getInputPaths())
			FileInputFormat.addInputPath(job, p);

		job.setInputFormatClass(FormatNameMap.getInputFormat(parser.getInputFormatName("qseq")));

		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(SequenceId.class);
		job.setMapOutputValueClass(SequencedFragment.class);

		job.setPartitionerClass(SequenceIdLocationPartitioner.class);
		job.setGroupingComparatorClass(GroupByLocationComparator.class);
		job.setSortComparatorClass(TwoOneThreeSortComparator.class);

		job.setReducerClass(Red.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SequencedFragment.class);

		// output
		job.setOutputFormatClass(DemuxOutputFormat.class);
		FileOutputFormat.setOutputPath(job, parser.getOutputPath());

		// Submit the job, then poll for progress until the job is complete
		boolean result = job.waitForCompletion(true);
		if (result)
		{
			LOG.info("done");
			if (parser.getCreateLaneContent())
				createLaneContentFiles(parser.getOutputPath(), parser.getSampleSheetPath());
			return 0;
		}
		else
		{
			LOG.fatal(this.getClass().getName() + " failed!");
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		int res = new SealToolRunner().run(new Demux(), args);
		System.exit(res);
	}
}
