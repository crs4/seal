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

package it.crs4.seal.demux;

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.ContextAdapter;
import it.crs4.seal.common.SequenceId;
import it.crs4.seal.common.GroupByLocationComparator;
import it.crs4.seal.common.SequenceIdLocationPartitioner;
import it.crs4.seal.demux.TwoOneThreeSortComparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.cli.*;

import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;


import java.net.URI;
import java.io.IOException;

public class Demux extends Configured implements Tool 
{
	private static final Log LOG = LogFactory.getLog(Demux.class);
	private static final String LocalSampleSheetName = "sample_sheet.csv";

	public Demux() {
		super();
	}

	public static class Map extends Mapper<LongWritable, Text, SequenceId, Text> 
	{
		private DemuxMapper impl;
		private IMRContext<SequenceId,Text> contextAdapter;

		@Override
		public void setup(Context context)
		{
			impl = new DemuxMapper();
			contextAdapter = new ContextAdapter<SequenceId,Text>(context);
		}

		@Override
		public void map(LongWritable pos, Text qseq, Context context) throws java.io.IOException, InterruptedException
		{
			impl.map(pos, qseq, contextAdapter);
		}
	}

	public static class Red extends Reducer<SequenceId, Text, Text, Text>
	{
		private DemuxReducer impl;
		private IMRContext<Text,Text> contextAdapter;

		@Override
		public void setup(Context context) throws IOException
		{
			impl = new DemuxReducer();
			impl.setup(LocalSampleSheetName, context.getConfiguration());

			contextAdapter = new ContextAdapter<Text,Text>(context);
			LOG.info("DemuxReducer setup.  Sample sheet loaded");
		}

		@Override
		public void reduce(SequenceId key, Iterable<Text> values, Context context) throws IOException, InterruptedException
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

	@Override
	public int run(String[] args) throws Exception {
		LOG.info("starting");

		DemuxOptionParser parser = new DemuxOptionParser();
		parser.parse(getConf(), args);

		// must be called before creating the job, since the job
		// *copies* the Configuration.
		distributeSampleSheet(parser.getSampleSheetPath());

		// Create a Job using the processed conf
		Job job = new Job(getConf(), makeJobName(parser.getInputPaths().get(0)));

		job.setJarByClass(Demux.class);

		// input paths
		for (Path p: parser.getInputPaths())
			FileInputFormat.addInputPath(job, p);

		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(SequenceId.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(SequenceIdLocationPartitioner.class);
		job.setGroupingComparatorClass(GroupByLocationComparator.class);
		job.setSortComparatorClass(TwoOneThreeSortComparator.class);

		job.setReducerClass(Red.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// output
		job.setOutputFormatClass(DemuxOutputFormat.class);
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
			LOG.fatal(this.getClass().getName() + " failed!");
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Demux(), args);
		System.exit(res);
	}
}
