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

package it.crs4.seal.recab;

import it.crs4.seal.common.ClusterUtils;
import it.crs4.seal.common.ContextAdapter;
import it.crs4.seal.common.IMRContext;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.ToolRunner;


import java.net.URI;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Collection;

public class RecabTable extends Configured implements Tool 
{
	private static final Log LOG = LogFactory.getLog(RecabTable.class);

	public static final int DEFAULT_RED_TASKS_PER_TRACKER = 3;

	public static final String LocalVariantsFile = "variants_table.data";

	public static final String VariantsFileTypeProperty = "seal.recab.variants-table-type";
	public static final String VariantsFileTypeVcf = "vcf";
	public static final String VariantsFileTypeRod = "rod";


	// Whether to only consider SNP variation locations or to consider all variation types.
	public static final String SnpsOnlyProperty = "seal.recab.snps-only";
	// Default:  consider only SNPs
	public static final boolean SnpsOnlyDefault = true;

	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{
		private RecabTableMapper impl;
		private IMRContext<Text,Text> contextAdapter;

		@Override
		public void setup(Context context) throws IOException
		{
			impl = new RecabTableMapper();
			contextAdapter = new ContextAdapter<Text,Text>(context);

			Configuration conf = context.getConfiguration();

			// known variation sites
			Reader in = new FileReader(LocalVariantsFile);
			SnpReader reader;
			if (conf.get(VariantsFileTypeProperty).equals(VariantsFileTypeVcf))
			{
				VcfSnpReader vcfReader = new VcfSnpReader(in);
				vcfReader.setReadSnpsOnly(conf.getBoolean(SnpsOnlyProperty, SnpsOnlyDefault));
				reader = vcfReader;
			}
			else if (conf.get(VariantsFileTypeProperty).equals(VariantsFileTypeRod))
			{
				reader = new RodFileSnpReader(in);
				if (!conf.getBoolean(SnpsOnlyProperty, SnpsOnlyDefault))
					throw new RuntimeException("Sorry.  Using all variat types is currently not supported for Rod files.  Please let the Seal developers know if this is important to you.");
			}

			setup(reader);
		}

		@Override
		public void map(LongWritable pos, Text sam, Context context) throws java.io.IOException, InterruptedException
		{
			impl.map(pos, sam, contextAdapter);
		}
	}

	public static class Red extends Reducer<Text, Text, Text, Text>
	{
		//private DemuxReducer impl;
		//private IMRContext<Text,Text> contextAdapter;

		@Override
		public void setup(Context context) throws IOException
		{
			//impl = new DemuxReducer();
			//impl.setup(LocalSampleSheetName, context.getConfiguration());

			//contextAdapter = new ContextAdapter<Text,Text>(context);
			//LOG.info("DemuxReducer setup.  Sample sheet loaded");
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			//impl.reduce(key, values, contextAdapter);
		}
	}


	private void distributeVariantsFile(RecabTableOptionParser parser)
	{
		Configuration conf = getConf();
		DistributedCache.createSymlink(conf); // create symlinks in each task's working directory for the distributed files

		String distPath;
		String variantsFileType;

		if (parser.getVcfFile() != null)
		{
			variantsFileType = VariantsFileTypeVcf;
			distPath = parser.getVcfFile().toString();
		}
		else if (parser.getRodFile() != null)
		{
			variantsFileType = VariantsFileTypeRod;
			distPath = parser.getRodFile().toString();
		}
		else
			throw new RuntimeException("BUG!! RecabTableOptionParser defined with getRodFile and getVcfFile both null!");

		distPath += "#" + LocalVariantsFile;
		DistributedCache.addCacheFile(new URI(distPath), conf);
	}

	@Override
	public int run(String[] args) throws Exception {
		LOG.info("starting");

		Configuration conf = getConf();
		RecabTableOptionParser parser = new RecabTableOptionParser();
		parser.parse(conf, args);

		// must be called before creating the job, since the job
		// *copies* the Configuration.
		distributeVariantsFile(parser);

		// Create a Job using the processed conf
		Job job = new Job(getConf(), "RecabTable " + parser.getInputPaths().get(0));

		job.setJarByClass(RecabTable.class);

		// input paths
		for (Path p: parser.getInputPaths())
			FileInputFormat.addInputPath(job, p);

		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(Red.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// output
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
		int res = ToolRunner.run(new RecabTable(), args);
		System.exit(res);
	}
}
