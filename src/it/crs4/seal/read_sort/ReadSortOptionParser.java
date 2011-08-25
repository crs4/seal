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

import it.crs4.seal.common.SealToolParser;
import it.crs4.seal.common.ClusterUtils;

import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.commons.cli.*;

public class ReadSortOptionParser {

	public static final String ConfigSection = "ReadSort";
	public static final int DEFAULT_RED_TASKS_PER_NODE = 3;

	private SealToolParser parser;

	private Options options; // for custom options
	private Option ann;
	private Option distReference;

	public ReadSortOptionParser() 
	{
		// define the custom options
		options = new Options();

		ann = OptionBuilder
						.withDescription("annotation file (.ann) of the BWA reference used to create the SAM data")
						.hasArg()
						.withArgName("ref.ann")
						.withLongOpt("annotations")
						.create("ann");
		options.addOption(ann);

		distReference = OptionBuilder
		                .withDescription("BWA reference on HDFS used to create the SAM data, to be distributed by DistributedCache")
			             .hasArg()
			             .withArgName("archive")
			             .withLongOpt("distributed-reference")
			             .create("distref");
		options.addOption(distReference);

		////
		parser = new SealToolParser(ConfigSection, options);
	}


	public void parse(Configuration conf, String[] args) throws IOException
	{
		try
	 	{
			CommandLine line = parser.parseOptions(conf, args);

			// if a number of reducers was specified make sure it's > 0
			if (parser.getNReducers() != null)
			{
				if (parser.getNReducers() <= 0)
					throw new ParseException("Number of reduce tasks, when specified, must be > 0");
			}
			else // number of reduce tasks not specified
			{
				conf.set(ClusterUtils.NUM_RED_TASKS_PROPERTY, 
						String.valueOf(DEFAULT_RED_TASKS_PER_NODE * ClusterUtils.getNumberTaskTrackers(conf)));
			}
			/********* distributed reference and annotations *********/
			if (line.hasOption(distReference.getOpt()))
			{
				// Distribute the reference archive, and create a // symlink "reference" to the directory
				Path optPath = new Path(line.getOptionValue(distReference.getOpt()));
				optPath = optPath.makeQualified(optPath.getFileSystem(conf));
				Path cachePath = new Path(optPath.toString() + "#reference");
				conf.set("mapred.cache.archives", cachePath.toString());
				conf.set("mapred.create.symlink", "yes");

				if (line.hasOption(ann.getOpt()))
					conf.set(ReadSort.REF_ANN_PROP_NAME, "reference/" + line.getOptionValue(ann.getOpt()));
				else
					throw new ParseException("You must specify the name of the annotation file within the distributed reference archive with -" + ann.getOpt());
			}
			else if (line.hasOption(ann.getOpt()))
			{
				// direct access to the reference annotation
				conf.set(ReadSort.REF_ANN_PROP_NAME, line.getOptionValue(ann.getOpt()));
			}
			else
				throw new ParseException("You must provide the path the reference annotation file (<ref>.ann)");
		}
		catch( ParseException e ) 
		{
			parser.defaultUsageError("it.crs4.seal.read_sort.ReadSort", e.getMessage()); // doesn't return
		}
	}

	public ArrayList<Path> getInputPaths() 
	{
		ArrayList<Path> retval = new ArrayList<Path>(parser.getNumInputPaths());
		for (Path p: parser.getInputPaths())
			retval.add(p);

		return retval;
	}

	public Path getOutputPath() { return parser.getOutputPath(); }
}
