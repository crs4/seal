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

package it.crs4.seal.prq;

import it.crs4.seal.common.SealToolParser;
import it.crs4.seal.common.ClusterUtils;

import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.commons.cli.*;

public class PrqOptionParser {

	public static final String ConfigSection = "Prq";
	public static final int DEFAULT_RED_TASKS_PER_NODE = 3;

	public static final int DefaultMinBasesThreshold = 30;
	public static final String MinBasesThresholdConfigName = "bl.prq.min-bases-per-read";

	public static final boolean DropFailedFilterDefault = true;
	public static final String DropFailedFilterConfigName = "bl.prq.drop-failed-filter";

	public static final boolean WarningOnlyIfUnpairedDefault = false;
	public static final String WarningOnlyIfUnpairedConfigName = "bl.prq.warning-only-if-unpaired";

	public enum InputFormat {
		qseq,
		fastq
	};

	public static final String InputFormatDefault = InputFormat.qseq.toString();
	public static final String InputFormatConfigName = "bl.prq.input-format";

	private InputFormat inputFormat;

	private SealToolParser parser;

	public PrqOptionParser() 
	{
		parser = new SealToolParser(ConfigSection, null);
		parser.setMinReduceTasks(1);
	}

	public void parse(Configuration conf, String[] args) throws IOException
	{
		conf.setInt(MinBasesThresholdConfigName, DefaultMinBasesThreshold);
		conf.setBoolean(DropFailedFilterConfigName, DropFailedFilterDefault);
		conf.setBoolean(WarningOnlyIfUnpairedConfigName, WarningOnlyIfUnpairedDefault);
		conf.set(InputFormatConfigName, InputFormatDefault);

		try
	 	{
			CommandLine line = parser.parseOptions(conf, args);

			if (parser.getNReduceTasks() == null) // number of reduce tasks not specified
			{
				conf.set(ClusterUtils.NUM_RED_TASKS_PROPERTY, 
						String.valueOf(DEFAULT_RED_TASKS_PER_NODE * ClusterUtils.getNumberTaskTrackers(conf)));
			}
			
			String input = conf.get(InputFormatConfigName);
			try {
				inputFormat = Enum.valueOf(InputFormat.class, input); // throws IllegalArgumentException 
			}
			catch (IllegalArgumentException e) {
				throw new ParseException("Unknown input format name " + input + ". Try 'qseq' or 'fastq'");
			}
		}
		catch( ParseException e ) 
		{
			parser.defaultUsageError("it.crs4.seal.prq.PairReadsQSeq", e.getMessage()); // doesn't return
		}
	}

	public InputFormat getSelectedInputFormat() { return inputFormat; }

	public ArrayList<Path> getInputPaths() 
	{
		ArrayList<Path> retval = new ArrayList<Path>(parser.getNumInputPaths());
		for (Path p: parser.getInputPaths())
			retval.add(p);

		return retval;
	}

	public Path getOutputPath() { return parser.getOutputPath(); }
}
