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

	public static final String ConfigSection = "PRQ";
	public static final int DEFAULT_RED_TASKS_PER_NODE = 3;

	private SealToolParser parser;

	public PrqOptionParser() 
	{
		parser = new SealToolParser(ConfigSection, null);
		parser.setMinReduceTasks(1);
	}

	public void parse(Configuration conf, String[] args) throws IOException
	{
		try
	 	{
			CommandLine line = parser.parseOptions(conf, args);

			if (parser.getNReduceTasks() == null) // number of reduce tasks not specified
			{
				conf.set(ClusterUtils.NUM_RED_TASKS_PROPERTY, 
						String.valueOf(DEFAULT_RED_TASKS_PER_NODE * ClusterUtils.getNumberTaskTrackers(conf)));
			}
		}
		catch( ParseException e ) 
		{
			parser.defaultUsageError("it.crs4.seal.prq.PairReadsQSeq", e.getMessage()); // doesn't return
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
