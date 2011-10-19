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

import it.crs4.seal.common.SealToolParser;

import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.commons.cli.*;

public class RecabTableOptionParser {

	public static final int DEFAULT_N_REDUCERS = 1;

	private SealToolParser parser;
	private Options options;

	public RecabTableOptionParser() 
	{
		// define the options
		options = new Options();
		parser = new SealToolParser(options);
	}

	public void parse(Configuration conf, String[] args) throws IOException
	{
		try
	 	{
			CommandLine line = parser.parseOptions(conf, args);

			if (parser.getNReducers() != null)
			{
				int r = parser.getNReducers();
				if (r <= 0)
					throw new ParseException("Number of reducers, when specified, must be > 0");
			}
		}
		catch( ParseException e ) 
		{
			parser.defaultUsageError("it.crs4.seal.recab.RecabTable", e.getMessage()); // doesn't return
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
	public boolean isNReducersSpecified() { return parser.getNReducers() != null; }
	public int getNReducers()
 	{ 
		if (parser.getNReducers() == null) 
			return DEFAULT_N_REDUCERS;
		else
			return parser.getNReducers();
 	}
}
