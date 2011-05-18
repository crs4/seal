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

import it.crs4.seal.common.SealToolParser;

import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.commons.cli.*;

public class DemuxOptionParser {

	private SealToolParser parser;
	private Options demuxOptions;
	private Option sampleSheetOpt;
	private Path sampleSheetPath;

	public DemuxOptionParser() 
	{
		// define the options
		demuxOptions = new Options();

		sampleSheetOpt = OptionBuilder
											.withDescription("Sample sheet for the experiment")
											.hasArg()
											.withArgName("FILE")
											.withLongOpt("sample-sheet")
											.create("s");
		demuxOptions.addOption(sampleSheetOpt);
		parser = new SealToolParser(demuxOptions);
	}

	public void parse(Configuration conf, String[] args) throws IOException
	{
		try
	 	{
			CommandLine line = parser.parseOptions(conf, args);
			// options
			if (line.hasOption( sampleSheetOpt.getOpt() ))
			{
				sampleSheetPath = new Path(line.getOptionValue(sampleSheetOpt.getOpt()));
				if (sampleSheetPath.getFileSystem(conf).exists(sampleSheetPath))
					sampleSheetPath = sampleSheetPath.makeQualified(sampleSheetPath.getFileSystem(conf));
				else
					throw new ParseException("Sample sheet " + sampleSheetPath.toString() + " doesn't exist");
			}
			else
				throw new ParseException("Missing " + sampleSheetOpt.getLongOpt() + " argument");
		}
		catch( ParseException e ) 
		{
			parser.defaultUsageError(demuxOptions, e.getMessage()); // doesn't return
		}
	}

	public Path getOutputPath() 
	{
		return parser.getOutputPath();
	}

	public ArrayList<Path> getInputPaths() 
	{
		ArrayList<Path> retval = new ArrayList<Path>(parser.getNumInputPaths());
		for (Path p: parser.getInputPaths())
			retval.add(p);

		return retval;
	}

	public Path getSampleSheetPath()
	{
		return sampleSheetPath;
	}
}
