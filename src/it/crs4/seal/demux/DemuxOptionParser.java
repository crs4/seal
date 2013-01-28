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
import it.crs4.seal.common.SealToolParser;

import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.commons.cli.*;

public class DemuxOptionParser extends SealToolParser {

	public static final String ConfigSection = "Demux";

	private Option sampleSheetOpt;
	private Path sampleSheetPath;

	private Option laneContentOpt;
	private boolean createLaneContent;

	private Option maxTagMismatchesOpt;
	private int maxTagMismatches;

	private Option noIndexReadsOpt;
	private boolean noIndexReads;

	public DemuxOptionParser()
	{
		super(ConfigSection, "seal_demux");

		sampleSheetOpt = OptionBuilder
											.withDescription("Sample sheet for the experiment")
											.hasArg()
											.withArgName("FILE")
											.withLongOpt("sample-sheet")
											.create("s");
		options.addOption(sampleSheetOpt);

		laneContentOpt = OptionBuilder
											.withDescription("create LaneContent files")
											.withLongOpt("create-lane-content")
											.create("l");
		createLaneContent = false;
		options.addOption(laneContentOpt);

		maxTagMismatchesOpt = OptionBuilder
			.withDescription("Maximum number of acceptable barcode substitution errors (default: 0)")
			.hasArg()
			.withArgName("N")
			.withLongOpt("mismatches")
			.create("m");
		maxTagMismatches = 0; // default value
		options.addOption(maxTagMismatchesOpt);

		noIndexReadsOpt = OptionBuilder
			.withDescription("Dataset doesn't contain index reads.  Sort reads only by lane (default: false)")
			.withLongOpt("no-index")
			.create("ni");
		noIndexReads = false; // default value
		options.addOption(noIndexReadsOpt);

		this.setMinReduceTasks(1);
		this.setAcceptedInputFormats(new String[] { "qseq", "fastq" });
		this.setAcceptedOutputFormats(new String[] { "qseq", "fastq" });
	}

	@Override
	protected CommandLine parseOptions(Configuration conf, String[] args)
	  throws IOException, ParseException
	{
		CommandLine line = super.parseOptions(conf, args);
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
			throw new ParseException("Missing --" + sampleSheetOpt.getLongOpt() + " argument");

		if (line.hasOption(laneContentOpt.getOpt()))
			createLaneContent = true;

		if (line.hasOption(maxTagMismatchesOpt.getOpt()))
		{
			String s = line.getOptionValue(maxTagMismatchesOpt.getOpt());
			try {
				maxTagMismatches = Integer.parseInt(s);
				if (maxTagMismatches < 0)
					throw new ParseException("Maximum number of acceptable barcode mismatches must be greater than zero (got " + s + ")");
				conf.set(Demux.CONF_MAX_MISMATCHES, String.valueOf(maxTagMismatches));
			}
			catch (NumberFormatException e) {
				throw new ParseException("Invalid number '" + s + "' for " + maxTagMismatchesOpt.getLongOpt());
			}
		}

		if (line.hasOption(noIndexReadsOpt.getOpt()))
			noIndexReads = true;

		// set number of reduce tasks to use
		conf.set(ClusterUtils.NUM_RED_TASKS_PROPERTY, String.valueOf(getNReduceTasks()));
		return line;
	}

	public Path getSampleSheetPath() { return sampleSheetPath; }

	public boolean getCreateLaneContent() { return createLaneContent; }

	public int getMaxTagMismatches() { return maxTagMismatches; }

	public boolean getNoIndexReads() { return noIndexReads; }
}
