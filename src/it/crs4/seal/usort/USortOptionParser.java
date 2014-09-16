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

package it.crs4.seal.usort;

import it.crs4.seal.common.SealToolParser;
import it.crs4.seal.common.ClusterUtils;

import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.QseqInputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class USortOptionParser extends SealToolParser {

	private static final Log LOG = LogFactory.getLog(USort.class);

	public static final String ConfigSection = "USort";

	public static final String InputFormatDefault = "qseq";
	public static final String OutputFormatDefault = "qseq";

	public USortOptionParser()
	{
		super(ConfigSection, "seal usort");
		this.setMinReduceTasks(USort.NUM_REDUCE_TASKS);
		this.setAcceptedInputFormats(new String[] { "qseq", "fastq" });
		this.setAcceptedOutputFormats(new String[] { "qseq", "fastq" });
	}

	@Override
	protected CommandLine parseOptions(Configuration conf, String[] args)
	  throws IOException, ParseException
	{
		CommandLine line = super.parseOptions(conf, args);

		// set number of reduce tasks to use
		conf.set(ClusterUtils.NUM_RED_TASKS_PROPERTY, String.valueOf(USort.NUM_REDUCE_TASKS));
		return line;
	}
}
