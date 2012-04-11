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

package it.crs4.seal.tsv_sort;

import it.crs4.seal.common.ClusterUtils;
import it.crs4.seal.common.SealToolParser;

import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.commons.cli.*;

public class TsvSortOptionParser extends SealToolParser {

	public static final String ConfigSection = "TsvSort";

	private Option keyOption;
	private Option delimiterOption;

	public TsvSortOptionParser()
	{
		super(ConfigSection, "seal_tsvsort");

		// define custom options

		keyOption = OptionBuilder
		              .withDescription("sort key list. Comma-separated numbers, ranges specified with '-' [default: whole line]")
		              .hasArg()
		              .withArgName("index[-last_index]")
		              .withLongOpt("key")
		              .create("k");
		options.addOption(keyOption);

		delimiterOption = OptionBuilder
		              .withDescription("character string that delimits fields [default: <TAB>]")
		              .hasArg()
		              .withArgName("DELIM")
		              .withLongOpt("field-separator")
		              .create("t");
		options.addOption(delimiterOption);

		this.setMinReduceTasks(1);
	}

	@Override
	protected CommandLine parseOptions(Configuration conf, String[] args)
	  throws IOException, ParseException
	{
		CommandLine line = super.parseOptions(conf, args);

		if (line.hasOption(keyOption.getOpt()))
			conf.set(TsvInputFormat.COLUMN_KEYS_CONF, line.getOptionValue(keyOption.getOpt()));

		if (line.hasOption(delimiterOption.getOpt()))
		{
			String delim = line.getOptionValue(delimiterOption.getOpt());
			conf.set(TsvInputFormat.DELIM_CONF, delim);
		}

		// set number of reduce tasks to use
		conf.set(ClusterUtils.NUM_RED_TASKS_PROPERTY, String.valueOf(getNReduceTasks()));
		return line;
	}
}
