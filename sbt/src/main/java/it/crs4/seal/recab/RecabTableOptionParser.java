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

package it.crs4.seal.recab;

import it.crs4.seal.common.SealToolParser;
import it.crs4.seal.common.ClusterUtils;

import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.commons.cli.*;

public class RecabTableOptionParser extends SealToolParser {

	public static final String ConfigSection = "RecabTable";

	private Option vcfFileOpt;
	private Path vcfFilePath;

	private Option rodFileOpt;
	private Path rodFilePath;

	private Configuration conf;

	@SuppressWarnings("static") // for OptionBuilder
	public RecabTableOptionParser()
	{
		super(ConfigSection, "seal recab_table");

		// define the options
		vcfFileOpt = OptionBuilder
		              .withDescription("VCF file with known variation sites")
		              .hasArg()
		              .withArgName("FILE")
		              .withLongOpt("vcf-file")
		              .create("vcf");
		options.addOption(vcfFileOpt);

		rodFileOpt = OptionBuilder
		             .withDescription("ROD file with known variation sites")
		             .hasArg()
		             .withArgName("FILE")
		             .withLongOpt("rod-file")
		             .create("rod");
		options.addOption(rodFileOpt);

		this.setMinReduceTasks(1);
		this.setAcceptedInputFormats(new String[] { "bam", "sam" });
		conf = null;
	}

	@Override
	protected CommandLine parseOptions(Configuration conf, String[] args)
	  throws IOException, ParseException
	{
		if (conf == null)
			throw new NullPointerException("null conf provided");

		this.conf = conf;

		CommandLine line = super.parseOptions(conf, args);

		if (line.hasOption(vcfFileOpt.getOpt()) &&  line.hasOption(rodFileOpt.getOpt()))
			throw new ParseException("You can't specify both VCF (" + vcfFileOpt.getLongOpt() + ") and ROD (" + rodFileOpt.getLongOpt() + ") files.  Please specify one or the other.");

		if (line.hasOption(vcfFileOpt.getOpt()))
		{
			vcfFilePath = new Path( line.getOptionValue(vcfFileOpt.getOpt()) );
			if (!vcfFilePath.getFileSystem(conf).exists(vcfFilePath))
				throw new ParseException("File " + vcfFilePath + " doesn't exist");
		}
		else if (line.hasOption(rodFileOpt.getOpt()))
		{
			rodFilePath = new Path( line.getOptionValue(rodFileOpt.getOpt()) );
			if (!rodFilePath.getFileSystem(conf).exists(rodFilePath))
				throw new ParseException("File " + rodFilePath + " doesn't exist");
		}
		else
			throw new ParseException("You must specify a file with known genetic variation sites (either VCF or ROD).");

		// set number of reduce tasks to use
		conf.set(ClusterUtils.NUM_RED_TASKS_PROPERTY, String.valueOf(getNReduceTasks()));
		return line;
	}

	public Path getVcfFile() { return vcfFilePath; }
	public Path getRodFile() { return rodFilePath; }
}
