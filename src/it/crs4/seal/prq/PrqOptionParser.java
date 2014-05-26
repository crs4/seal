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

package it.crs4.seal.prq;

import it.crs4.seal.common.SealToolParser;
import it.crs4.seal.common.ClusterUtils;
import it.crs4.seal.common.Utils;

import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PrqOptionParser extends SealToolParser {

	private static final Log LOG = LogFactory.getLog(PairReadsQSeq.class);

	public static final String ConfigSection = "Prq";

	public static final String InputFormatDefault = "qseq";
	public static final String OLD_INPUT_FORMAT_CONF = "seal.prq.input-format";

	public static final int DefaultMinBasesThreshold = 30;
	public static final String MinBasesThresholdConfigName = "seal.prq.min-bases-per-read";
	public static final String MinBasesThresholdConfigName_deprecated = "bl.prq.min-bases-per-read";

	public static final boolean DropFailedFilterDefault = true;
	public static final String DropFailedFilterConfigName = "seal.prq.drop-failed-filter";
	public static final String DropFailedFilterConfigName_deprecated = "bl.prq.drop-failed-filter";

	public static final boolean WarningOnlyIfUnpairedDefault = false;
	public static final String WarningOnlyIfUnpairedConfigName = "seal.prq.warning-only-if-unpaired";
	public static final String WarningOnlyIfUnpairedConfigName_deprecated = "bl.prq.warning-only-if-unpaired";

	public static final int NumReadsExpectedDefault = 2;
	public static final String NumReadsExpectedConfigName = "seal.prq.num-reads";

	private Option opt_traditionalIds;
	private boolean makeTraditionalIds = false;

	private Option opt_numReads;
	private int numReads = NumReadsExpectedDefault;

	@SuppressWarnings("static") // for OptionBuilder
	public PrqOptionParser()
	{
		super(ConfigSection, "seal prq");
		this.setMinReduceTasks(1);
		this.setAcceptedInputFormats(new String[] { "qseq", "fastq" });
		this.setAcceptedOutputFormats(new String[] { "prq" });

		opt_traditionalIds = OptionBuilder
			.withDescription("Create traditional read ids rather than new Illumina fastq-style read ids.")
			.withLongOpt("traditional-ids")
			.create("t");
		options.addOption(opt_traditionalIds);

		opt_numReads = OptionBuilder
			.withDescription("Number of reads expected per template.")
			.hasArg()
			.withLongOpt("num-reads")
			.create("nr");
		options.addOption(opt_numReads);
	}

	@Override
	protected CommandLine parseOptions(Configuration conf, String[] args)
	  throws IOException, ParseException
	{
		conf.setInt(MinBasesThresholdConfigName, DefaultMinBasesThreshold);
		conf.setBoolean(DropFailedFilterConfigName, DropFailedFilterDefault);
		conf.setBoolean(WarningOnlyIfUnpairedConfigName, WarningOnlyIfUnpairedDefault);
		conf.setInt(NumReadsExpectedConfigName, NumReadsExpectedDefault);

		CommandLine line = super.parseOptions(conf, args);

		/* **** handle deprected properties **** */
		if (conf.get(PrqOptionParser.OLD_INPUT_FORMAT_CONF) != null)
		{
			throw new ParseException("The property " + PrqOptionParser.OLD_INPUT_FORMAT_CONF + " is no longer supported.\n" +
				 "Please use the command line option --input-format instead.");
		}

		Utils.checkDeprecatedProp(conf, LOG, MinBasesThresholdConfigName_deprecated, MinBasesThresholdConfigName);
		Utils.checkDeprecatedProp(conf, LOG, DropFailedFilterConfigName_deprecated, DropFailedFilterConfigName);
		Utils.checkDeprecatedProp(conf, LOG, WarningOnlyIfUnpairedConfigName_deprecated, WarningOnlyIfUnpairedConfigName);

		// Let the deprecated properties override the new ones, unless the new ones have a non-default value.
		// If the new property has a non-default value, it must have been set by the user.
		// If, on the other hand, the deprecated property has a value, it must have been set by the user since
		// we're not setting them here.
		if (conf.get(MinBasesThresholdConfigName_deprecated) != null &&
				conf.getInt(MinBasesThresholdConfigName, DefaultMinBasesThreshold) == DefaultMinBasesThreshold)
		{
			conf.setInt(MinBasesThresholdConfigName, conf.getInt(MinBasesThresholdConfigName_deprecated, DefaultMinBasesThreshold));
		}

		if (conf.get(DropFailedFilterConfigName_deprecated) != null &&
				conf.getBoolean(DropFailedFilterConfigName, DropFailedFilterDefault) == DropFailedFilterDefault)
		{
			conf.setBoolean(DropFailedFilterConfigName, conf.getBoolean(DropFailedFilterConfigName_deprecated, DropFailedFilterDefault));
		}

		if (conf.get(WarningOnlyIfUnpairedConfigName_deprecated) != null &&
				conf.getBoolean(WarningOnlyIfUnpairedConfigName, WarningOnlyIfUnpairedDefault) == WarningOnlyIfUnpairedDefault)
		{
			conf.setBoolean(WarningOnlyIfUnpairedConfigName, conf.getBoolean(WarningOnlyIfUnpairedConfigName_deprecated, WarningOnlyIfUnpairedDefault));
		}

		/* **** end handle deprecated properties **** */

		if (line.hasOption(opt_traditionalIds.getOpt()))
			conf.setBoolean(PairReadsQSeq.PRQ_CONF_TRADITIONAL_IDS, true);

		if (line.hasOption(opt_numReads.getOpt()))
		{
			int numReads;
			try {
				numReads = Integer.valueOf(line.getOptionValue(opt_numReads.getOpt()));
				if (numReads <= 0)
					throw new ParseException("Number of reads per fragment must be >= 0 (got " + numReads + ")");
				if (numReads > 2) {
					throw new ParseException(
							"Working with more than two reads per template is not supported at the moment.\n" +
							"If you're interested in seeing this feature implemented contact the Seal developers.");
				}
			} catch (NumberFormatException e) {
				throw new ParseException(e.getMessage());
			}
			conf.setInt(NumReadsExpectedConfigName, numReads);
		}

		// set number of reduce tasks to use
		conf.set(ClusterUtils.NUM_RED_TASKS_PROPERTY, String.valueOf(getNReduceTasks()));
		return line;
	}
}
