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

import it.crs4.seal.common.IMRContext;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RecabTableMapper
{
	private static final Log LOG = LogFactory.getLog(RecabTableMapper.class);

	public static enum SiteCounters {
		Used,
		SkippedVariations,
	};

	public static enum BaseCounters {
		Used,
		Skipped,
	};

	private SnpTable snps;
	private TextSamMapping currentMapping;
	private ArrayList<Integer> referenceCoordinates;

	public void setup(SnpReader reader) throws IOException
	{
		snps = new SnpTable();
		LOG.info("loading known variation sites.");
		snps.load(reader);
		if (LOG.isInfoEnabled())
			LOG.info("loaded " + snps.size() + " known variation sites.");

		referenceCoordinates = new ArrayList<Integer>(200);
	}

	public void map(LongWritable ignored, Text sam, IMRContext<Text, Text> context) throws IOException, InterruptedException
	{
		currentMapping = new TextSamMapping(sam);

	}
}
