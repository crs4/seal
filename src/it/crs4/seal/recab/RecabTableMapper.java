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
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RecabTableMapper
{
	private static final Log LOG = LogFactory.getLog(RecabTableMapper.class);

	private static final byte SANGER_OFFSET = 33;
	
	public static enum BaseCounters {
		Used,
		BadBases,
		SnpMismatches,
		SnpBases,
		NonSnpMismatches,
	};

	public static enum ReadCounters {
		Processed,
		Unmapped,
	};

	private SnpTable snps;

	private AbstractSamMapping currentMapping;
	private ArrayList<Integer> referenceCoordinates;
	private ArrayList<Boolean> referenceMatches;
	private ArrayList<Covariate> covariateList;

	private Text key = new Text();
	private ObservationCount value = new ObservationCount();

	public void setup(SnpReader reader, IMRContext<Text, ObservationCount> context, Configuration conf) throws IOException
	{
		snps = new SnpTable();
		LOG.info("loading known variation sites.");
		snps.load(reader);
		if (LOG.isInfoEnabled())
			LOG.info("loaded " + snps.size() + " known variation sites.");

		referenceCoordinates = new ArrayList<Integer>(200);
		referenceMatches = new ArrayList<Boolean>(200);

		// TODO:  make it configurable
		covariateList = new ArrayList<Covariate>(5);
		covariateList.add( new ReadGroupCovariate(conf) );
		covariateList.add( new QualityCovariate() );
		covariateList.add( new CycleCovariate() );
		covariateList.add( new DinucCovariate() );

		// set counters
		for (BaseCounters c: BaseCounters.class.getEnumConstants())
			context.increment(c, 0);

		for (ReadCounters c: ReadCounters.class.getEnumConstants())
			context.increment(c, 0);
	}

	public void map(LongWritable ignored, Text sam, IMRContext<Text, ObservationCount> context) throws IOException, InterruptedException
	{
		currentMapping = new TextSamMapping(sam);
		context.increment(ReadCounters.Processed, 1);

		// skip unmapped reads or with mapq 0
		if (currentMapping.isUnmapped() || currentMapping.getMapQ() == 0)
		{
			context.increment(ReadCounters.Unmapped, 1);
			return;
		}

		final String contig  = currentMapping.getContig();
		final ByteBuffer seq = currentMapping.getSequence();
		final ByteBuffer qual = currentMapping.getBaseQualities();

		currentMapping.calculateReferenceCoordinates(referenceCoordinates);
		currentMapping.calculateReferenceMatches(referenceMatches);

		for (Covariate cov: covariateList)
			cov.applyToMapping(currentMapping);

		for (int i = 0; i < currentMapping.getLength(); ++i)
		{
			byte base = seq.get();
			byte quality = qual.get();

			if (base == 'N' || quality == SANGER_OFFSET)
				context.increment(BaseCounters.BadBases, 1);
			else
			{
				int pos = referenceCoordinates.get(i);
				if (pos > 0) // valid reference position
				{
					// is it a known variation site?
					if (snps.isSnpLocation(contig, pos))
					{
						// skip this base
						context.increment(BaseCounters.SnpBases, 1);

						if (!referenceMatches.get(i))
							context.increment(BaseCounters.SnpMismatches, 1);
					}
					else
					{
						key.clear();
						for (Covariate cov: covariateList)
						{
							String tmp = cov.getValue(i);
							key.append(tmp.getBytes(RecabTable.ASCII), 0, tmp.length());
							key.append(RecabTable.TableDelimBytes, 0, RecabTable.TableDelimBytes.length);
						}

						boolean match = referenceMatches.get(i);
						if (match)
							value.set(1, 0); // (num observations, num mismatches)
						else
						{ // mismatch
							context.increment(BaseCounters.NonSnpMismatches, 1);
							value.set(1, 1);
						}

						context.write(key, value);

						// use this base
						context.increment(BaseCounters.Used, 1);
					}
				}
			}
		}
	}
}
