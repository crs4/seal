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

	public static final String CONF_SKIP_KNOWN_VAR_SITES = "seal.recab.skip-known-variant-sites"; // mainly for testing

	private static final byte SANGER_OFFSET = 33;

	public static enum BaseCounters {
		All,
		Used,
		BadBases,
		VariantMismatches,
		VariantBases,
		NonVariantMismatches,
		AdaptorBasesTrimmed
	};

	public static enum ReadCounters {
		Processed,
		FilteredTotal,
		FilteredUnmapped,
		FilteredMapQ,
		FilteredDuplicate,
		FilteredQC,
		FilteredSecondaryAlignment
	};

	private VariantTable snps;

	private AbstractSamMapping currentMapping;
	private ArrayList<Integer> referenceCoordinates;
	private ArrayList<Boolean> referenceMatches;
	private ArrayList<Covariate> covariateList;
	private IMRContext<Text, ObservationCount> context;
	private boolean skipKnownVariantPositions = true;

	private Text key = new Text();
	private ObservationCount value = new ObservationCount();

	public void setup(VariantReader reader, IMRContext<Text, ObservationCount> context, Configuration conf) throws IOException
	{
		this.context = context;
		snps = new ArrayVariantTable();
		LOG.info("Using " + snps.getClass().getName() + " snp table implementation.");
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

		skipKnownVariantPositions = conf.getBoolean(CONF_SKIP_KNOWN_VAR_SITES, true);
		if (!skipKnownVariantPositions)
			LOG.warn("Not skipping known variant sites.  This is not recommended for regular usage.");
	}

	protected boolean readFailsFilters(AbstractSamMapping map)
	{
		boolean fails = false;
		if (map.isUnmapped())
		{
			context.increment(ReadCounters.FilteredUnmapped, 1);
			fails = true;
		}
		else if (map.getMapQ() == 0 || map.getMapQ() == 255)
		{
			context.increment(ReadCounters.FilteredMapQ, 1);
			fails = true;
		}
		else if (map.isDuplicate())
		{
			context.increment(ReadCounters.FilteredDuplicate, 1);
			fails = true;
		}
		else if (map.isFailedQC())
		{
			context.increment(ReadCounters.FilteredQC, 1);
			fails = true;
		}
		else if (map.isSecondaryAlign())
		{
			context.increment(ReadCounters.FilteredSecondaryAlignment, 1);
			fails = true;
		}

		if (fails)
			context.increment(ReadCounters.FilteredTotal, 1);

		return fails;
	}

	public void map(LongWritable ignored, Text sam, IMRContext<Text, ObservationCount> context) throws IOException, InterruptedException
	{
		currentMapping = new TextSamMapping(sam);
		context.increment(ReadCounters.Processed, 1);
		context.increment(BaseCounters.All, currentMapping.getLength());

		if (readFailsFilters(currentMapping))
			return;

		final String contig  = currentMapping.getContig();

		int left, right;
		if (currentMapping.isTemplateLengthAvailable() &&
		    currentMapping.getTemplateLength() < currentMapping.getLength())
		{
			// Insert size is less than the read size, so we've sequenced part of the read adapter.
			// We need to trim it the last read_length - insert_size bases sequenced.
			if (currentMapping.isOnReverse())
			{
				// trim from front
				left = currentMapping.getLength() - currentMapping.getTemplateLength();
				right = currentMapping.getLength();
			}
			else
			{
				// trim from the back
				left = 0;
				right = currentMapping.getTemplateLength();
			}

			context.increment(BaseCounters.AdaptorBasesTrimmed, currentMapping.getLength() - (right - left));
		}
		else
		{
			left = 0;
			right = currentMapping.getLength();
		}

		currentMapping.calculateReferenceCoordinates(referenceCoordinates);
		currentMapping.calculateReferenceMatches(referenceMatches);

		for (Covariate cov: covariateList)
			cov.applyToMapping(currentMapping);

		final ByteBuffer seq = currentMapping.getSequence();
		final ByteBuffer qual = currentMapping.getBaseQualities();
		if (left > 0)
		{
			// advance the buffers
			seq.position( seq.position() + left );
			qual.position( qual.position() + left );
		}

		for (int i = left; i < right; ++i)
		{
			byte base = seq.get();
			byte quality = qual.get();

			if (base == 'N' || quality <= SANGER_OFFSET)
				context.increment(BaseCounters.BadBases, 1);
			else
			{
				int pos = referenceCoordinates.get(i);
				if (pos > 0) // valid reference position
				{
					// is it a known variation site?
					if (skipKnownVariantPositions && snps.isVariantLocation(contig, pos))
					{
						context.increment(BaseCounters.VariantBases, 1);

						if (!referenceMatches.get(i))
							context.increment(BaseCounters.VariantMismatches, 1);
					}
					else
					{
						// use this base
						context.increment(BaseCounters.Used, 1);

						key.clear();
						for (Covariate cov: covariateList)
						{
							String tmp = cov.getValue(i);
							key.append(tmp.getBytes(RecabTable.ASCII), 0, tmp.length());
							key.append(RecabTable.TableDelimBytes, 0, RecabTable.TableDelimBytes.length);
						}

						boolean match = referenceMatches.get(i);
						if (match)
						{
							value.set(1, 0); // (num observations, num mismatches)
						}
						else
						{ // mismatch
							context.increment(BaseCounters.NonVariantMismatches, 1);
							value.set(1, 1);
						}

						context.write(key, value);
					}
				}
			}
		}
	}
}
