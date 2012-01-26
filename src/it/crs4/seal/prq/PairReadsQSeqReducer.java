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

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.SequenceId;

import org.apache.hadoop.io.Text;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class PairReadsQSeqReducer
{
	private static final Log LOG = LogFactory.getLog(PairReadsQSeqReducer.class);

	private Text outputKey = new Text();
	private Text outputValue = new Text();
	private int minBasesThreshold = 0;
	private boolean dropFailedFilter = true;
	private boolean warnOnlyIfUnpaired = false;

	private static final byte[] delimByte = { 9 }; // tab character
	private static final String delim = "\t";
	private static final char UnknownBase = 'N';

	public static enum ReadCounters {
		NotEnoughBases,
		FailedFilter,
		Unpaired,
		Dropped
	}

	public void setMinBasesThreshold(int v) { minBasesThreshold = v; }
	public void setDropFailedFilter(boolean v) { dropFailedFilter = v; }
	public void setWarnOnlyIfUnpaired(boolean v) { warnOnlyIfUnpaired = v; }

	public void setup(IMRContext<Text, Text> context)
	{
		// create counters with a value of 0.
		context.increment(ReadCounters.NotEnoughBases, 0);
		context.increment(ReadCounters.FailedFilter, 0);
		context.increment(ReadCounters.Dropped, 0);
	}

	public void reduce(SequenceId key, Iterable<Text> values, IMRContext<Text,Text> context) 
		throws IOException, InterruptedException
	{
		outputKey.set( key.getLocation() );
		outputValue.clear();

		int nReads = 0;
		int nBadReads = 0;
		for (Text read: values)
		{
			++nReads;
			int[] fieldsPos = findFields(read);
			// filtered read?
			// If dropFailedFilter is false it shortcuts the test and sets filterPassed directly to true.
			// If it's true then we check whether the field is equal to '1'
			boolean filterPassed = !dropFailedFilter || read.getBytes()[fieldsPos[2]] == (byte)'1';

			if (!filterPassed)
			{
				context.increment(ReadCounters.FailedFilter, 1);
				++nBadReads;
			}
			else if (!checkReadQuality(read, fieldsPos))
			{
				context.increment(ReadCounters.NotEnoughBases, 1);
				++nBadReads;
			}

			if (nReads > 1)
				outputValue.append(delimByte, 0, delimByte.length);

			outputValue.append(read.getBytes(), 0, fieldsPos[2] - 1); // -1 so we exclude the last delimiter
		}

		if (nReads == 1)
		{
			context.increment(ReadCounters.Unpaired, nReads);
			if (warnOnlyIfUnpaired)
				LOG.warn("unpaired read!\n" + outputValue.toString());
			else
				throw new RuntimeException("unpaired read for key " + key.toString() + "\nread: " + outputValue.toString());
		}
		else if (nReads != 2)
		{
			throw new RuntimeException("wrong number of reads for key " + key.toString() + 
					"(expected 2, got " + nReads + ")\n" + outputValue.toString());
		}

		if (nReads == 2 && nBadReads < nReads) // if they're paired and they're not all bad
			context.write(outputKey, outputValue);
		else
			context.increment(ReadCounters.Dropped, nReads);
		
		context.progress();
	}

	private int[] findFields(Text read)
	{
		int[] fieldsPos = new int[3];
		fieldsPos[0] = 0;

		for (int i = 1; i <= 2; ++i)
		{
			fieldsPos[i] = read.find(delim, fieldsPos[i-1]) + 1; // +1 since we get the position of the delimiter
			if (fieldsPos[i] <= 0)
				throw new RuntimeException("invalid read/quality format: " + read.toString());
		}

		return fieldsPos;
	}

	/**
	 * Verify whether a read satisfies quality standards.
	 * For now this method verifies whether the read has at least 
	 * minBasesThreshold known bases (ignoring unknown bases N).
	 */
	protected boolean checkReadQuality(Text read, int[] fieldsPos)
	{
		/* The read's delimiter is at the bytes before the second field starts */
		int readEnd = fieldsPos[1] - 1;

		// The condition is "min number of valid bases".  However, we consider 
		// the inverse condition "max number of unknowns".
		// readEnd is also the length of the read fragment
		// readEnd - minBasesThreshold gives us the maximum number of unknowns acceptable.
		int nAcceptableUnknowns = readEnd - minBasesThreshold;

		if (nAcceptableUnknowns < 0) // the fragment is shorter than minBasesThreshold
			return false;

		int nUnknownBases = 0;
		byte[] data = read.getBytes(); // we can work directly in bytes as long as we only has ASCII characters
		for (int pos = 0; pos < readEnd; ++pos)
		{
			if (data[pos] == UnknownBase)
			{
				++nUnknownBases;
				if (nUnknownBases > nAcceptableUnknowns)
					return false;
			}
		}
		return true;
	}
}

