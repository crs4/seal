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

package it.crs4.seal.prq;

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.SequenceId;

import org.apache.hadoop.io.Text;

import java.io.IOException;

public class PairReadsQSeqMapper
{
	private StringBuilder builder;
	private SequenceId sequenceKey = new SequenceId();
	private Text sequenceValue = new Text();

	private static final int LINE_SIZE = 500;
	private static final byte[] Delim = { 9 }; // tab
	private static final byte[] ZeroOne = { '0', '1' };

	public void setup()
	{
		builder = new StringBuilder(LINE_SIZE);
	}

	public void map(Text ignored, SequencedFragment read, IMRContext<SequenceId, Text> context) throws IOException, InterruptedException
	{
		// build the key
		builder.delete(0, builder.length());
		builder.append(read.getInstrument()).append("_").append(read.getRunNumber());
		builder.append(":").append(read.getLane());
		builder.append(":").append(read.getTile());
		builder.append(":").append(read.getXpos());
		builder.append(":").append(read.getYpos());
		// finally the index field
		builder.append("#").append(read.getIndexSequence());

		// field up and including the index number goes in the location.  The read is on its own.
		sequenceKey.set(builder.toString(), read.getRead());

		// then the tab-delimited value
		sequenceValue.clear();
		sequenceValue.append(read.getSequence().getBytes(), 0, read.getSequence().getLength());
		sequenceValue.append(Delim, 0, Delim.length);
		sequenceValue.append(read.getQuality().getBytes(), 0, read.getQuality().getLength());
		sequenceValue.append(Delim, 0, Delim.length);
		sequenceValue.append(ZeroOne, (read.getFilterPassed() ? 1 : 0 ), 1);

		context.write(sequenceKey, sequenceValue);
		context.progress();
	}
}

