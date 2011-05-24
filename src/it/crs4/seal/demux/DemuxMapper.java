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

package it.crs4.seal.demux;

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.SequenceId;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Mapper for demultiplexing reads.
 * Maps each qseq record into a SequenceId (key) and the same qseq record (value).
 */
public class DemuxMapper
{
	private static final String QseqDelim = "\t";

	private SequenceId key = new SequenceId();

	public void map(LongWritable ignored, Text qseq, IMRContext<SequenceId, Text> context) throws IOException, InterruptedException
	{
		try
		{
			int readNumPosition = findReadNumPosition(qseq);
			int readNumEnd = qseq.find(QseqDelim, readNumPosition);
			if (readNumEnd < 0)
					throw new RuntimeException("invalid qseq line " + qseq.toString());

			int readno = Integer.parseInt( Text.decode(qseq.getBytes(), readNumPosition, readNumEnd - readNumPosition) );
			if (readno <= 0)
				throw new RuntimeException("Invalid read number in qseq" + readno + ".  Text: " + qseq.toString());

			key.set( Text.decode(qseq.getBytes(), 0, readNumPosition-1), readno); // -1 to eliminate the tab
			context.write(key, qseq);
		}
		catch (java.nio.charset.CharacterCodingException e)
		{
			throw new RuntimeException("Character coding error in qseq: " + e.getMessage());
		}
	}

	private int findReadNumPosition(Text qseq)
	{
		int pos = 0;
		for (int i = 1; i <= 7; ++i)
		{
			pos = qseq.find(QseqDelim, pos) + 1;
			if (pos < 0)
				throw new RuntimeException("invalid qseq line " + qseq.toString());
		}
		return pos;
	}
}
