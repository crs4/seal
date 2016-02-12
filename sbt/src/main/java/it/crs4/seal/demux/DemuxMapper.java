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

package it.crs4.seal.demux;

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.SequenceId;

import org.seqdoop.hadoop_bam.SequencedFragment;

import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Mapper for demultiplexing reads.
 *
 * Maps each sequence record into a SequenceId (key) and the same sequence record (value).
 *
 * The key is generated with (Instrument, run number, lane, tile, xpos ypos).
 * Thus, the reducer gets all the reads for the specific position on the
 * flowcell:  read 1, read 2 (barcode), and read 3.
 */
public class DemuxMapper
{
	private SequenceId key = new SequenceId();
	private StringBuilder sBuilder = new StringBuilder(500);

	/**
	 * Forms a key and ensures the sequence defines its lane and read number.
	 * The key's location is defined with a colon-delimited string containing
	 * (instrument, run number, lane, tile, xpos, ypos).
	 *
	 * @throws RuntimeException If the sequence doesn't define its necessary location fields.
	 */
	public void map(Text inputKey, SequencedFragment seq, IMRContext<SequenceId, SequencedFragment> context) throws IOException, InterruptedException
	{
		checkFields(seq);
		sBuilder.setLength(0);

		if (seq.getRead() <= 0)
			throw new RuntimeException("Invalid read number " + seq.getRead() + " in sequence .  Record: " + seq.toString());

		// The key is: instrument, run number, lane, tile, xpos, ypos, read number, delimited by ':' characters.
		sBuilder.append(seq.getInstrument()).append(':');
		sBuilder.append(seq.getRunNumber()) .append(':');
		sBuilder.append(seq.getLane())      .append(':');
		sBuilder.append(seq.getTile())      .append(':');
		sBuilder.append(seq.getXpos())      .append(':');
		sBuilder.append(seq.getYpos());

		key.set(sBuilder.toString(), seq.getRead());
		context.write(key, seq);
	}

	private void checkFields(SequencedFragment seq)
	{
		try
		{
			if (seq.getInstrument() == null)
				throw new RuntimeException("missing instrument name");

			if (seq.getRunNumber() == null)
				throw new RuntimeException("missing run number");

			if (seq.getLane() == null)
				throw new RuntimeException("missing lane");

			if (seq.getRead() == null)
				throw new RuntimeException("missing read");

			if (seq.getTile() == null)
				throw new RuntimeException("missing tile");

			if (seq.getXpos() == null)
				throw new RuntimeException("missing xpos");

			if (seq.getYpos() == null)
				throw new RuntimeException("missing ypos");
		}
		catch (RuntimeException e) {
			throw new RuntimeException(e.toString() + " in sequence record: " + seq);
		}
	}
}
