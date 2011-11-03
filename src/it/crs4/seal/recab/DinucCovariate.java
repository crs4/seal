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

import java.util.HashMap;
import java.nio.ByteBuffer;

/**
 * Dinucleotide covariate.
 * Assumes reads mapped to the reverse strand have been reversed and complemented.
 *
 * TODO:  profile!
 */
public class DinucCovariate implements Covariate
{
	private int readLength = -1;
	private boolean forwardStrand;
	private ByteBuffer sequence;
	private ByteBuffer complement;
	private String[] values;

	private static final int BUFFER_SIZE = 200;

	private static final HashMap<Byte, Byte> complementByte;
	static {
		complementByte = new HashMap<Byte, Byte>();
		complementByte.put((byte)'N', (byte)'N');
		complementByte.put((byte)'A', (byte)'T');
		complementByte.put((byte)'C', (byte)'G');
		complementByte.put((byte)'G', (byte)'C');
		complementByte.put((byte)'T', (byte)'A');
	}

	public DinucCovariate()
	{
		complement = ByteBuffer.allocate(BUFFER_SIZE);
		complement.mark();
		values = new String[BUFFER_SIZE];
	}

	public void applyToMapping(AbstractSamMapping m)
	{
		readLength = m.getLength();
		forwardStrand = !m.isOnReverse();
		sequence = m.getSequence();

		if (!forwardStrand)
		{
			if (complement.limit() < readLength) // ensure space is sufficient
			{
				complement = ByteBuffer.allocate(readLength);
				complement.mark();
			}
			// now complement the read into the buffer
			complement.reset();
			int startPos = sequence.position();
			for (int pos = startPos + readLength - 1; pos >= startPos; --pos)
				complement.put( complementByte.get( sequence.get(pos)) );
		 	complement.reset();

			sequence = complement;
		}

		// calculate the dinucleotides
		int sequenceStart = sequence.position();

		values[0] = new String( new byte[]{'N', sequence.get(sequenceStart)});
		for (int i = 1; i < readLength; ++i)
			values[i] = new String(sequence.array(), sequenceStart + i - 1, 2);
	}

	public String getValue(int pos)
	{
		if (readLength < 0)
			throw new RuntimeException("BUG! readLength == " + readLength + ". applyToMapping not called before CycleCovariate.getValue");
		else if (pos < 0 || pos >= readLength)
			throw new IndexOutOfBoundsException("pos " + pos + " is out of read boundaries [0," + readLength + ")");

		return values[pos];
	}
}
