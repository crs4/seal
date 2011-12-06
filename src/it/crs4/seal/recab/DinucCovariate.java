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
	private static final String NN = "NN";

	private int readLength = -1;
	private boolean forwardStrand;
	private ByteBuffer sequence;
	private ByteBuffer revComplement;
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
		revComplement = ByteBuffer.allocate(BUFFER_SIZE);
		revComplement.mark();
		values = new String[BUFFER_SIZE];
	}

	public void applyToMapping(AbstractSamMapping m)
	{
		readLength = m.getLength();
		forwardStrand = !m.isOnReverse();
		sequence = m.getSequence();

		if (!forwardStrand)
		{
			// calculate the reversed complement
			if (revComplement.capacity() < readLength) // ensure space is sufficient
			{
				revComplement = ByteBuffer.allocate(readLength);
				revComplement.mark();
			}
			// now revComplement the read into the buffer
			revComplement.reset();
			int startPos = sequence.position();
			for (int pos = startPos + readLength - 1; pos >= startPos; --pos)
				revComplement.put( complementByte.get( sequence.get(pos)) );
		 	revComplement.reset();

			sequence = revComplement;
		}
		// now sequence is what was read by the sequencer

		// Calculate the dinucleotides.  We calculate them starting from the beginning
		// of 'sequence', but the order in which they are inserted into 'values' must
		// match the original base order.
		final byte[] seqArray = sequence.array();
		final int sequenceStart = sequence.position();
		int valuesPtr;
		int valuesIncrement;
		if (forwardStrand)
		{
			values[0] = NN;
			valuesPtr = 1;
			valuesIncrement = 1;
		}
		else
		{
			values[readLength - 1] = NN;
			valuesPtr = readLength - 2;
			valuesIncrement = -1;
		}

		// for each "previous" base, if it's an N insert a value of NN, otherwise the
		// dinucleotide (previous, previous+1)
		for (int previous = 0; previous < readLength - 1; ++previous)
		{
			if (seqArray[sequenceStart + previous] == 'N') // if previous base is an N
				values[ valuesPtr ] = NN;
			else
				values[ valuesPtr ] = new String(seqArray, sequenceStart + previous, 2); // string length 2 starting from seqArray[previous]
			valuesPtr += valuesIncrement;
		}
	}

	public String getValue(int pos)
	{
		if (readLength < 0)
			throw new RuntimeException("BUG! readLength == " + readLength + ". applyToMapping not called before DinucCovariate.getValue");
		else if (pos < 0 || pos >= readLength)
			throw new IndexOutOfBoundsException("pos " + pos + " is out of read boundaries [0," + readLength + ")");

		return values[pos];
	}
}
