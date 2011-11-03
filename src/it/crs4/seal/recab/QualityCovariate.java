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

import java.nio.ByteBuffer;

/**
 * Dinucleotide covariate.
 * Assumes reads mapped to the reverse strand have been reversed and complemented.
 *
 * TODO:  profile!
 */
public class QualityCovariate implements Covariate
{
	private int readLength = -1;
	private ByteBuffer qualities;
	private int startPos;

	public void applyToMapping(AbstractSamMapping m)
	{
		readLength = m.getLength();
		qualities = m.getBaseQualities();
		startPos = qualities.position();
	}

	public String getValue(int pos)
	{
		if (readLength < 0)
			throw new RuntimeException("BUG! readLength == " + readLength + ". applyToMapping not called before QualityCovariate.getValue");
		else if (pos < 0 || pos >= readLength)
			throw new IndexOutOfBoundsException("pos " + pos + " is out of read boundaries [0," + readLength + ")");

		return String.valueOf(qualities.get(startPos + pos));
	}
}
