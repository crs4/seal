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

import java.nio.ByteBuffer;

/**
 * Quality covariate.
 *
 * For a given base i, returns its 0-based quality value as a string.
 *
 * TODO:  profile!
 */
public class QualityCovariate implements Covariate
{
	private int readLength = -1;
	private ByteBuffer qualities;
	private int startPos;
	private static final byte SANGER_OFFSET = 33;

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

		byte sangerValue = qualities.get(startPos + pos);

		if (sangerValue < 33)
			throw new RuntimeException("base quality value out of sanger range [33,127]. Found value: " + sangerValue + " (ASCII " + ((char)sangerValue));

		return String.valueOf(sangerValue - SANGER_OFFSET);
	}
}
