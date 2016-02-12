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

import it.crs4.seal.common.AbstractTaggedMapping;

public class CycleCovariate implements Covariate
{
	private int readNum = -1;
	private int readLength = -1;
	private boolean forwardStrand;

	public void applyToMapping(AbstractTaggedMapping m)
	{
		// is isRead2 is set, then read 2.  All other cases (e.g. unpaired), then read 1
		readNum = m.isRead2() ? 2 : 1;
		readLength = m.getLength();
		forwardStrand = !m.isOnReverse();
	}

	public String getValue(int pos)
	{
		if (readLength < 0)
			throw new RuntimeException("BUG! readLength == " + readLength + ". applyToMapping not called before CycleCovariate.getValue");
		else if (readNum < 0)
			throw new RuntimeException("BUG! readNum == " + readNum + ". applyToMapping not called before CycleCovariate.getValue");
		else if (pos < 0 || pos >= readLength)
			throw new IndexOutOfBoundsException("pos " + pos + " is out of read boundaries [0," + readLength + ")");

		int cycle;
		if (forwardStrand)
			cycle = pos + 1;
		else
			cycle = readLength - pos;

		if (readNum == 2)
			cycle *= -1;

		return String.valueOf(cycle);
	}
}
