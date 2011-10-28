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

//import it.crs4.seal.recab.ReadableSeqMapping;

import java.util.ArrayList;
import java.util.regex.*;

public abstract class AbstractSeqMapping implements ReadableSeqMapping
{
	////////////////////////////////////////////////
	// variables
	////////////////////////////////////////////////
	protected static final Pattern CigarElementPattern = Pattern.compile("(\\d+)(\\[MIDNSHP\\])");
	protected ArrayList<AlignOp> alignment;

	////////////////////////////////////////////////
	// methods
	////////////////////////////////////////////////

	public ArrayList<AlignOp> getAlignment()
	{
		// scan the CIGAR string and cache the results
		if (alignment == null)
		{
			ArrayList<AlignOp> result = new ArrayList<AlignOp>(5);
			Matcher m = CigarElementPattern.matcher(getCigarStr());

			while (m.find())
				result.add( new AlignOp(AlignOp.AlignOpType.fromSymbol(m.group(2)), Integer.parseInt(m.group(1))) );

			if (!m.hitEnd())
				throw new RuntimeException("Invalid CIGAR pattern " + getCigarStr());
		}
		return alignment;
	}

	public boolean isPaired() {
		return AlignFlags.Paired.is(getFlag());
	}

	public boolean isProperlyPaired() {
		return AlignFlags.ProperlyPaired.is(getFlag());
	}

	public boolean isMapped() {
		return AlignFlags.Unmapped.isNot(getFlag());
	}

	public boolean isUnmapped() {
		return AlignFlags.Unmapped.is(getFlag());
	}

	public boolean isMateMapped() {
		return AlignFlags.MateUnmapped.isNot(getFlag());
	}

	public boolean isMateUnmapped() {
		return AlignFlags.MateUnmapped.is(getFlag());
	}

	public boolean isOnReverse() {
		return AlignFlags.OnReverse.is(getFlag());
	}

	public boolean isMateOnReverse() {
		return AlignFlags.MateOnReverse.is(getFlag());
	}

	public boolean isRead1() {
		return AlignFlags.Read1.is(getFlag());
	}

	public boolean isRead2() {
		return AlignFlags.Read2.is(getFlag());
	}

	public boolean isSecondaryAlign() {
		return AlignFlags.SecondaryAlignment.is(getFlag());
	}

	public boolean isFailedQC() {
		return AlignFlags.FailedQC.is(getFlag());
	}

	public boolean isDuplicate() {
		return AlignFlags.Duplicate.is(getFlag());
	}
}
