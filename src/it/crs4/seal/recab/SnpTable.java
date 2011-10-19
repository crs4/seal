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

import it.crs4.seal.common.FormatException;
import it.crs4.seal.common.CutString;

import java.io.Reader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

public class SnpTable
{
	private static final int InitialCapacityPerChr = 400000;
	private static final float LoadFactor = 0.80f;

	/**
	 * Main data structure.
	 * We use a Map with one entry per contig/chromosome.
	 * For each contig, we have a Set which stores all its SNP positions.
	 */
	// XXX: save some memory with Integer as opposed to Long.  We'll be fine with
	// the human genome, but...
	private Map< String, Set<Integer> > data;

	public boolean isSnpLocation(String chr, long pos)
	{
		if (pos > Integer.MAX_VALUE)
			throw new RuntimeException("pos bigger than expected!  File a bug!!");

		Set<Integer> s = data.get(chr);
		if (s != null)
			return s.contains((int)pos);
		return false;
	}

	public void load(SnpReader reader) throws IOException, FormatException
	{
		data = new HashMap< String, Set<Integer> >(30); // initial capacity for ok for human genome plus a few extra contigs
		SnpDef snp = new SnpDef();

		while (reader.nextEntry(snp)) // snp is re-used
		{
			// col 1
			String chr = snp.getContigName();
			Set<Integer> s = data.get(chr);
			if (s == null)
			{
				s = new HashSet<Integer>(InitialCapacityPerChr, LoadFactor);
				data.put(chr, s);
			}

			s.add(snp.getPosition());
		}
	}
}
