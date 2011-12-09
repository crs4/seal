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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ArrayListSnpTable implements SnpTable
{
	private static final Log LOG = LogFactory.getLog(ArrayListSnpTable.class);

	private static final int InitialCapacityPerChr = 400000;

	/**
	 * Main data structure.
	 * We use a Map with one entry per contig/chromosome.
	 */
	// XXX: save some memory with Integer as opposed to Long.  We'll be fine with
	// the human genome, but large genomes would be a problem.
	//
	// TODO:  Can we be more clever in the way we use store these things to save some memory?
	protected Map< String, ArrayList<Integer> > data;

	public boolean isSnpLocation(String chr, long pos)
	{
		if (pos > Integer.MAX_VALUE)
			throw new RuntimeException("pos bigger than expected!  File a bug!!");

		ArrayList<Integer> list = data.get(chr);
		if (list != null)
			return Collections.binarySearch(list, (int)pos) >= 0;
		else
			return false;
	}

	public void load(SnpReader reader) throws IOException, FormatException
	{
		data = new HashMap< String, ArrayList<Integer> >(30); // initial capacity for ok for human genome plus a few extra contigs
		SnpDef snp = new SnpDef();
		long count = 0;

		while (reader.nextEntry(snp)) // snp is re-used
		{
			// col 1
			String chr = snp.getContigName();
			ArrayList<Integer> list = data.get(chr);
			if (list == null)
			{
				list = new ArrayList<Integer>(InitialCapacityPerChr);
				data.put(chr, list);
			}

			// should we verify that the positions are sorted?
			// if (list.get( list.size() - 1 ) > snp.getPosition())
			// 	throw new RuntimeException("list is not is sorted order! Found position " + list.get( list.size() - 1 ) + " before " + snp.getPosition());
			list.add(snp.getPosition());

			count += 1;
			if (LOG.isInfoEnabled())
			{
				if (count % 100000 == 0)
					LOG.info("Loaded " + count);
			}
		}
		LOG.info("Loaded a total of " + count + " known variations");
	}

	public int size()
	{
		int sum = 0;
		if (data != null)
		{
			for (List s: data.values())
				sum += s.size();
		}

		return sum;
	}

	public Set<String> getContigs()
	{
		return data.keySet();
	}
}
