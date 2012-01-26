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

public class ArrayListVariantTable implements VariantTable
{
	private static final Log LOG = LogFactory.getLog(ArrayListVariantTable.class);

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

	public boolean isVariantLocation(String chr, long pos)
	{
		if (pos > Integer.MAX_VALUE)
			throw new RuntimeException("pos bigger than expected!  File a bug!!");

		ArrayList<Integer> list = data.get(chr);
		if (list != null)
			return Collections.binarySearch(list, (int)pos) >= 0;
		else
			return false;
	}

	public void load(VariantReader reader) throws IOException, FormatException
	{
		data = new HashMap< String, ArrayList<Integer> >(30); // initial capacity for ok for human genome plus a few extra contigs
		VariantRegion snp = new VariantRegion();
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

			int refpos = snp.getPosition();
			int end = refpos + snp.getLength();
			// reference positions [refpos,end) are to be inserted as variants

			// find the the index of the element after which we want to insert
			// our new variant region
			int ipos = list.size() - 1;
			while (ipos >= 0 && list.get(ipos) >= refpos)
				--ipos;

			// if ipos at the last element simply append
			if (ipos >= list.size() - 1)
			{
				for (; refpos < end; ++refpos)
					list.add(refpos);
			}
			else
			{
				// Insert before the last element.
				// Increment ipos, so it becomes the index at which to start inserting
				ipos += 1;

				for (; refpos < end; ++refpos, ++ipos)
				{
					// for each position in the variant region, if it's not already in
					// our list insert it.
					if (list.get(ipos) != refpos)
						list.add(ipos, refpos);
				}
			}

			count += 1;
			if (LOG.isInfoEnabled())
			{
				if (count % 1000000 == 0)
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
