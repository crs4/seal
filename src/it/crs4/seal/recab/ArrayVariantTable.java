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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ArrayVariantTable implements VariantTable
{
	private static final Log LOG = LogFactory.getLog(ArrayVariantTable.class);

	private static final int InitialCapacityPerChr = 400000;

	protected static class IntSortedArray {

		private static final int CompactionThreshold = 50*1024 / 4; // 50 KB

		protected int[] data;
		protected int size;

		public IntSortedArray()
		{
			data = new int[1000];
			size = 0;
		}

		public IntSortedArray(int initialStorage)
		{
			data = new int[initialStorage];
			size = 0;
		}

		/**
		 * Add the position to the list of variant positions.
		 * This method is optimized to receive positions in increasing order.
		 */
		public IntSortedArray add(int value)
		{
			// find the insert position
			int ipos = size - 1; // insert position at last element
			while (ipos >= 0 && data[ipos] > value)
				--ipos;
			// either ipos < 0 or data[ipos] <= value

			if (ipos < 0 || data[ipos] != value)
			{
				// Value not already in array so we have to make an insertion

				growIfNecessary(); // ensure room for 1 new element

				if (ipos < size - 1)
				{
					// insert before the last element.  We need to shift some elements down
					System.arraycopy(data, ipos + 1, data, ipos + 2, size - ipos - 1);
				}

				data[ipos+1] = value;
				size += 1;
			}

			return this;
		}

		protected void growIfNecessary()
		{
			if (size >= data.length)
			{
				// grow the array
				int[] newData = new int[data.length*2];
				System.arraycopy(data, 0, newData, 0, size);
				data = newData;
			}
		}

		public int size() { return size; }

		public boolean contains(int element)
		{
			return Arrays.binarySearch(data, 0, size, element) >= 0;
		}

		public void compact()
		{
			if (data.length - size > CompactionThreshold)
				data = Arrays.copyOf(data, size);
		}
	}

	/**
	 * Main data structure.
	 * We use a Map with one entry per contig/chromosome.
	 */
	// XXX: save some memory with Integer as opposed to Long.  We'll be fine with
	// the human genome, but large genomes would be a problem.
	//
	// TODO:  Can we be more clever in the way we use store these things to save some memory?
	protected Map< String, IntSortedArray > data;

	public boolean isVariantLocation(String chr, long pos)
	{
		if (pos > Integer.MAX_VALUE)
			throw new RuntimeException("pos bigger than expected!  File a bug!!");

		IntSortedArray list = data.get(chr);
		if (list != null)
			return list.contains((int)pos);
		else
			return false;
	}

	public void load(VariantReader reader) throws IOException, FormatException
	{
		data = new HashMap< String, IntSortedArray >(30); // initial capacity for ok for human genome plus a few extra contigs
		VariantRegion snp = new VariantRegion();
		long count = 0;

		while (reader.nextEntry(snp)) // snp is re-used
		{
			// col 1
			String chr = snp.getContigName();
			IntSortedArray list = data.get(chr);
			if (list == null)
			{
				list = new IntSortedArray(InitialCapacityPerChr);
				data.put(chr, list);
			}

			int end = snp.getPosition() + snp.getLength();
			for (int pos = snp.getPosition(); pos < end; ++pos)
				list.add(pos);

			count += 1;
			if (LOG.isInfoEnabled())
			{
				if (count % 1000000 == 0)
					LOG.info("Loaded " + count);
			}
		}

		for (IntSortedArray array: data.values())
			array.compact();

		LOG.info("Loaded a total of " + count + " known variations");
	}

	public int size()
	{
		int sum = 0;
		if (data != null)
		{
			for (IntSortedArray s: data.values())
				sum += s.size();
		}

		return sum;
	}

	public Set<String> getContigs()
	{
		return data.keySet();
	}
}
