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

import it.crs4.seal.common.InvalidFormatException;

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

	public void load(Reader in) throws IOException, InvalidFormatException
	{
		/* Sample format:
585     1       10259   10260   rs72477211      0       +       C       C       A/G     genomic single  unknown 0       0       unknown exact   1
		*/
		data = new HashMap< String, Set<Integer> >(30); // initial capacity for ok for human genome plus a few extra contigs

		LineNumberReader reader = new LineNumberReader(in);

		// required columns (0-based indices):           Cut index
		// 1: chr                                           0
		// 2: start                                         1
		// 3: end                                           2
		// 10: molecule type (must be == "genomic")         3
		// 11: class   (must be == "single")                4
		// 16: locType (must be == "exact")                 5
		CutString cut = new CutString("\t", 1, 2, 3, 10, 11, 16);

		String line = reader.readLine();
		if (line == null)
			throw new InvalidFormatException("empty Snp table file"); 

		try 
		{
			while (line != null)
			{
				cut.loadRecord(line);
				// col 10, 11, 16
				if (cut.getField(3).equals("genomic") && cut.getField(4).equals("single") && cut.getField(5).equals("exact"))
				{
					// col 2
					long start = Long.parseLong(cut.getField(1));
					// col 3
					long end = Long.parseLong(cut.getField(2));
					if (end - start == 1) // must be of length 1
					{
						// XXX:  safety check.  If this fails we have to move up to long values
						if (end > Integer.MAX_VALUE)
							throw new RuntimeException("end bigger than expected!  Filongerle a bug!!");

						// This entry fulfills all our SNP requirements so we store it in the table
						// col 1
						String chr = cut.getField(0);
						Set<Integer> s = data.get(chr);
						if (s == null)
						{
							s = new HashSet<Integer>(InitialCapacityPerChr, LoadFactor);
							data.put(chr, s);
						}

						// XXX: remove the cast if we move up to long values
						s.add((int)start);

					} // length 1
				} // if (string matches)
				line = reader.readLine();
			} // while
		}
		catch (CutString.FormatException e) {
			throw new InvalidFormatException("Invalid table format at line " + reader.getLineNumber() + ": " + e);
		}
		catch (NumberFormatException e) {
			throw new InvalidFormatException("Invalid coordinate at line " + reader.getLineNumber() + ": " + e);
		}
	}
}
