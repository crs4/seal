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
import java.io.LineNumberReader;
import java.io.Reader;

/**
 * Read SNPs from a Rod file.
 */
public class RodFileVariantReader implements VariantReader
{
	private LineNumberReader reader;
	private CutString cutter;

	/* Sample format:
585     1       10259   10260   rs72477211      0       +       C       C       A/G     genomic single  unknown 0       0       unknown exact   1
	*/
	// required columns (0-based indices):           Cut index
	// 1: chr                                           0
	// 2: start                                         1
	// 3: end                                           2
	// 10: molecule type (must be == "genomic")         3
	// 11: class   (must be == "single")                4
	// 16: locType (must be == "exact")                 5

	public RodFileVariantReader(Reader in) throws IOException
	{
		reader = new LineNumberReader(in);
		// see the sample format above to understand the indices selected.
		cutter = new CutString("\t", 1, 2, 3, 10, 11, 16);
	}

	public boolean nextEntry(VariantRegion dest) throws FormatException, IOException
	{
		boolean gotRecord = false;
		String line;

		try
		{
			do
			{
	 			line = reader.readLine();
				if (line != null)
				{
					cutter.loadRecord(line);
					// col 10, 11, 16
					if (cutter.getField(3).equals("genomic") && cutter.getField(4).equals("single") && cutter.getField(5).equals("exact"))
					{
						// col 2
						long start = Long.parseLong(cutter.getField(1));
						// col 3
						long end = Long.parseLong(cutter.getField(2));
						if (end - start == 1) // must be of length 1
						{
							// XXX:  safety check.  If this fails we have to move up to long values
							if (end > Integer.MAX_VALUE)
								throw new RuntimeException("end bigger than expected!  File a bug!!");

							// This entry fulfills all our SNP requirements so we return it
							// col 1
							dest.setContigName(cutter.getField(0));
							// XXX: remove the cast if we move up to long values
							dest.setPosition((int)start);
							dest.setLength((int)(end - start));
							gotRecord = true;
						} // length 1
					} // if (string matches)
				}
				else // line is null
				{
					if (reader.getLineNumber() == 0)
						throw new FormatException("empty Variant table file");
				}
			} while (line != null && !gotRecord);
		}
		catch (CutString.FormatException e) {
			throw new FormatException("Invalid table format at line " + reader.getLineNumber() + ": " + e);
		}
		catch (NumberFormatException e) {
			throw new FormatException("Invalid coordinate at line " + reader.getLineNumber() + ": " + e);
		}

		return gotRecord;
	}
}
