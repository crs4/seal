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

package it.crs4.seal.common;

import java.util.ArrayList;

public class CutString
{
	public static class FormatException extends Exception {
		private static final long serialVersionUID = 1L;
		public FormatException(String msg, String record)
		{
			super(msg + "Record: " + record.toString());
		}
	}

	private final String delim;

	private ArrayList<Integer> columns;
	private String[] extractedFields;
	private String currentRecord = null;

	/**
	 * @param delim:  delimiter string that separates fields within the records to be scanned.
	 * @param cols:  the sorted, zero-based indices of the columns to be extracted from the records.
	 */
	public CutString(String delimiter, int... cols)
	{
		if (delimiter.length() == 0)
			throw new IllegalArgumentException("empty string is an invalid delimiter");

		delim = delimiter;
		columns = new ArrayList<Integer>(cols.length);
		for (int c: cols)
			columns.add(c);

		if (columns.size() == 0)
			throw new IllegalArgumentException("no columns specified");

		// ensure the columns are in sorted order
		for (int i = 1; i < columns.size(); ++i)
		{
			if ( columns.get(i-1) >= columns.get(i) )
				throw new IllegalArgumentException("specified columns must be in sorted order and must not contain duplicates");
		}

		// initialize index
		extractedFields = new String[columns.size()];
	}

	public void loadRecord(String record) throws FormatException
	{
		currentRecord = record;

		int pos = 0; // the byte position within the record
		int fieldno = 0; // the field index within the record
		int colno = 0; // the index within the list of requested fields (columns)

		while (pos < record.length() && colno < columns.size()) // iterate over each field
		{
			int endpos = record.indexOf(delim, pos); // the field's end position
			if (endpos < 0)
				endpos = record.length();

			if (columns.get(colno) == fieldno) // if we're at a requested field
			{
				extractedFields[colno] = record.substring(pos, endpos);
				colno += 1; // advance column
			}

			pos = endpos + 1; // the next starting position is the current end + 1
			fieldno += 1;
		}

		if (colno < columns.size())
			throw new FormatException("Missing field(s) in record. Field " + colno + " (zero-based) not found.", record);
	}

	public String getField(int i)
	{
		if (currentRecord == null)
			throw new RuntimeException("getField called before loading a record");

		return extractedFields[i];
	}

	public String getCurrentRecord()
	{
		return currentRecord;
	}
}
