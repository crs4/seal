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

import it.crs4.seal.common.CutText;
import it.crs4.seal.common.AbstractSamMapping;

import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;

/**
 * Implements a SAM mapping read from a Text object.
 */
public class TextSamMapping extends AbstractSamMapping
{
	protected static final String Delim = "\t";

	protected CutText cutter;
	protected Text unparsedData; // part of the record we don't read unless we need to
	protected int flag;
	protected int pos5 = 0;
	protected byte mapQ;
	protected int matePos5 = 0;
	protected int insertSize = 0;

	protected int seqLen;
	protected int qualityStart;

	protected int tagsStart;

	// samples SAM record
	//                    0                   	 1	 2  	  3	   4   5  6   7   8	                         9                         	                 10
	// DCW97JN1_252:1:1105:15329:186955#GGCTAC	89	chr1	12134	30	51M	=	12134	0	TTGTCTGCATGTAACTTAATACCACAACCAGGCATAGGGGAAAGATTGGAG	IJJJJIJJJJJJJIIGJIHFEIHFJJJJJJJJJJJJJJHHHHHFFFFFCCC	XT:A:R	NM:i:0	SM:i:0	AM:i:0	X0:i:6	X1:i:1	XM:i:0	XO:i:0	XG:i:0	MD:Z:51

	public TextSamMapping(Text sam) throws FormatException
	{
		unparsedData = new Text();
		cutter = new CutText(Delim, 0, 1, 2, 3, 4, 5, 6, 7, 8); // all fields up to and including insert size

		try
		{
			cutter.loadRecord(sam);
			flag = Integer.parseInt(cutter.getField(1)); // set flag first so we can use the flag methods
			mapQ = Byte.parseByte(cutter.getField(4));

			if (isMapped())
				pos5 = Integer.parseInt(cutter.getField(3));
			if (isMateMapped())
				matePos5 = Integer.parseInt(cutter.getField(7));
			if (isMapped() && isMateMapped())
				insertSize = Integer.parseInt(cutter.getField(8));
		}
		catch (CutText.FormatException e) {
			throw new FormatException("sam formatting problem: " + e + ". Record: " + sam);
		}
		catch (NumberFormatException e) {
			throw new FormatException("sam formatting problem.  Found text in place of a number.  Record: " + sam);
		}

		int seqStart = cutter.getFieldPos(8) + cutter.getField(8).length() + 1;
		if (seqStart > sam.getLength())
			throw new FormatException("Incomplete SAM record -- missing fields. Record: " + sam);
		// copy the sequence and tag data to our internal buffer
		unparsedData.set(sam.getBytes(), seqStart, sam.getLength() - seqStart);

		// Find the end of the sequence field.  Search for a Delim after the insert size field.
		int end = unparsedData.find(Delim);
		if (end < 0)
			throw new FormatException("Bad SAM format.  Missing terminator for sequence field.  SAM: " + sam);
		seqLen = end;

		// now repeat for the quality field
		qualityStart = end + 1;
		if (qualityStart > unparsedData.getLength())
			throw new FormatException("Incomplete SAM record -- missing quality field. Record: " + sam);
		end = unparsedData.find(Delim, qualityStart);
		if (end < 0)
			end = unparsedData.getLength();
		if (seqLen != end - qualityStart)
		{
			throw new FormatException("Length of sequence (" + seqLen + ") is different from length of quality string ("
					+ (end - qualityStart) + "). Record: " + sam);
		}

		tagsStart = end + 1;
	}

	public String getName() { return cutter.getField(0); }

	public int getFlag() { return flag; }

	public String getContig()
	{
		if (isUnmapped())
			throw new IllegalStateException();
	 	return cutter.getField(2);
	}
	public int get5Position()
	{
		if (isUnmapped())
			throw new IllegalStateException();
	 	return pos5;
	}

	public int getMapQ() { return mapQ; }

	public String getCigarStr()
 	{
		if (isUnmapped())
			throw new IllegalStateException();
	 	return cutter.getField(5);
	}

	public boolean isTemplateLengthAvailable()
	{
		return insertSize != 0;
	}

	public int getTemplateLength()
	{
		int abs = Math.abs(insertSize);
		if (abs > 0)
			return abs;
		else
			throw new IllegalStateException();
	}

	public ByteBuffer getSequence() { return (ByteBuffer)ByteBuffer.wrap(unparsedData.getBytes(), 0, seqLen).mark(); }
	public ByteBuffer getBaseQualities() { return (ByteBuffer)ByteBuffer.wrap(unparsedData.getBytes(), qualityStart, seqLen).mark(); }
	public int getLength() { return seqLen; }

	protected String getTagText(String name)
	{
		if (tagsStart >= unparsedData.getLength()) // no tags
			return null;

		String text = null;
		try {
			int pos = unparsedData.find(Delim + name, tagsStart - 1);
			if (pos >= 0)
			{
				int fieldEnd = unparsedData.find(Delim, pos + 1); // fieldEnd: index one position beyond the last char of the field
				if (fieldEnd < 0)
					fieldEnd = unparsedData.getLength();
				// decode n bytes from start
				//  start = pos + 1 (+1 to skip the delimiter)
				//  n = fieldEnd - start
				//    = fieldEnd - (pos + 1)
				//    = fieldEnd - pos - 1
				text = Text.decode(unparsedData.getBytes(), pos + 1, fieldEnd - pos - 1);
			}
		}
		catch (java.nio.charset.CharacterCodingException e) {
			throw new RuntimeException("character coding error retrieving tag '" + name + "' from SAM record " + this.toString());
		}

		return text;
	}

	public String toString()
	{
		StringBuilder builder = new StringBuilder(1000);
		for (int i = 0; i <= 8; ++i)
			builder.append(cutter.getField(i)).append('\t');
		builder.append(unparsedData.toString());

		return builder.toString();
	}
}
