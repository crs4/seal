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
import it.crs4.seal.common.CutText;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;

public class SamTextSeqMapping extends AbstractSeqMapping
{
	protected static final String Delim = "\t";

	protected CutText cutter;
	protected Text source;
	protected int flag;
	protected long pos5;
	protected byte mapQ;
	protected int seqLength;
	protected long matePos5;
	protected int insertSize;

	protected int seqStart;
	protected int seqLen;
	protected int qualityStart;
	protected int qualityLen;

	protected int tagsStart;

	// samples SAM record
	//                    0                   	 1	 2  	  3	   4   5  6   7   8	                         9                         	                 10
	// DCW97JN1_252:1:1105:15329:186955#GGCTAC	89	chr1	12134	30	51M	=	12134	0	TTGTCTGCATGTAACTTAATACCACAACCAGGCATAGGGGAAAGATTGGAG	IJJJJIJJJJJJJIIGJIHFEIHFJJJJJJJJJJJJJJHHHHHFFFFFCCC	XT:A:R	NM:i:0	SM:i:0	AM:i:0	X0:i:6	X1:i:1	XM:i:0	XO:i:0	XG:i:0	MD:Z:51

	public SamTextSeqMapping(Text sam) throws FormatException
	{
		source = sam;
		cutter = new CutText(Delim, 0, 1, 2, 3, 4, 5, 6, 7, 8); // all fields up to and including insert size

		try
		{
			cutter.loadRecord(source);
			flag = Integer.parseInt(cutter.getField(1));
			pos5 = Long.parseLong(cutter.getField(3));
			mapQ = Byte.parseByte(cutter.getField(4));
			matePos5 = Long.parseLong(cutter.getField(7));
			insertSize = Integer.parseInt(cutter.getField(8));
		}
		catch (CutText.FormatException e) {
			throw new FormatException("sam formatting problem: " + e + ". Record: " + source);
		}
		catch (NumberFormatException e) {
			throw new FormatException("sam formatting problem.  Found text in place of a number.  Record: " + source);
		}

		// Find the end of the sequence field.  Search for a Delim after the insert size field.
		seqStart = cutter.getFieldPos(8) + cutter.getField(8).length() + 1;
		if (seqStart > source.getLength())
			throw new FormatException("Incomplete SAM record -- missing fields. Record: " + source);
		int end = source.find(Delim, seqStart);
		if (end < 0)
			throw new FormatException("Bad SAM format.  Missing terminator for sequence field.  SAM: " + source);
		seqLen = end - seqStart;

		// now repeat for the quality field
		qualityStart = end + 1;
		if (qualityStart > source.getLength())
			throw new FormatException("Incomplete SAM record -- missing fields. Record: " + source);
		end = source.find(Delim, qualityStart);
		if (end < 0)
			throw new FormatException("Bad SAM format.  Missing terminator for quality field.  SAM: " + source);
		qualityLen = end - qualityStart;

		tagsStart = end + 1;
	}

	public String getName() { return cutter.getField(0); }
	public int getFlag() { return flag; }
	public String getContig() { return cutter.getField(2); }
	public long get5Position() { return pos5; }
	public byte getMapQ() { return mapQ; }
	public String getCigarStr() { return cutter.getField(5); }
	public ByteBuffer getSequence() { return ByteBuffer.wrap(source.getBytes(), seqStart, seqLen); }
	public ByteBuffer getBaseQualities() { return ByteBuffer.wrap(source.getBytes(), qualityStart, qualityLen); }
	public int getLength() { return seqLen; }
}