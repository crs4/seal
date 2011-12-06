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

import java.util.List;
import java.nio.ByteBuffer;

public interface ReadableSeqMapping
{
	public String getName();
	public int getFlag();
	public String getContig() throws IllegalStateException;
	public int get5Position() throws IllegalStateException;
	public byte getMapQ();
	public String getCigarStr() throws IllegalStateException;
	
	/**
	 * This mapping's DNA sequence.
	 * The ASCII representation of the base sequence is contained in the ByteBuffer,
	 * starting at buffer.position() and ending at buffer.limit() (exclusive).
	 * The buffer is mark()ed at the start of the sequence.
	 */
	public ByteBuffer getSequence();
	
	/**
	 * This mapping's DNA sequence's base quality scores.
	 * The ASCII Phred+33 (Sanger encoding) representation of the base quality 
	 * scores sequence is contained in the ByteBuffer,
	 * starting at buffer.position() and ending at buffer.limit() (exclusive).
	 * The buffer is mark()ed at the start of the sequence.
	 */
	public ByteBuffer getBaseQualities();
	public int getLength();

	public List<AlignOp> getAlignment() throws IllegalStateException;
}
