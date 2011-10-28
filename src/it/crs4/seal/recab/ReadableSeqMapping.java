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

import java.util.ArrayList;
import java.nio.ByteBuffer;

public interface ReadableSeqMapping
{
	public String getName();
	public int getFlag();
	public String getContig();
	public long get5Position();
	public byte getMapQ();
	public String getCigarStr();
	public ByteBuffer getSequence();
	public ByteBuffer getBaseQualities();
	public int getLength();

	public ArrayList<AlignOp> getAlignment();
}
