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

/**
 * Bean to store the minimal definition of a SNP: contig and position.
 */
public class VariantRegion
{
	private String contigName;
	private int startPos;
	private int length;

	public VariantRegion() {}
	public VariantRegion(String contig, int startPos)
	{
		this(contig, startPos, 1);
	}

	public VariantRegion(String contig, int startPos, int length)
	{
		if (startPos <= 0)
			throw new IllegalArgumentException("Variant position must be > 0");
		if (length <= 0)
			throw new IllegalArgumentException("Variant length  must be > 0");

		contigName = contig;
		this.startPos = startPos;
		this.length = length;
	}

	public String getContigName() { return contigName; }
	public int getPosition() { return startPos; }
	public int getLength() { return length; }

	public void setContigName(String name) { contigName = name; }

	public void setPosition(int pos) 
	{
		if (pos <= 0)
			throw new IllegalArgumentException("Variant position must be > 0");
	 	startPos = pos; 
	}

	public void setLength(int len) 
	{
		if (len <= 0)
			throw new IllegalArgumentException("Variant length  must be > 0");
		length = len; 
	}

	public void set(VariantRegion other)
	{
		contigName = other.contigName;
		startPos = other.startPos;
		length = other.length;
	}
}
