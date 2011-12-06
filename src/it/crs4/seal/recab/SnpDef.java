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
public class SnpDef
{
	private String contigName;
	private int position;

	public SnpDef() {}
	public SnpDef(String contig, int pos)
	{
		contigName = contig;
		position = pos;
	}

	public String getContigName() { return contigName; }
	public int getPosition() { return position; }

	public void setContigName(String name) { contigName = name; }
	public void setPosition(int pos) { position = pos; }

	public void set(SnpDef other)
	{
		contigName = other.contigName;
		position = other.position;
	}
}
