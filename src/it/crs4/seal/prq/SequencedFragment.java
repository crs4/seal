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

package it.crs4.seal.prq;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class SequencedFragment implements Writable
{
	protected Text sequence = new Text();
	protected Text quality = new Text();

	/**
	 * Get sequence Text object.
	 * Trade encapsulation for efficiency.  Here we expose the internal Text
	 * object so that data may be read and written diretly from/to it.
	 */
	public Text getSequence() { return sequence; }

	/**
	 * Get quality Text object.
	 * Trade encapsulation for efficiency.  Here we expose the internal Text
	 * object so that data may be read and written diretly from/to it.
	 */
	public Text getQuality() { return quality; }

	public void readFields(DataInput in) throws IOException
	{
		sequence.readFields(in);
		quality.readFields(in);
	}

	public void write(DataOutput out) throws IOException
	{
		sequence.write(out);
		quality.write(out);
	}
}
