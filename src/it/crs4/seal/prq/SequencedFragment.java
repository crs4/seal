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

public class SequencedFragment// implements Writable
{
	protected Text sequence = new Text();
	protected Text quality = new Text();

	protected String instrument;
	protected Integer runNumber;
	protected String flowcellId;
	protected Integer lane;
	protected Integer tile;
	protected Integer xpos;
	protected Integer ypos;
	protected Integer read;
	protected Boolean filterPassed;
	protected Integer controlNumber;
	protected String indexSequence;

	public void clear()
	{
		if (sequence == null)
			sequence = new Text();
		else
			sequence.clear();

		if (quality == null)
			quality = new Text();
		else
			quality.clear();

		instrument = null;
		runNumber = null;
		flowcellId = null;
		lane = null;
		tile = null;
		xpos = null;
		ypos = null;
		read = null;
		filterPassed = null;
		controlNumber = null;
		indexSequence = null;
	}

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

	public void setInstrument(String v) { instrument = v; }
	public void setRunNumber(Integer v) { runNumber = v; }
	public void setFlowcellId(String v) { flowcellId = v; }
	public void setLane(Integer v) { lane = v; }
	public void setTile(Integer v) { tile = v; }
	public void setXpos(Integer v) { xpos = v; }
	public void setYpos(Integer v) { ypos = v; }
	public void setRead(Integer v) { read = v; }
	public void setFilterPassed(Boolean v) { filterPassed = v; }
	public void setControlNumber(Integer v) { controlNumber = v; }
	public void setIndexSequence(String v) { indexSequence = v; }

	public String getInstrument() { return instrument; }
	public Integer getRunNumber() { return runNumber; }
	public String getFlowcellId() { return flowcellId; }
	public Integer getLane() { return lane; }
	public Integer getTile() { return tile; }
	public Integer getXpos() { return xpos; }
	public Integer getYpos() { return ypos; }
	public Integer getRead() { return read; }
	public Boolean getFilterPassed() { return filterPassed; }
	public Integer getControlNumber() { return controlNumber; }
	public String getIndexSequence() { return indexSequence; }

/*
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
	*/
}
