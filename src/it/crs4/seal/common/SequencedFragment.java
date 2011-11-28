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

package it.crs4.seal.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class SequencedFragment implements Writable
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

	// for serialization of nullable fiels
	protected static final int Instrument_Present     = 0x0001;
	protected static final int RunNumber_Present      = 0x0002;
	protected static final int FlowcellId_Present     = 0x0004;
	protected static final int Lane_Present           = 0x0008;
	protected static final int Tile_Present           = 0x0010;
	protected static final int Xpos_Present           = 0x0020;
	protected static final int Ypos_Present           = 0x0040;
	protected static final int Read_Present           = 0x0080;
	protected static final int FilterPassed_Present   = 0x0100;
	protected static final int ControlNumber_Present  = 0x0200;
	protected static final int IndexSequence_Present  = 0x0400;

	public void clear()
	{
		sequence.clear();
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

	public void setSequence(Text seq) 
	{
		if (seq == null)
			throw new IllegalArgumentException("can't have a null sequence");
	 	sequence = seq; 
	}

	public void setQuality(Text qual) 
	{
		if (qual == null)
			throw new IllegalArgumentException("can't have a null quality");
	 	quality = qual; 
	}

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

	/**
	 * Recreates a pseudo qseq record with the fields available.
	 */
	public String toString()
	{
		String delim = "\t";
		StringBuilder builder = new StringBuilder(800);
		builder.append(instrument).append(delim);
		builder.append(runNumber).append(delim);
		builder.append(flowcellId).append(delim);
		builder.append(lane).append(delim);
		builder.append(tile).append(delim);
		builder.append(xpos).append(delim);
		builder.append(ypos).append(delim);
		builder.append(indexSequence).append(delim);
		builder.append(read).append(delim);
		builder.append(sequence).append(delim);
		builder.append(quality).append(delim);
		builder.append((filterPassed == null || filterPassed) ? 1 : 0);
		return builder.toString();
	}

	public boolean equals(Object other)
	{
		if (other != null && other instanceof SequencedFragment)
		{
			SequencedFragment otherFrag = (SequencedFragment)other;

			if (instrument == null && otherFrag.instrument != null || instrument != null && !instrument.equals(otherFrag.instrument))
				return false;
			if (runNumber == null && otherFrag.runNumber != null || runNumber != null && !runNumber.equals(otherFrag.runNumber))
				return false;
			if (flowcellId == null && otherFrag.flowcellId != null || flowcellId != null && !flowcellId.equals(otherFrag.flowcellId))
				return false;
			if (lane == null && otherFrag.lane != null || lane != null && !lane.equals(otherFrag.lane))
				return false;
			if (tile == null && otherFrag.tile != null || tile != null && !tile.equals(otherFrag.tile))
				return false;
			if (xpos == null && otherFrag.xpos != null || xpos != null && !xpos.equals(otherFrag.xpos))
				return false;
			if (ypos == null && otherFrag.ypos != null || ypos != null && !ypos.equals(otherFrag.ypos))
				return false;
			if (read == null && otherFrag.read != null || read != null && !read.equals(otherFrag.read))
				return false;
			if (filterPassed == null && otherFrag.filterPassed != null || filterPassed != null && !filterPassed.equals(otherFrag.filterPassed))
				return false;
			if (controlNumber == null && otherFrag.controlNumber != null || controlNumber != null && !controlNumber.equals(otherFrag.controlNumber))
				return false;
			if (indexSequence == null && otherFrag.indexSequence != null || indexSequence != null && !indexSequence.equals(otherFrag.indexSequence))
				return false;
			// sequence and quality can't be null
			if (!sequence.equals(otherFrag.sequence))
				return false;
			if (!quality.equals(otherFrag.quality))
				return false;

			return true;
		}
		else
			return false;
	}

	public void readFields(DataInput in) throws IOException
	{
		// TODO:  reimplement with a serialization system (e.g. Avro)

		// serialization order:
		// 1) sequence
		// 2) quality
		// 3) int with flags indicating which fields are defined (see *_Present flags)
		// 4..end) the rest of the fields

		this.clear();

		sequence.readFields(in);
		quality.readFields(in);

		int presentFlags = WritableUtils.readVInt(in);
		if ( (presentFlags & Instrument_Present) != 0) instrument = WritableUtils.readString(in);
		if ( (presentFlags & RunNumber_Present) != 0) runNumber = WritableUtils.readVInt(in);
		if ( (presentFlags & FlowcellId_Present) != 0) flowcellId = WritableUtils.readString(in);
		if ( (presentFlags & Lane_Present) != 0) lane = WritableUtils.readVInt(in);
		if ( (presentFlags & Tile_Present) != 0) tile = WritableUtils.readVInt(in);
		if ( (presentFlags & Xpos_Present) != 0) xpos = WritableUtils.readVInt(in);
		if ( (presentFlags & Ypos_Present) != 0) ypos = WritableUtils.readVInt(in);
		if ( (presentFlags & Read_Present) != 0) read = WritableUtils.readVInt(in);
		if ( (presentFlags & FilterPassed_Present) != 0) filterPassed = WritableUtils.readVInt(in) == 1;
		if ( (presentFlags & ControlNumber_Present) != 0) controlNumber = WritableUtils.readVInt(in);
		if ( (presentFlags & IndexSequence_Present) != 0) indexSequence = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException
	{
		// TODO:  reimplement with a serialization system (e.g. Avro)

		sequence.write(out);
		quality.write(out);

		int presentFlags = 0;
		if (instrument != null) presentFlags |= Instrument_Present;
		if (runNumber != null) presentFlags |= RunNumber_Present;
		if (flowcellId != null) presentFlags |= FlowcellId_Present;
		if (lane != null) presentFlags |= Lane_Present;
		if (tile != null) presentFlags |= Tile_Present;
		if (xpos != null) presentFlags |= Xpos_Present;
		if (ypos != null) presentFlags |= Ypos_Present;
		if (read != null) presentFlags |= Read_Present;
		if (filterPassed != null) presentFlags |= FilterPassed_Present;
		if (controlNumber != null) presentFlags |= ControlNumber_Present;
		if (indexSequence != null) presentFlags |= IndexSequence_Present;
		
		WritableUtils.writeVInt(out, presentFlags);

		if (instrument != null) WritableUtils.writeString(out, instrument);
		if (runNumber != null) WritableUtils.writeVInt(out, runNumber);
		if (flowcellId != null) WritableUtils.writeString(out, flowcellId);
		if (lane != null) WritableUtils.writeVInt(out, lane);
		if (tile != null) WritableUtils.writeVInt(out, tile);
		if (xpos != null) WritableUtils.writeVInt(out, xpos);
		if (ypos != null) WritableUtils.writeVInt(out, ypos);
		if (read != null) WritableUtils.writeVInt(out, read);
		if (filterPassed != null) WritableUtils.writeVInt(out, filterPassed ? 1 : 0);
		if (controlNumber != null) WritableUtils.writeVInt(out, controlNumber);
		if (indexSequence != null) WritableUtils.writeString(out, indexSequence);
	}
}
