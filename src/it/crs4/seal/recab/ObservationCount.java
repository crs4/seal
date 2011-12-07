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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;


public class ObservationCount implements Writable
{
	protected long observations;
	protected long mismatches;

	public ObservationCount()
	{
		set(0,0);
	}

	public ObservationCount(long obs, long mis)
	{
		set(obs, mis);
	}

	public long getObservations() { return observations; }
	public long getMismatches() { return mismatches; }

	public void set(long obs, long mis)
	{
		if (obs < 0)
			throw new IllegalArgumentException("Negative observations");
		if (mis < 0)
			throw new IllegalArgumentException("Negative mismatches");
		if (obs < mis)
			throw new IllegalArgumentException("More mismatches than observations!");

		observations = obs;
		mismatches = mis;
	}

	/**
	 * Set this to equal other.
	 */
	public void set(ObservationCount other)
	{
		this.observations = other.observations;
		this.mismatches = other.mismatches;
	}

	/**
	 * Add the counts in other to this ObservationCount.
	 *
	 * @return this ObservationCount, modified.
	 */
	public ObservationCount addToThis(ObservationCount other)
	{
		this.mismatches += other.mismatches;
		this.observations += other.observations;
		return this;
	}

	public void readFields(DataInput in) throws IOException
	{
		observations = WritableUtils.readVLong(in);
		mismatches = WritableUtils.readVLong(in);
	}

	public void write(DataOutput out) throws IOException
	{
		WritableUtils.writeVLong(out, observations);
		WritableUtils.writeVLong(out, mismatches);
	}

	public boolean equals(Object other)
	{
		if (other instanceof ObservationCount)
		{
			ObservationCount otherCount = (ObservationCount)other;
			if (observations == otherCount.observations && mismatches == otherCount.mismatches)
				return true;
			else
				return false;
		}
		else
			return false;
	}

	public String toString()
	{
		return "(" + observations + " observations, " + mismatches + " mismatches)";
	}
}
