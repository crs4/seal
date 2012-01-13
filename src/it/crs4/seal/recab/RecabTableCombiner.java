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

import it.crs4.seal.common.IMRContext;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class RecabTableCombiner
{
	protected ObservationCount sum = new ObservationCount();
	
	public void setup(Configuration conf)
	{
	}

	public void reduce(Text key, Iterable<ObservationCount> values, IMRContext<Text, ObservationCount> context) throws IOException, InterruptedException
	{
		sum.set(0,0);

		for (ObservationCount counts: values)
			sum.addToThis(counts);

		if (sum.getObservations() > 0)
			context.write(key, sum);
	}
}
