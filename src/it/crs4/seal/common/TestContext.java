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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.io.Writable;

public class TestContext<K,V> implements IMRContext<K,V>
{
	private static class CounterStore
	{
		private HashMap<String, HashMap<String, Long> > counters;

		public CounterStore()
		{
			counters = new HashMap<String, HashMap<String, Long> >(5);
		}

		public long getValue(String group, String name)
		{
			HashMap<String, Long> map = counters.get(group);
			if (map != null)
			{
				Long value = map.get(name);
				if (value != null)
					return value;
			}
			return 0;
		}

		public void setValue(String group, String name, long value)
		{
			HashMap<String, Long> map = counters.get(group);
			if (map == null)
			{
				map = new HashMap<String, Long>();
				counters.put(group, map);
			}
			map.put(name, value);
		}
	}

	public LinkedHashMap<K, V> output;

	private int progressCalled;
	private String lastStatus;
	private CounterStore counters;

	public TestContext()
	{
		output = new LinkedHashMap<K,V>(30);
		progressCalled = 0;
		counters = new CounterStore();
	}

	public void progress()
	{
		progressCalled += 1;
	}

	public boolean getProgressCalled() { return progressCalled > 0; }
	public int getNumProgressCalls() { return progressCalled; }

	public void setStatus(String msg)
	{
		lastStatus = msg;
	}
	public String getLastStatus() { return lastStatus; }

	@SuppressWarnings("unchecked") 
	public void write(K key, V value) throws java.io.IOException, InterruptedException
	{
		if (output.containsKey(key))
			throw new RuntimeException("key " + key + " already exists in output!  Sorry.  TextContext isn't implemented to handle this");
    // duplicate the objects we store to sever dependencies to the mapper or reducer objects,
		// like the real Hadoop context does.
		output.put( (K)duplicateWritable(key), (V)duplicateWritable(value)); // unchecked casts that generate warnings
	}

	public int getNumWrites() { return output.size(); }

	public void increment(Enum<?> counter, long value)
	{
		increment(counter.getClass().getName(), counter.name(), value);
	}

	public void increment(String groupName, String counterName, long value)
	{
		long currentValue = counters.getValue(groupName, counterName);
		counters.setValue(groupName, counterName, currentValue + value);
	}

	public long getCounterValue(String groupName, String counterName)
	{
		return counters.getValue(groupName, counterName);
	}

	private Object duplicateWritable(Object oldItem) throws java.io.IOException
	{
		Writable w = (Writable) oldItem;
		// duplicate the key by serializing and then unserializing it
		ByteArrayOutputStream obytes = new ByteArrayOutputStream();
		DataOutputStream ostream = new DataOutputStream(obytes);

		w.write(ostream);
		ostream.close();

		Object newItem;
		try {
			newItem = oldItem.getClass().newInstance();
		}
		catch (Exception e)
		{
			throw new RuntimeException("error instantiating duplicate key or value.  Message: " + e.getMessage());
		}

		ByteArrayInputStream ibytes = new ByteArrayInputStream(obytes.toByteArray());
		DataInputStream istream = new DataInputStream(ibytes);
		((Writable)newItem).readFields(istream);
		return newItem;
	}
}
