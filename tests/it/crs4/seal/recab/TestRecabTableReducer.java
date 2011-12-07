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

package tests.it.crs4.seal.recab;

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.TestContext;
import it.crs4.seal.recab.RecabTable;
import it.crs4.seal.recab.RecabTableReducer;
import it.crs4.seal.recab.ObservationCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.*;
import static org.junit.Assert.*;

public class TestRecabTableReducer
{
	private RecabTableReducer reducer;
	private TestContext<Text, Text> context;
	private Configuration conf;

	@Before
	public void setup()
	{
		conf = new Configuration();
		context = new TestContext<Text, Text>();
		reducer = new RecabTableReducer();
	}

	@Test
	public void testSetup()
	{
		reducer.setup(conf);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSetupNegSmoothing()
	{
		conf.set(RecabTableReducer.CONF_SMOOTHING, "-1");
		reducer.setup(conf);
	}

	@Test
	public void testSimpleReduce() throws IOException, InterruptedException
	{
		reducer.setup(conf);

		Text key = TestRecabTableMapper.prepKey("rg", "30", "1", "AG");
		List<ObservationCount> values = Arrays.asList( new ObservationCount(3,1) );

		reducer.reduce(key, values, context);

		Set<Text> keys = context.getKeys();
		assertEquals(1, keys.size());
		assertNull( keys.iterator().next() );

		List<Text> emittedValues = context.getAllValues();
		assertEquals(1, emittedValues.size());
		Text output = emittedValues.get(0);

		String expected = key.toString() + "3" + RecabTable.TableDelim + "1" + RecabTable.TableDelim + "3";
		assertEquals(expected, output.toString());
	}

	@Test
	public void testReduceMoreObs() throws IOException, InterruptedException
	{
		reducer.setup(conf);

		Text key = TestRecabTableMapper.prepKey("rg", "30", "1", "AG");
		List<ObservationCount> values = Arrays.asList( 
				new ObservationCount(43,5), 
				new ObservationCount(34,10), 
				new ObservationCount(23,5) );

		reducer.reduce(key, values, context);

		Set<Text> keys = context.getKeys();
		assertEquals(1, keys.size());
		assertNull( keys.iterator().next() );

		List<Text> emittedValues = context.getAllValues();
		assertEquals(1, emittedValues.size());
		Text output = emittedValues.get(0);

		String expected = key.toString() + "100" + RecabTable.TableDelim + "20" + RecabTable.TableDelim + "7";
		assertEquals(expected, output.toString());
	}

	@Test
	public void testDifferentSmoothing() throws IOException, InterruptedException
	{
		conf.set(RecabTableReducer.CONF_SMOOTHING, "10");
		reducer.setup(conf);

		Text key = TestRecabTableMapper.prepKey("rg", "30", "1", "AG");
		List<ObservationCount> values = Arrays.asList( new ObservationCount(3,1) );

		reducer.reduce(key, values, context);

		List<Text> emittedValues = context.getAllValues();
		assertEquals(1, emittedValues.size());
		Text output = emittedValues.get(0);

		String expected = key.toString() + "3" + RecabTable.TableDelim + "1" + RecabTable.TableDelim + "1";
		assertEquals(expected, output.toString());
	}


	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestRecabTableReducer.class.getName());
	}
}
