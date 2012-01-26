// Copyright (C) 2011-2012 CRS4.
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

import org.junit.*;
import static org.junit.Assert.*;

import it.crs4.seal.recab.ObservationCount;

import java.io.IOException;
import java.io.DataInput;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import java.io.DataOutput;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

public class TestObservationCount
{
	private ObservationCount count;

	@Before
	public void setup()
	{
		count = new ObservationCount();
	}

	@Test
	public void testDefaultConstructor()
	{
		assertEquals(0L, count.getObservations());
		assertEquals(0L, count.getMismatches());
	}

	@Test
	public void testParameterizedConstructor()
	{
		ObservationCount c = new ObservationCount(66, 55);
		assertEquals(66, c.getObservations());
		assertEquals(55, c.getMismatches());
	}

	@Test(expected=IllegalArgumentException.class)
	public void testConstructNegativeObservations()
	{
		new ObservationCount(-1, 22);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testConstructNegativeMismatches()
	{
		new ObservationCount(22, -1);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testConstructMoreObsThanMis()
	{
		new ObservationCount(22, 44);
	}

	@Test
	public void testEquals()
	{
		assertEquals(new ObservationCount(55, 44), new ObservationCount(55, 44));
		assertFalse( (new ObservationCount(66, 55)).equals(new ObservationCount(66, 1)) );
		assertFalse( (new ObservationCount(66, 55)).equals(new ObservationCount(60, 55)) );
	}

	@Test
	public void testSet()
	{
		count.set(21, 12);
		assertEquals(21, count.getObservations());
		assertEquals(12, count.getMismatches());
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSetNegativeObservations()
	{
		count.set(-1, 22);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSetNegativeMismatches()
	{
		count.set(22, -1);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSetMoreObsThanMis()
	{
		count.set(22, 44);
	}

	@Test
	public void testSetWithObservationCount()
	{
		ObservationCount one = new ObservationCount(5, 3);
		ObservationCount two = new ObservationCount(7, 1);

		one.set(two);
		assertEquals(two, one);
		assertNotSame(one, two);
	}

	@Test
	public void testAddToThis()
	{
		ObservationCount one = new ObservationCount(5, 3);
		ObservationCount two = new ObservationCount(7, 1);

		ObservationCount result = one.addToThis(two);
		assertSame(one, result);

		assertEquals(7+5, one.getObservations());
		assertEquals(3+1, one.getMismatches());
	}

	///////////////////////////////////////////////////////////////
	// serialization
	///////////////////////////////////////////////////////////////
	private static ObservationCount cloneBySerialization(ObservationCount original) throws IOException
	{
		ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
		DataOutputStream dataOutput = new DataOutputStream(outputBuffer);
		original.write(dataOutput);
		dataOutput.close();

		ObservationCount newCount = new ObservationCount();
		newCount.readFields( new DataInputStream( new ByteArrayInputStream(outputBuffer.toByteArray())));

		return newCount;
	}

	@Test
	public void testSerialization() throws IOException
	{
		count = new ObservationCount(66, 55);
		assertEquals(count, cloneBySerialization(count));
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestObservationCount.class.getName());
	}
}
