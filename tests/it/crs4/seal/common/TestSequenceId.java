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


package tests.it.crs4.seal.common;

import org.junit.*;
//import org.junit.runners.Suite;
import static org.junit.Assert.*;

import it.crs4.seal.common.SequenceId;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class TestSequenceId 
{
	private SequenceId seqid;
	private SequenceId s2;

	@Before
	public void setup()
	{
		seqid = new SequenceId();
	}

	@Test
	public void testDefaults()
	{
		assertEquals("", seqid.getLocation());
		assertEquals(127, seqid.getRead());
	}

	@Test
  public void testSetGood()
	{
		String location = "mylocation";
		int n = 5;
		seqid.set(location, n);
		assertEquals(location, seqid.getLocation());
		assertEquals(n, seqid.getRead());
	}

	@Test(expected=IllegalArgumentException.class)
  public void testSetBadReadNum1()
	{
		seqid.set(seqid.getLocation(), 0);
	}

	@Test(expected=IllegalArgumentException.class)
  public void testSetBadReadNum2()
	{
		seqid.set(seqid.getLocation(), 128);
	}

	@Test
	public void testEquals1()
	{
		s2 = new SequenceId();
		assertTrue(seqid.equals(s2));
		assertTrue(s2.equals(seqid));
	}

	@Test
	public void testEquals2()
	{
		s2 = new SequenceId();
		s2.set(s2.getLocation(), 5);
		assertFalse(seqid.equals(s2));
		assertFalse(s2.equals(seqid));
	}

	@Test
	public void testEquals3()
	{
		s2 = new SequenceId();
		s2.set("some location", s2.getRead());
		assertFalse(seqid.equals(s2));
		assertFalse(s2.equals(seqid));
	}

	@Test
	public void testCompare1()
	{
		s2 = new SequenceId();
		assertEquals(0, seqid.compareTo(s2));
		assertEquals(0, s2.compareTo(seqid));
	}

	@Test
	public void testCompare2()
	{
		s2 = new SequenceId();
		seqid.set("a", 1);
		s2.set("a", 2);
		assertTrue(seqid.compareTo(s2) < 0);
		assertTrue(s2.compareTo(seqid) > 0);
	}

	@Test
	public void testCompare3()
	{
		s2 = new SequenceId();
		seqid.set("a", 1);
		s2.set("b", 1);
		assertTrue(seqid.compareTo(s2) < 0);
		assertTrue(s2.compareTo(seqid) > 0);
	}

	@Test
	public void testIO() throws java.io.IOException
	{
		seqid.set("location", 2);

		ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
		DataOutputStream outputStream = new DataOutputStream(dataStream);
		seqid.write(outputStream);
		outputStream.close();

		byte[] raw_data = dataStream.toByteArray();

		ByteArrayInputStream inputByteStream = new ByteArrayInputStream(raw_data);
		DataInputStream inputStream = new DataInputStream(inputByteStream);
		s2 = new SequenceId();
		s2.readFields(inputStream);

		assertEquals(seqid, s2);
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestSequenceId.class.getName());
	}
}
