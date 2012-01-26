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


package tests.it.crs4.seal.common;

import org.junit.*;
import static org.junit.Assert.*;

import it.crs4.seal.common.GroupByLocationComparator;
import it.crs4.seal.common.SequenceId;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

public class TestGroupByLocationComparator
{
	private GroupByLocationComparator comparator;
	private SequenceId s1;
	private SequenceId s2;
	private ByteArrayOutputStream byteStream1;
	private DataOutputStream oStream1;
	private ByteArrayOutputStream byteStream2;
	private DataOutputStream oStream2;

	@Before
	public void setup()
	{
		comparator = new GroupByLocationComparator();

		s1 = new SequenceId();
		s1.set("a", 1);
		s2 = new SequenceId();
		s1.set("a", 2);

		byteStream1 = new ByteArrayOutputStream();
		oStream1 = new DataOutputStream(byteStream1);
		byteStream2 = new ByteArrayOutputStream();
		oStream2 = new DataOutputStream(byteStream2);
	}

	private int writeAndCompareSequenceIds() throws java.io.IOException
	{
		s1.write(oStream1);
		s2.write(oStream2);
		oStream1.close();
		oStream2.close();
		return comparator.compare( byteStream1.toByteArray(), 0, byteStream1.size(), byteStream2.toByteArray(), 0, byteStream2.size() );
	}

	@Test
	public void testCompareS1ltS2() throws java.io.IOException
	{
		s1.set("a", 1);
		s2.set("b", 2);
		assertTrue( writeAndCompareSequenceIds() < 0);
	}

	@Test
	public void testCompareS1gtS2() throws java.io.IOException
	{
		s1.set("b", 1);
		s2.set("a", 2);
		assertTrue( writeAndCompareSequenceIds() > 0);
	}

	@Test
	public void testCompareS1eqS2() throws java.io.IOException
	{
		s1.set("a", 1);
		s2.set("a", 1);
		assertTrue( writeAndCompareSequenceIds() == 0);
	}

	@Test
	public void testCompareS1neS2ButGrouped1() throws java.io.IOException
	{
		s1.set("a", 1);
		s2.set("a", 2);
		assertTrue( writeAndCompareSequenceIds() == 0);
	}

	@Test
	public void testCompareS1neS2ButGrouped2() throws java.io.IOException
	{
		s1.set("a", 2);
		s2.set("a", 1);
		assertTrue( writeAndCompareSequenceIds() == 0);
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestGroupByLocationComparator.class.getName());
	}
}
