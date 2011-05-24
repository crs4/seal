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


package tests.it.crs4.seal.demux;

import it.crs4.seal.common.SequenceId;
import it.crs4.seal.demux.TwoOneThreeSortComparator;

import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.*;
import static org.junit.Assert.*;

public class TestTwoOneThreeSortComparator
{
	private TwoOneThreeSortComparator cmp;
	private SequenceId s1;
	private SequenceId s2;

	private ByteArrayOutputStream bStream1;
	private ByteArrayOutputStream bStream2;
	private DataOutputStream dStream1;
	private DataOutputStream dStream2;

	private static final String location1 = "location1";
	private static final String location2 = "location2";

	private int bStream2StartPos;

	@Before
	public void setup()
	{
		cmp = new TwoOneThreeSortComparator();
		s1 = new SequenceId();
		s2 = new SequenceId();

		bStream1 = new ByteArrayOutputStream();
		bStream2 = new ByteArrayOutputStream();

		// write some junk at the start to avoid always starting from position 0
		bStream2.write(0xFF);
		bStream2StartPos = bStream2.size();

		dStream1 = new DataOutputStream(bStream1);
		dStream2 = new DataOutputStream(bStream2);
	}

	@Test
	public void testEq() throws IOException
	{
		s1.set(location1, 1);
		s2.set(location1, 1);
		assertEquals(0, writeAndCmp());
	}

	@Test
	public void testS1LocationBeforeS2() throws IOException
	{
		s1.set(location1, 1);
		s2.set(location2, 1);
		assertEquals(-1, writeAndCmp());
	}

	@Test
	public void testS1LocationAfterS2() throws IOException
	{
		s1.set(location2, 1);
		s2.set(location1, 1);
		assertEquals(1, writeAndCmp());
	}

	@Test
	public void testSameLoc_R1R2() throws IOException
	{
		s1.set(location1, 1);
		s2.set(location1, 2);
		assertEquals(1, writeAndCmp());
	}

	@Test
	public void testSameLoc_R1R3() throws IOException
	{
		s1.set(location1, 1);
		s2.set(location1, 3);
		assertEquals(-1, writeAndCmp());
	}

	@Test
	public void testSameLoc_R2R1() throws IOException
	{
		s1.set(location1, 2);
		s2.set(location1, 1);
		assertEquals(-1, writeAndCmp());
	}

	@Test
	public void testSameLoc_R2R3() throws IOException
	{
		s1.set(location1, 2);
		s2.set(location1, 3);
		assertEquals(-1, writeAndCmp());
	}

	@Test
	public void testSameLoc_R3R1() throws IOException
	{
		s1.set(location1, 3);
		s2.set(location1, 1);
		assertEquals(1, writeAndCmp());
	}

	@Test
	public void testSameLoc_R3R2() throws IOException
	{
		s1.set(location1, 3);
		s2.set(location1, 2);
		assertEquals(1, writeAndCmp());
	}

	private int writeAndCmp() throws IOException
	{
		writeSequenceIds();
		return cmpSequenceIds();
	}

	private int cmpSequenceIds()
	{
		return cmp.compare( bStream1.toByteArray(), 0, bStream1.size(), 
				                bStream2.toByteArray(), bStream2StartPos, bStream2.size()-bStream2StartPos );
	}

	private void writeSequenceIds() throws IOException
	{
		s1.write(dStream1);
		s2.write(dStream2);
		dStream1.close();
		dStream2.close();
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestTwoOneThreeSortComparator.class.getName());
	}
}
