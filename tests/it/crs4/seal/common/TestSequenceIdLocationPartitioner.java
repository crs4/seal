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
import static org.junit.Assert.*;

import it.crs4.seal.common.SequenceId;
import it.crs4.seal.common.SequenceIdLocationPartitioner;

public class TestSequenceIdLocationPartitioner 
{
	private SequenceIdLocationPartitioner<?> partitioner;
	private SequenceId s1;
	private SequenceId s2;

	@Before
	public void setup()
	{
		partitioner = new SequenceIdLocationPartitioner();
		s1 = new SequenceId();
		s2 = new SequenceId();
	}

	@Test
	public void testSame()
	{
		s1.set("location1", 1);
		assertEquals(partitioner.getPartition(s1, null, 10), partitioner.getPartition(s1, null, 10));
	}

	@Test
	public void testDiffRead()
	{
		s1.set("location1", 1);
		s2.set("location1", 2);
		assertEquals(partitioner.getPartition(s1, null, 10), partitioner.getPartition(s2, null, 10));
	}

	@Test
	public void testDiffLocation()
	{
		s1.set("location1", 1);
		s2.set("1noitacollocation1", 2);
		assertFalse(partitioner.getPartition(s1, null, 10) == partitioner.getPartition(s2, null, 10));
	}


	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestSequenceIdLocationPartitioner.class.getName());
	}
}
