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

import java.io.StringReader;

import org.junit.*;
import static org.junit.Assert.*;

import it.crs4.seal.recab.SnpTable;
import it.crs4.seal.common.FormatException;

public class TestSnpTable
{
	private SnpTable emptyTable;

	@Before
	public void setup()
	{
		emptyTable = new SnpTable();
	}

	@Test(expected=FormatException.class)
	public void testLoadEmpty() throws java.io.IOException
	{
		emptyTable.load( new StringReader("") );
	}

	@Test
	public void testDontLoadCdna() throws java.io.IOException
	{
		emptyTable.load( new StringReader("585	1	14435	14436	rs1045951	0	-	G	G	C/T	cDNA	single	unknown	0	0	unknown	exact	3") );
		assertFalse( emptyTable.isSnpLocation("1", 14435));
	}

	@Test
	public void testDontLoadNonSingle() throws java.io.IOException
	{
		emptyTable.load( new StringReader("585	1	14435	14436	rs1045951	0	-	G	G	C/T	genomic	insertion	unknown	0	0	unknown	exact	3") );
		assertFalse( emptyTable.isSnpLocation("1", 14435));
	}

	@Test
	public void testDontLoadNonExact() throws java.io.IOException
	{
		emptyTable.load( new StringReader("585	1	14435	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	between	3") );
		assertFalse( emptyTable.isSnpLocation("1", 14435));
	}

	@Test
	public void testDontLoadIfLongerThan1() throws java.io.IOException
	{
		emptyTable.load( new StringReader("585	1	14435	14437	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3") );
		assertFalse( emptyTable.isSnpLocation("1", 14435));
	}

	@Test
	public void testSimple() throws java.io.IOException
	{
		emptyTable.load( new StringReader("585	chr1	14435	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3") );
		assertTrue( emptyTable.isSnpLocation("chr1", 14435));
	}

	@Test
	public void testMultiple() throws java.io.IOException
	{
		String data = 
"585	1	14435	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3\n" +
"585	1	10259	10260	rs72477211	0	+	C	C	A/G	genomic	single	unknown	0	0	unknown	exact	1";
		emptyTable.load( new StringReader(data) );
		assertTrue( emptyTable.isSnpLocation("1", 14435));
		assertTrue( emptyTable.isSnpLocation("1", 10259));
	}

	@Test(expected=RuntimeException.class)
	public void testPositionTooBig() throws java.io.IOException
	{
		emptyTable.load( new StringReader("585	1	14435	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3") );
		emptyTable.isSnpLocation("1", Integer.MAX_VALUE + 1L);
	}

	@Test(expected=FormatException.class)
	public void testBadCoord() throws java.io.IOException
	{
		emptyTable.load( new StringReader("585	1	aaa	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3") );
	}

	@Test(expected=FormatException.class)
	public void testBadCoord2() throws java.io.IOException
	{
		emptyTable.load( new StringReader("585	1	14435	aaa	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3") );
	}

	@Test(expected=RuntimeException.class)
	public void testQueryTooBig() throws java.io.IOException
	{
		emptyTable.load( new StringReader("585	1	14435	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3") );
		emptyTable.isSnpLocation("1", Integer.MAX_VALUE + 1L);
	}

	@Test(expected=RuntimeException.class)
	public void testCoordTooBig() throws java.io.IOException
	{
		emptyTable.load( new StringReader("585	1	4500000000	4500000001	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3") );
	}
}
