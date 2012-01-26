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

import java.io.StringReader;
import java.io.IOException;

import it.crs4.seal.common.FormatException;
import it.crs4.seal.recab.RodFileVariantReader;
import it.crs4.seal.recab.VariantRegion;

public class TestRodFileVariantReader
{
	private String rodSample =
"585	1	10259	10260	rs72477211	0	+	C	C	A/G	genomic	single	unknown	0	0	unknown	exact	1\n" +
"585	1	10433	10433	rs56289060	0	+	-	-	-/C	genomic	insertion	unknown	0	0	unknown	between	1\n" +
"585	1	10937	10938	rs28853987	0	+	G	G	A/G	genomic	single	unknown	0	0	unknown	exact	1\n" +
"585	1	11013	11014	rs28484712	0	+	G	G	A/G	genomic	single	unknown	0	0	unknown	exact	1\n";

	private String numberFormatErrorSample =
		"585	1	abc	10260	rs72477211	0	+	C	C	A/G	genomic	single	unknown	0	0	unknown	exact	1";

	private RodFileVariantReader snpReader;
	private VariantRegion snp;

	@Before
	public void setup()
	{
		snp = new VariantRegion();
	}

	@Test
	public void testFilter() throws IOException
	{
		snpReader = new RodFileVariantReader( new StringReader(rodSample) );
		int count = 0;

		while (snpReader.nextEntry(snp))
			++count;
		assertEquals(3, count);
	}

	@Test
	public void testReading() throws IOException
	{
		snpReader = new RodFileVariantReader( new StringReader(rodSample) );

		assertTrue(snpReader.nextEntry(snp));
		assertEquals("1", snp.getContigName());
		assertEquals(10259, snp.getPosition());

		assertTrue(snpReader.nextEntry(snp));
		assertEquals("1", snp.getContigName());
		assertEquals(10937, snp.getPosition());
	}

	@Test(expected=FormatException.class)
	public void testEmpty() throws IOException
	{
		snpReader = new RodFileVariantReader( new StringReader("") );
		snpReader.nextEntry(snp);
	}

	@Test(expected=FormatException.class)
	public void testNumberFormatError() throws IOException
	{
		snpReader = new RodFileVariantReader( new StringReader(numberFormatErrorSample) );
		snpReader.nextEntry(snp);
	}
}
