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


package tests.it.crs4.seal.read_sort;

import java.io.StringReader;

import org.junit.*;
import static org.junit.Assert.*;

import it.crs4.seal.read_sort.FastaChecksummer;
import it.crs4.seal.common.UnknownItemException;
import it.crs4.seal.common.FormatException;

public class TestFastaChecksummer
{
	private static final String fastaSample1 =
		"> name\n" +
		"AGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCT\n" +
		"TCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGA";

	private static final String fastaSample2 =
		">name\n" +
		"AGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCT\n" +
		"TCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGA";

	private static final String fastaSampleLongName =
		">name part2 part3\n" +
		"AGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCT\n" +
		"TCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGA";


	private static final String badFastaMissingHeader =
		"AGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCTAGCT\n" +
		"TCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGA";


	private FastaChecksummer emptyChecksummer;
	private FastaChecksummer loadedChecksummer;

	@Before
	public void setup() throws java.io.IOException
	{
		emptyChecksummer = new FastaChecksummer();

		loadedChecksummer = new FastaChecksummer();
		loadedChecksummer.setInput( new StringReader(fastaSample1) );
		loadedChecksummer.calculate();
	}

	@Test(expected=IllegalStateException.class)
	public void testNoInput() throws java.io.IOException
	{
		emptyChecksummer.calculate();
	}

	@Test(expected=IllegalStateException.class)
	public void testNoCalculate() throws java.io.IOException
	{
		emptyChecksummer.setInput( new StringReader(fastaSample1) );
		emptyChecksummer.iterator();
	}

	@Test
	public void testHasChecksum()
	{
		assertTrue(loadedChecksummer.hasChecksum("name"));
	}

	@Test
	public void testNoSpaceBeforeName() throws java.io.IOException
	{
		emptyChecksummer.setInput( new StringReader(fastaSample2) );
		emptyChecksummer.calculate();
		assertTrue(emptyChecksummer.hasChecksum("name"));
	}


	@Test
	public void testMultiPartName() throws java.io.IOException
	{
		emptyChecksummer.setInput( new StringReader(fastaSampleLongName) );
		emptyChecksummer.calculate();
		assertTrue(emptyChecksummer.hasChecksum("name"));
	}

	@Test(expected=UnknownItemException.class)
	public void testUnknownItem()
	{
		loadedChecksummer.getChecksum("unknown item");
	}

	@Test
	public void testChecksumIterator()
	{
		int count = 0;
		for (FastaChecksummer.ChecksumEntry entry: loadedChecksummer)
		{
			count += 1;
			assertEquals("name", entry.getName());
			assertEquals("fa3ee16b821a1e1fbd8d8381c0a5ba8d", entry.getChecksum());
		}
		assertEquals(1, count);
	}

	@Test(expected=FormatException.class)
	public void testMissingHeader() throws java.io.IOException
	{
		emptyChecksummer.setInput( new StringReader(badFastaMissingHeader) );
		emptyChecksummer.calculate();
	}

	@Test(expected=FormatException.class)
	public void testEmptyFasta() throws java.io.IOException
	{
		emptyChecksummer.setInput( new StringReader("") );
		emptyChecksummer.calculate();
	}
}
