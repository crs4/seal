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

package tests.it.crs4.it.common;

import org.junit.*;
import static org.junit.Assert.*;

import it.crs4.seal.common.LineReader;

import org.apache.hadoop.io.Text;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class TestLineReader
{
	public static final String input10 = "0123456789";
	public static final String input22 = "0123456789\n0987654321\n";

	private LineReader reader;
	private Text dest = new Text();

	@Test
	public void testReadBufferedLine() throws IOException
	{
		reader = new LineReader(new ByteArrayInputStream(input22.getBytes()), 22);
		reader.readLine(dest);
		assertEquals("0123456789", dest.toString());
	}

	@Test
	public void testSkipOnBufferedLine() throws IOException
	{
		reader = new LineReader(new ByteArrayInputStream(input22.getBytes()), 22);
		long skipped = reader.skip(1);
		assertEquals(1, skipped);
		reader.readLine(dest);
		assertEquals("123456789", dest.toString());
	}

	@Test
	public void testReadBeyondBuffer() throws IOException
	{
		reader = new LineReader(new ByteArrayInputStream(input22.getBytes()), 5);
		reader.readLine(dest);
		assertEquals("0123456789", dest.toString());
	}

	@Test
	public void testSkipBeyondBuffer() throws IOException
	{
		reader = new LineReader(new ByteArrayInputStream(input22.getBytes()), 5);
		long skipped = reader.skip(11);
		assertEquals(11, skipped);
		reader.readLine(dest);
		assertEquals("0987654321", dest.toString());
	}

	@Test
	public void testSkipBeyondInput() throws IOException
	{
		reader = new LineReader(new ByteArrayInputStream(input10.getBytes()), 5);
		long skipped = reader.skip(11);
		assertEquals(10, skipped);

		skipped = reader.skip(11);
		assertEquals(0, skipped);
	}

}
