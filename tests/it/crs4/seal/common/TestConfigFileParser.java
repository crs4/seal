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

import it.crs4.seal.common.ConfigFileParser;
import it.crs4.seal.common.FormatException;

import org.junit.*;
import static org.junit.Assert.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

public class TestConfigFileParser {

	ConfigFileParser emptyParser;
	ConfigFileParser parser;

	static final String CfgDefaultNoTitle =
		"key1:value1\n" + 
		"key2:value2\n";

	static final String CfgDefaultWithTitle =
		"[DEFAULT]\n" +
		"key1:value1\n" + 
		"key2:value2\n";

	static final String CfgSection1 =
		"[DEFAULT]\n" +
		"key1:value1\n" + 
		"key2:value2\n" + 
		"[ Section1 ]\n" +
		"keyS1: valueS1\n";

	static final String CfgSection2 =
		"[DEFAULT]\n" +
		"key1:value1\n" + 
		"key2:value2\n" + 
		"[ Section1 ]\n" +
		"keyS1: valueS1\n" +
		"[ Section2 ]\n" +
		"keyS2: valueS2\n";

	@Before
	public void setup()
	{
		emptyParser = new ConfigFileParser();
		parser = new ConfigFileParser();
	}

	@Test
	public void testEmpty()
	{
		assertFalse( emptyParser.hasSection("bla") );
		assertTrue( emptyParser.hasSection("DEFAULT") );
		assertTrue( emptyParser.getSectionNames().isEmpty() );
		Iterator<ConfigFileParser.KvPair > it = emptyParser.getSectionIterator("non existent");
		assertFalse( it.hasNext() );
	}

	@Test
	public void testDefault() throws IOException, FormatException
	{
		parser.load( new StringReader(CfgDefaultWithTitle) );
		assertTrue( parser.hasSection("DEFAULT") );
		assertTrue( parser.hasSection("default") );
		assertTrue( parser.hasSection("deFAult") );

		Map<String, String> map = toMap(parser.getSectionIterator("anything"));
		assertEquals(2, map.size());
		assertEquals("value1", map.get("key1"));
		assertEquals("value2", map.get("key2"));
	}

	@Test
	public void testEquals() throws IOException, FormatException
	{
		parser.load( new StringReader("key1=value1\n") );
		assertEquals("value1", parser.getValue("default", "key1"));
	}

	@Test
	public void testColon() throws IOException, FormatException
	{
		parser.load( new StringReader("key1:value1\n") );
		assertEquals("value1", parser.getValue("default", "key1"));
	}

	@Test
	public void testTrimKey() throws IOException, FormatException
	{
		parser.load( new StringReader("    key1          :value1\n") );
		assertEquals("value1", parser.getValue("default", "key1"));
	}

	@Test
	public void testTrimValue() throws IOException, FormatException
	{
		parser.load( new StringReader("key1:     value1       \n") );
		assertEquals("value1", parser.getValue("default", "key1"));
	}

	@Test
	public void testSection1() throws IOException, FormatException
	{
		parser.load( new StringReader(CfgSection1) );
		assertEquals("value1", parser.getValue("Section1", "key1"));
		assertEquals("valueS1", parser.getValue("Section1", "keyS1"));

		Map<String, String> map = toMap(parser.getSectionIterator("Section1"));
		// make sure the iterator goes through all k-v pairs, even the ones inherited from DEFAULT
		assertEquals(3, map.size());
		assertEquals("value1", map.get("key1"));
		assertEquals("value2", map.get("key2"));
		assertEquals("valueS1", map.get("keyS1"));
	}

	@Test
	public void testSection2() throws IOException, FormatException
	{
		parser.load( new StringReader(CfgSection2) );
		assertEquals("value1", parser.getValue("Section1", "key1"));
		assertEquals("valueS1", parser.getValue("Section1", "keyS1"));
		assertNull( parser.getValue("Section2", "keyS1"));
		assertEquals("valueS2", parser.getValue("Section2", "keyS2"));
	}

	@Test(expected=it.crs4.seal.common.FormatException.class)
	public void testSpaceInSectionName() throws IOException, FormatException
	{
		parser.load( new StringReader("[ Section 1 ]") );
	}

	@Test
	public void testHashComment() throws IOException, FormatException
	{
		parser.load( new StringReader(" #key1=value1\nkey2=value2;\n") );
		assertNull( parser.getValue("default", "key1"));
		assertEquals("value2;", parser.getValue("default", "key2"));
	}

	@Test
	public void testSemiColonComment() throws IOException, FormatException
	{
		parser.load( new StringReader(" ;key1=value1\nkey2=value2;\n") );
		assertNull( parser.getValue("default", "key1"));
		assertEquals("value2;", parser.getValue("default", "key2"));
	}

	private Map<String, String> toMap(Iterator<ConfigFileParser.KvPair> it)
	{
		Map<String, String> map = new HashMap<String, String>();
		while (it.hasNext())
		{
			ConfigFileParser.KvPair pair = it.next();
			map.put(pair.getKey(), pair.getValue());
		}

		return map;
	}
}
