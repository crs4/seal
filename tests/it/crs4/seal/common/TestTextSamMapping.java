// Copyright (C) 2011-2016 CRS4.
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

import it.crs4.seal.common.FormatException;
import it.crs4.seal.common.TextSamMapping;

import org.apache.hadoop.io.Text;
import java.nio.ByteBuffer;

public class TestTextSamMapping
{
	@Ignore // tell JUnit to ignore this class
	private static class MapMule extends TextSamMapping {
		public MapMule(Text sam) {
			super(sam);
		}

		public String publicGetTagText(String name) {
			return getTagText(name);
		}
	}

	private static final String sam = "ERR020229.100000/1	89	chr6	3558357	37	91M	=	3558678	400	AGCTTCTTTGACTCTCGAATTTTAGCACTAGAAGAAATAGTGAGGATTATATATTTCAGAAGTTCTCACCCAGGATATCAGAACACATTCA	5:CB:CCBCCB>:C@;BBBB??B;?>1@@=C=4ACCAB3A8=CC=C?CBC=CBCCCCCCCCCCCCC@5>?=?CAAB=3=>====5>=AC?C	XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:91";

	private static final String unmapped = "UNMAPPED	93	*	*	0	*	=	3558678	*	AGCTT	5:CB:";

	private static final String differentSeqLengths = "UNMAPPED	93	*	*	0	*	=	3558678	*	ATT	5:CB:";

	@Test
	public void testFields() throws java.nio.charset.CharacterCodingException
	{
		TextSamMapping map = new TextSamMapping(new Text(sam));

		assertEquals("ERR020229.100000/1", map.getName());
		assertEquals(89, map.getFlag());
		assertEquals("chr6", map.getContig());
		assertEquals(3558357, map.get5Position());
		assertEquals(37, map.getMapQ());
		assertEquals("91M", map.getCigarStr());
		assertEquals(91, map.getLength());

		ByteBuffer buf = map.getSequence();
 		String s = Text.decode(buf.array(), buf.position(), map.getLength());
		assertEquals("AGCTTCTTTGACTCTCGAATTTTAGCACTAGAAGAAATAGTGAGGATTATATATTTCAGAAGTTCTCACCCAGGATATCAGAACACATTCA", s);

		buf = map.getBaseQualities();
		s = Text.decode(buf.array(), buf.position(), map.getLength());
		assertEquals("5:CB:CCBCCB>:C@;BBBB??B;?>1@@=C=4ACCAB3A8=CC=C?CBC=CBCCCCCCCCCCCCC@5>?=?CAAB=3=>====5>=AC?C", s);
	}

	@Test(expected=FormatException.class)
	public void testEmpty()
	{
		TextSamMapping map = new TextSamMapping(new Text(""));
	}

	@Test(expected=FormatException.class)
	public void testMissingFields()
	{
		TextSamMapping map = new TextSamMapping(new Text("ERR020229.100000/1	89	chr6	3558357	37	91M	=	3558678	400"));
	}

	@Test(expected=FormatException.class)
	public void testBadField()
	{
		TextSamMapping map = new TextSamMapping(new Text(sam.replace("89", "bla")));
	}

	@Test(expected=FormatException.class)
	public void testDifferentSeqLengths()
	{
		TextSamMapping map = new TextSamMapping(new Text(differentSeqLengths));
	}

	@Test
	public void testSimpleGetTagText()
	{
		MapMule m = new MapMule(new Text(sam));
		assertEquals("AM:i:0", m.publicGetTagText("AM"));
	}

	@Test
	public void testGetTextLastTag()
	{
		MapMule m = new MapMule(new Text(sam));
		assertEquals("MD:Z:91", m.publicGetTagText("MD"));
	}

	@Test
	public void testGetInexistentTagText()
	{
		MapMule m = new MapMule(new Text(sam));
		assertNull(m.publicGetTagText("NULL"));
	}

	@Test
	public void testNoTags()
	{
		MapMule m = new MapMule(new Text(unmapped));
		assertNull(m.publicGetTagText("NULL"));
	}

	@Test
	public void testUnmapped()
	{
		new TextSamMapping( new Text(unmapped) );
	}

	@Test(expected=IllegalStateException.class)
	public void testUnmappedGetContig()
	{
		TextSamMapping map = new TextSamMapping(new Text(unmapped));
		map.getContig();
	}

	@Test(expected=IllegalStateException.class)
	public void testUnmappedGet5Pos()
	{
		TextSamMapping map = new TextSamMapping(new Text(unmapped));
		map.get5Position();
	}

	@Test(expected=IllegalStateException.class)
	public void testUnmappedGetCigarStr()
	{
		TextSamMapping map = new TextSamMapping(new Text(unmapped));
		map.getCigarStr();
	}

	@Test(expected=IllegalStateException.class)
	public void testUnmappedGetAlignment()
	{
		TextSamMapping map = new TextSamMapping(new Text(unmapped));
		map.getAlignment();
	}

	@Test
	public void testDontDependOnOriginalData()
	{
		Text source = new Text(sam);
		TextSamMapping map = new TextSamMapping(source);
		source.set("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
		source.clear();
		assertEquals("AGCTTCTTTGACTCTCGAATTTTAGCACTAGAAGAAATAGTGAGGATTATATATTTCAGAAGTTCTCACCCAGGATATCAGAACACATTCA", map.getSequenceString());
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestTextSamMapping.class.getName());
	}
}
