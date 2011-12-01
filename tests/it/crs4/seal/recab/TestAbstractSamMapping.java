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

import it.crs4.seal.recab.AbstractSamMapping;
import it.crs4.seal.recab.AlignOp;
import it.crs4.seal.recab.AlignOp.AlignOpType;
import it.crs4.seal.common.FormatException;

import java.util.List;
import java.nio.ByteBuffer;

import org.junit.*;
import static org.junit.Assert.*;

public class TestAbstractSamMapping
{

	private static final String sam = "ERR020229.100000/1	89	chr6	3558357	37	91M	=	3558678	400	AGCTTCTTTGACTCTCGAATTTTAGCACTAGAAGAAATAGTGAGGATTATATATTTCAGAAGTTCTCACCCAGGATATCAGAACACATTCA	5:CB:CCBCCB>:C@;BBBB??B;?>1@@=C=4ACCAB3A8=CC=C?CBC=CBCCCCCCCCCCCCC@5>?=?CAAB=3=>====5>=AC?C	XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:91";

	private static final String samRead2 = "ERR020229.11100	163	chr2	207301655	60	91M	=	207302028	464	CACCCAAGAAATGTGTTGAATAAATGAATCAGGAGAGGCTGGTTAGCACTGTGCAGGGAGAGTGCCTTGCCTGTGATCTCTGCCAGTCGAC	GGGGGGGGFGGGGGGGGGGFGGGGGGGFGGGGGGGGGGG?EE5?=16450A?A@:9<A#################################	XT:A:U	NM:i:0	SM:i:37	AM:i:37	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:91";

	private static final String samUnmapped = "ERR020229.11100	79	*	*	0	*	*	*	*	CACCCAAGAAATGTGTTGAATAAATGAATCAGGAGAGGCTGGTTAGCACTGTGCAGGGAGAGTGCCTTGCCTGTGATCTCTGCCAGTCGAC	GGGGGGGGFGGGGGGGGGGFGGGGGGGFGGGGGGGGGGG?EE5?=16450A?A@:9<A#################################";

	@Ignore // tell JUnit not to try to instantiate this class
	private static class SimpleSamMapping extends AbstractSamMapping {
		private String source;
		private String[] fields;

		public SimpleSamMapping(String sam) {
			source = sam;
			fields = source.split("\t");
		}

		public String getName() { return fields[0]; }
		public int getFlag() { return Integer.parseInt(fields[1]); }
		public String getContig() { return fields[2]; }
		public long get5Position() { return Long.parseLong(fields[3]); }
		public byte getMapQ() { return Byte.parseByte(fields[4]); }
		public String getCigarStr() { return fields[5]; }
		public ByteBuffer getSequence() { return ByteBuffer.wrap(fields[9].getBytes()); }
		public ByteBuffer getBaseQualities() { return ByteBuffer.wrap(fields[10].getBytes()); }
		public int getLength() { return fields[9].length(); }

		protected String getTagText(String name)
		{
			for (int i = 11; i < fields.length; ++i)
			{
				if (fields[i].startsWith(name + ":"))
					return fields[i];
			}
			return null;
		}
	}

	private SimpleSamMapping simpleMapping;

	@Before
	public void setup()
	{
		simpleMapping = new SimpleSamMapping(sam);
	}

	@Test
	public void testSimpleGetAlignment()
	{
		List<AlignOp> list = simpleMapping.getAlignment();
		assertEquals(1, list.size());
		AlignOp align = list.get(0);
		assertEquals(AlignOpType.Match, align.getOp());
		assertEquals(91, align.getLen());
	}

	@Test
	public void testUnmappedGetAlignment()
	{
		simpleMapping = new SimpleSamMapping(samUnmapped);
		assertTrue(simpleMapping.getAlignment().isEmpty());
	}

	@Test
	public void testGetAlignmentElements()
	{
		String moreSam = "id	99	chr11	1	60	10M	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertEquals(new AlignOp(AlignOpType.Match, 10), simpleMapping.getAlignment().get(0));
		// repeat to test cached value
		assertEquals(new AlignOp(AlignOpType.Match, 10), simpleMapping.getAlignment().get(0));

		moreSam = "id	99	chr11	1	60	10I	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertEquals(new AlignOp(AlignOpType.Insert, 10), simpleMapping.getAlignment().get(0));

		moreSam = "id	99	chr11	1	60	10D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertEquals(new AlignOp(AlignOpType.Delete, 10), simpleMapping.getAlignment().get(0));

		moreSam = "id	99	chr11	1	60	10S	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertEquals(new AlignOp(AlignOpType.SoftClip, 10), simpleMapping.getAlignment().get(0));

		moreSam = "id	99	chr11	1	60	10H	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertEquals(new AlignOp(AlignOpType.HardClip, 10), simpleMapping.getAlignment().get(0));

		moreSam = "id	99	chr11	1	60	10N	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertEquals(new AlignOp(AlignOpType.Skip, 10), simpleMapping.getAlignment().get(0));

		moreSam = "id	99	chr11	1	60	10P	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertEquals(new AlignOp(AlignOpType.Pad, 10), simpleMapping.getAlignment().get(0));
	}


	@Test
	public void testComplexGetAlignment()
	{
		String moreSam = "id	99	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);

		List<AlignOp> list = simpleMapping.getAlignment();
		assertEquals(4, list.size());

		AlignOp align = list.get(0);
		assertEquals(AlignOpType.Match, align.getOp());
		assertEquals(3, align.getLen());

		align = list.get(1);
		assertEquals(AlignOpType.Insert, align.getOp());
		assertEquals(1, align.getLen());

		align = list.get(2);
		assertEquals(AlignOpType.Match, align.getOp());
		assertEquals(5, align.getLen());

		align = list.get(3);
		assertEquals(AlignOpType.Delete, align.getOp());
		assertEquals(1, align.getLen());
	}

	@Test(expected=FormatException.class)
	public void testInvalidCigar1()
	{
		String moreSam = "id	99	chr11	1	60	pippo	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		simpleMapping.getAlignment();
	}

	@Test(expected=FormatException.class)
	public void testInvalidCigar2()
	{
		String moreSam = "id	99	chr11	1	60	10	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		simpleMapping.getAlignment();
	}

	@Test(expected=FormatException.class)
	public void testInvalidCigar3()
	{
		String moreSam = "id	99	chr11	1	60	M	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		simpleMapping.getAlignment();
	}



	//
	// get*Tag are repeated more than once to exercise the caching mechanism
	//
	@Test
	public void testGetTag() throws NoSuchFieldException
	{
		assertEquals("91", simpleMapping.getTag("MD"));
		assertEquals("91", simpleMapping.getTag("MD"));
		assertEquals("U", simpleMapping.getTag("XT"));
		assertEquals("37", simpleMapping.getTag("SM"));
	}

	@Test(expected=NoSuchFieldException.class)
	public void testGetInexistantTag() throws NoSuchFieldException
	{
		simpleMapping.getTag("XX");
	}

	@Test
	public void testHasTag()
	{
		assertTrue(simpleMapping.hasTag("MD"));
		assertFalse(simpleMapping.hasTag("XX"));
	}

	@Test
	public void testIntTag() throws NoSuchFieldException
	{
		assertEquals(37, simpleMapping.getIntTag("SM"));
		assertEquals(37, simpleMapping.getIntTag("SM"));
	}

	@Test(expected=NumberFormatException.class)
	public void testBadIntTag() throws NoSuchFieldException
	{
		simpleMapping.getIntTag("XT");
	}

	@Test(expected=NumberFormatException.class)
	public void testBadDoubleTag() throws NoSuchFieldException
	{
		simpleMapping.getDoubleTag("XT");
	}
	
	@Test
	public void testDoubleTag() throws NoSuchFieldException
	{
		assertEquals(1, simpleMapping.getDoubleTag("X0"), 0.001);
		assertEquals(1, simpleMapping.getDoubleTag("X0"), 0.001);
	}

	/// flag tests

	@Test
	public void testPaired()
	{
		String moreSam = "id	67	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertTrue(simpleMapping.isPaired());
		assertTrue(simpleMapping.isProperlyPaired());

		moreSam = "id	0	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertFalse(simpleMapping.isPaired());
		assertFalse(simpleMapping.isProperlyPaired());
	}

	@Test
	public void testMapped()
	{
		String moreSam = "id	67	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertTrue(simpleMapping.isMapped());
		assertTrue(simpleMapping.isMateMapped());
		assertFalse(simpleMapping.isUnmapped());
		assertFalse(simpleMapping.isMateUnmapped());

		moreSam = "id	79	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertFalse(simpleMapping.isMapped());
		assertFalse(simpleMapping.isMateMapped());
		assertTrue(simpleMapping.isUnmapped());
		assertTrue(simpleMapping.isMateUnmapped());
	}

	@Test
	public void testStrand()
	{
		String moreSam = "id	67	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertFalse(simpleMapping.isOnReverse());
		assertFalse(simpleMapping.isMateOnReverse());

		moreSam = "id	115	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertTrue(simpleMapping.isOnReverse());
		assertTrue(simpleMapping.isMateOnReverse());
	}

	@Test
	public void testReadNumber()
	{
		String moreSam = "id	67	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertTrue(simpleMapping.isRead1());
		assertFalse(simpleMapping.isRead2());

		moreSam = "id	131	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertFalse(simpleMapping.isRead1());
		assertTrue(simpleMapping.isRead2());
	}

	@Test
	public void testSecondaryAlignment()
	{
		String moreSam = "id	67	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertFalse(simpleMapping.isSecondaryAlign());

		moreSam = "id	387	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertTrue(simpleMapping.isSecondaryAlign());
	}

	@Test
	public void testQcFlag()
	{
		String moreSam = "id	67	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertFalse(simpleMapping.isFailedQC());

		moreSam = "id	512	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertTrue(simpleMapping.isFailedQC());
	}

	@Test
	public void testDuplicateFlag()
	{
		String moreSam = "id	67	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertFalse(simpleMapping.isDuplicate());

		moreSam = "id	1024	chr11	1	60	3M1I5M1D	=	31	40	AGGAGAGGAG	1234512345";
		simpleMapping = new SimpleSamMapping(moreSam);
		assertTrue(simpleMapping.isDuplicate());
	}
}
