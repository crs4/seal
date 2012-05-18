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

import it.crs4.seal.common.AlignOp;
import it.crs4.seal.common.WritableMapping;

import java.util.List;
import java.util.Set;
import java.nio.ByteBuffer;

import org.junit.*;
import static org.junit.Assert.*;

public class TestWritableMapping
{
	private WritableMapping unmapped;
	private WritableMapping mapped;

	private String mappedSeq = "GGGGGGGGGG";
	private String mappedQual = "$$$$$$$$$$";

	@Before
	public void setup()
	{
		unmapped = new WritableMapping();

		mapped = new WritableMapping("read2", mappedSeq, mappedQual);
		mapped.setIsRead1(true);
		mapped.setIsMapped(true);
		mapped.setContig("contig");
		mapped.set5Position(123);
		mapped.setMapQ(30);
		mapped.setAlignment(AlignOp.scanCigar("" + mappedSeq.length() + "M"));
	}

	@Test
	public void testDefaultConstructor()
	{
		unmapped = new WritableMapping();

		assertNull(unmapped.getName());
		assertNull(unmapped.getSequence());
		assertNull(unmapped.getBaseQualities());

		assertEquals(0x4, unmapped.getFlag());
		assertFalse(unmapped.isTemplateLengthAvailable());
	}

	@Test
	public void testStringConstructor()
	{
		String seq = "AAAAAAAAAA";
		String qual = "##########";

		unmapped = new WritableMapping("read1", seq, qual);

		assertEquals(seq, unmapped.getSequenceString());
		assertEquals(qual, unmapped.getBaseQualitiesString());
	}

	@Test
	public void testName()
	{
		String name = "hello";
		unmapped.setName(name);
		assertEquals(name, unmapped.getName());
	}

	@Test
	public void testFlag()
	{
		int flag = 152;
		unmapped.setFlag(flag);
		assertEquals(flag, unmapped.getFlag());
	}

	@Test
	public void testContig()
	{
		assertEquals("contig", mapped.getContig());
	}

	@Test(expected=IllegalStateException.class)
	public void testUnmappedContig()
	{
		unmapped.getContig();
	}

	@Test
	public void testPosition()
	{
		assertEquals(123, mapped.get5Position());
	}

	@Test(expected=IllegalStateException.class)
	public void testUnmappedPosition()
	{
		unmapped.get5Position();
	}

	@Test
	public void testMapQ()
	{
		assertEquals(30, mapped.getMapQ());
	}

	@Test(expected=IllegalStateException.class)
	public void testUnmappedMapQ()
	{
		unmapped.getMapQ();
	}

	@Test
	public void testAlignment()
	{
		assertNotNull(mapped.getAlignment());
	}

	@Test(expected=IllegalStateException.class)
	public void testUnmappedAlignment()
	{
		unmapped.getAlignment();
	}

	@Test
	public void testLength()
	{
		assertEquals(mappedSeq.length(), mapped.getLength());
	}

	@Test
	public void testClear()
	{
		mapped.clear();

		assertNull(mapped.getName());
		assertNull(mapped.getSequence());
		assertNull(mapped.getBaseQualities());

		assertEquals(0x4, mapped.getFlag());
		assertFalse(mapped.isTemplateLengthAvailable());
	}

	@Test(expected=IllegalStateException.class)
	public void testUnmappedTemplateLength()
	{
		unmapped.setTemplateLength(33);
	}

	@Test(expected=IllegalStateException.class)
	public void testUnpairedTemplateLength()
	{
		mapped.setTemplateLength(33);
	}

	@Test(expected=IllegalStateException.class)
	public void testUnmappedMateTemplateLength()
	{
		mapped.setIsPaired(true);
		mapped.setIsMateUnmapped(true);
		mapped.setTemplateLength(33);
	}

	@Test
	public void testOkTemplateLength()
	{
		mapped.setIsPaired(true);
		mapped.setIsMateUnmapped(false);
		mapped.setTemplateLength(33);
		assertTrue(mapped.isTemplateLengthAvailable());
		assertEquals(33, mapped.getTemplateLength());
	}

	@Test
	public void testSetIsPaired()
	{
		unmapped.setIsPaired(true);
		assertTrue(unmapped.isPaired());
		unmapped.setIsPaired(false);
		assertFalse(unmapped.isPaired());
	}
 
	@Test
	public void testSetIsProperlyPaired()
	{
		unmapped.setIsProperlyPaired(true);
		assertTrue(unmapped.isProperlyPaired());
		unmapped.setIsProperlyPaired(false);
		assertFalse(unmapped.isProperlyPaired());
	}
 
	@Test
	public void testSetIsMapped()
	{
		unmapped.setIsMapped(true);
		assertTrue(unmapped.isMapped());
		unmapped.setIsMapped(false);
		assertFalse(unmapped.isMapped());
	}
 
	@Test
	public void testSetIsUnmapped()
	{
		unmapped.setIsUnmapped(true);
		assertTrue(unmapped.isUnmapped());
		unmapped.setIsUnmapped(false);
		assertFalse(unmapped.isUnmapped());
	}
 
	@Test
	public void testSetIsMateMapped()
	{
		unmapped.setIsMateMapped(true);
		assertTrue(unmapped.isMateMapped());
		unmapped.setIsMateMapped(false);
		assertFalse(unmapped.isMateMapped());
	}
 
	@Test
	public void testSetIsMateUnmapped()
	{
		unmapped.setIsMateUnmapped(true);
		assertTrue(unmapped.isMateUnmapped());
		unmapped.setIsMateUnmapped(false);
		assertFalse(unmapped.isMateUnmapped());
	}
 
	@Test
	public void testSetIsOnReverse()
	{
		unmapped.setIsOnReverse(true);
		assertTrue(unmapped.isOnReverse());
		unmapped.setIsOnReverse(false);
		assertFalse(unmapped.isOnReverse());
	}
 
	@Test
	public void testSetIsMateOnReverse()
	{
		unmapped.setIsMateOnReverse(true);
		assertTrue(unmapped.isMateOnReverse());
		unmapped.setIsMateOnReverse(false);
		assertFalse(unmapped.isMateOnReverse());
	}
 
	@Test
	public void testSetIsRead1()
	{
		unmapped.setIsRead1(true);
		assertTrue(unmapped.isRead1());
		unmapped.setIsRead1(false);
		assertFalse(unmapped.isRead1());
	}
 
	@Test
	public void testSetIsRead2()
	{
		unmapped.setIsRead2(true);
		assertTrue(unmapped.isRead2());
		unmapped.setIsRead2(false);
		assertFalse(unmapped.isRead2());
	}
 
	@Test
	public void testSetIsSecondaryAlign()
	{
		unmapped.setIsSecondaryAlign(true);
		assertTrue(unmapped.isSecondaryAlign());
		unmapped.setIsSecondaryAlign(false);
		assertFalse(unmapped.isSecondaryAlign());
	}
 
	@Test
	public void testSetIsFailedQC()
	{
		unmapped.setIsFailedQC(true);
		assertTrue(unmapped.isFailedQC());
		unmapped.setIsFailedQC(false);
		assertFalse(unmapped.isFailedQC());
	}
 
	@Test
	public void testSetIsDuplicate()
	{
		unmapped.setIsDuplicate(true);
		assertTrue(unmapped.isDuplicate());
		unmapped.setIsDuplicate(false);
		assertFalse(unmapped.isDuplicate());
	}

	@Test
	public void testSetTag() throws NoSuchFieldException
	{
		assertFalse(mapped.hasTag("XX"));
		mapped.setTag("XX", WritableMapping.TagDataType.Int, "22");
		assertEquals(22, mapped.getIntTag("XX"));
	}

	@Test
	public void testGetTagNames()
	{
		mapped.setTag("X1", WritableMapping.TagDataType.Int, "22");
		mapped.setTag("X2", WritableMapping.TagDataType.String, "bla");
		mapped.setTag("X3", WritableMapping.TagDataType.String, "lab");
		Set<String> tagSet = mapped.getTagNames();
		assertEquals(3, tagSet.size());
		assertTrue(tagSet.contains("X1"));
		assertTrue(tagSet.contains("X2"));
		assertTrue(tagSet.contains("X3"));
	}

	@Test
	public void testClearTags()
	{
		assertFalse(mapped.hasTag("XX"));
		mapped.setTag("XX", WritableMapping.TagDataType.Int, "22");
		assertTrue(mapped.hasTag("XX"));
		mapped.clear();
		assertFalse(mapped.hasTag("XX"));
	}
}
