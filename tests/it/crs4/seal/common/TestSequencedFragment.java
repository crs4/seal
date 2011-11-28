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

import it.crs4.seal.common.SequencedFragment;

import java.io.IOException;
import java.io.DataInput;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import java.io.DataOutput;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.io.Text;

public class TestSequencedFragment
{
	private SequencedFragment frag;
	private SequencedFragment frag2;

	@Before
	public void setup()
	{
		frag = new SequencedFragment();
		frag2 = new SequencedFragment();
	}

	@Test
	public void testInitialState()
	{
		assertNotNull(frag.getSequence());
		assertNotNull(frag.getQuality());

		assertNull(frag.getInstrument());
		assertNull(frag.getRunNumber());
		assertNull(frag.getFlowcellId());
		assertNull(frag.getLane());
		assertNull(frag.getTile());
		assertNull(frag.getXpos());
		assertNull(frag.getYpos());
		assertNull(frag.getRead());
		assertNull(frag.getFilterPassed());
		assertNull(frag.getControlNumber());
		assertNull(frag.getIndexSequence());

		assertNotNull(frag.toString());
	}

	@Test(expected=IllegalArgumentException.class)
	public void testNoNullSequence()
	{
		frag.setSequence(null);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testNoNullQuality()
	{
		frag.setQuality(null);
	}

	///////////////////////////////////////////////////////////////
	// equals
	///////////////////////////////////////////////////////////////
	@Test
	public void testEquals()
	{
		assertTrue(frag.equals(frag2));

		frag.getSequence().append("AAAA".getBytes(), 0, 4);
		assertFalse( frag.equals(frag2) );
	}

	@Test
	public void testEqualsSequence()
	{
		frag.getSequence().append("AAAA".getBytes(), 0, 4);
		assertFalse( frag.equals(frag2) );
		frag2.getSequence().append("AAAA".getBytes(), 0, 4);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsQuality()
	{
		frag.getQuality().append("AAAA".getBytes(), 0, 4);
		assertFalse( frag.equals(frag2) );
		frag2.getQuality().append("AAAA".getBytes(), 0, 4);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsInstrument()
	{
		frag.setInstrument("instrument");
		assertFalse( frag.equals(frag2) );
		frag2.setInstrument("instrument");
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsRunNumber()
	{
		frag.setRunNumber(240);
		assertFalse( frag.equals(frag2) );
		frag2.setRunNumber(240);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsFlowcellId()
	{
		frag.setFlowcellId("id");
		assertFalse( frag.equals(frag2) );
		frag2.setFlowcellId("id");
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsLane()
	{
		frag.setLane(2);
		assertFalse( frag.equals(frag2) );
		frag2.setLane(2);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsTile()
	{
		frag.setTile(1000);
		assertFalse( frag.equals(frag2) );
		frag2.setTile(1000);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsXpos()
	{
		frag.setXpos(1234);
		assertFalse( frag.equals(frag2) );
		frag2.setXpos(1234);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsYpos()
	{
		frag.setYpos(1234);
		assertFalse( frag.equals(frag2) );
		frag2.setYpos(1234);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsRead()
	{
		frag.setRead(2);
		assertFalse( frag.equals(frag2) );
		frag2.setRead(2);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsFilterPassed()
	{
		frag.setFilterPassed(false);
		assertFalse( frag.equals(frag2) );
		frag2.setFilterPassed(false);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsControlNumber()
	{
		frag.setControlNumber(314);
		assertFalse( frag.equals(frag2) );
		frag2.setControlNumber(314);
		assertTrue( frag.equals(frag2) );
	}

	@Test
	public void testEqualsIndexSequence()
	{
		frag.setIndexSequence("ABC");
		assertFalse( frag.equals(frag2) );
		frag2.setIndexSequence("ABC");
		assertTrue( frag.equals(frag2) );
	}

	///////////////////////////////////////////////////////////////
	// serialization
	///////////////////////////////////////////////////////////////
	private static SequencedFragment cloneBySerialization(SequencedFragment original) throws IOException
	{
		ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
		DataOutputStream dataOutput = new DataOutputStream(outputBuffer);
		original.write(dataOutput);
		dataOutput.close();

		SequencedFragment newFrag = new SequencedFragment();
		newFrag.readFields( new DataInputStream( new ByteArrayInputStream(outputBuffer.toByteArray())));

		return newFrag;
	}

	@Test
	public void testSerializationEmpty() throws IOException
	{
		assertEquals(frag, cloneBySerialization(frag));
	}

	@Test
	public void testSerializationWithSeq() throws IOException
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"));
		assertEquals(frag, cloneBySerialization(frag));
	}

	@Test
	public void testSerializationWithFields() throws IOException
	{
		frag.setSequence(new Text("AGTAGTAGTAGTAGTAGTAGTAGTAGTAGT"));
		frag.setQuality(new Text("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"));

		frag.setInstrument("machine");
		frag.setLane(3);
		frag.setRead(1);
		frag.setIndexSequence("CAT");

		assertEquals(frag, cloneBySerialization(frag));
	}


	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestSequencedFragment.class.getName());
	}
}
