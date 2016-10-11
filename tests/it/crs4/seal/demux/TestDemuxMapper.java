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


package tests.it.crs4.seal.demux;

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.TestContext;
import it.crs4.seal.demux.DemuxMapper;
import it.crs4.seal.common.SequenceId;

import org.seqdoop.hadoop_bam.SequencedFragment;

import org.apache.hadoop.io.Text;

import java.util.Map;
import java.util.Iterator;

import org.junit.*;
import static org.junit.Assert.*;

public class TestDemuxMapper
{
	private DemuxMapper mapper;
	private TestContext<SequenceId, SequencedFragment> context;

	private Text key1;
	private SequencedFragment fragment1;

	private Text key2;
	private SequencedFragment fragment2;

	private Text keyInvalidReadNum;
	private SequencedFragment fragmentInvalidReadNum;

	@Before
	public void setup()
	{
		mapper = new DemuxMapper();
		context = new TestContext<SequenceId, SequencedFragment>();

		key1 = new Text("machine:240:1:1111:2222:3333:1");
		fragment1 = new SequencedFragment();
		fragment1.setSequence(new Text(".CCAGTACAAGCACCATGCTTAACAAAAGACTGTCCAAAATAAACATGCAA"));
		fragment1.setQuality(new Text("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
		fragment1.setInstrument("machine");
		fragment1.setRunNumber(240);
		fragment1.setLane(1);
		fragment1.setTile(1111);
		fragment1.setXpos(2222);
		fragment1.setYpos(3333);
		fragment1.setIndexSequence("0");
		fragment1.setRead(1);
		fragment1.setFilterPassed(true);

		key2 = new Text("machine:240:1:1111:2222:3333:2");
		fragment2 = new SequencedFragment();
		fragment2.setSequence(new Text(".CCAGTACAAGCACCATGCTTAACAAAAGACTGTCCAAAATAAACATGCAA"));
		fragment2.setQuality(new Text("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
		fragment2.setInstrument("machine");
		fragment2.setRunNumber(240);
		fragment2.setLane(1);
		fragment2.setTile(1111);
		fragment2.setXpos(2222);
		fragment2.setYpos(3333);
		fragment2.setIndexSequence("0");
		fragment2.setRead(2);
		fragment2.setFilterPassed(true);

		keyInvalidReadNum = new Text("machine:240:1:1111:2222:3333:0");
		fragmentInvalidReadNum = new SequencedFragment();
		fragmentInvalidReadNum.setSequence(new Text(".CCAGTACAAGCACCATGCTTAACAAAAGACTGTCCAAAATAAACATGCAA"));
		fragmentInvalidReadNum.setQuality(new Text("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
		fragmentInvalidReadNum.setInstrument("machine");
		fragmentInvalidReadNum.setRunNumber(240);
		fragmentInvalidReadNum.setLane(1);
		fragmentInvalidReadNum.setTile(1111);
		fragmentInvalidReadNum.setXpos(2222);
		fragmentInvalidReadNum.setYpos(3333);
		fragmentInvalidReadNum.setIndexSequence("0");
		fragmentInvalidReadNum.setRead(0);
		fragmentInvalidReadNum.setFilterPassed(true);
	}

	@Test
	public void testMap() throws java.io.IOException, InterruptedException
	{
		// map two records.
		mapper.map(key1, fragment1, context);
		assertEquals(1, context.getNumWrites());

		mapper.map(key2, fragment2, context);
		assertEquals(2, context.getNumWrites());

		Iterator< TestContext.Tuple<SequenceId,SequencedFragment> > it = context.iterator();
		TestContext.Tuple<SequenceId,SequencedFragment> entry = it.next();

		SequenceId key = entry.getKey();
		// expected key is as above with the last :1 removed
		assertEquals(key1.toString().substring(0, key1.getLength() - 2), key.getLocation());
		assertEquals(1, key.getRead());

		assertEquals(fragment1, entry.getValue());

		entry = it.next();
		key = entry.getKey();
		// expected key is as above with the last :2 removed
		assertEquals(key2.toString().substring(0, key2.getLength() - 2), key.getLocation());
		assertEquals(2, key.getRead());

		assertEquals(fragment2, entry.getValue());
	}

	@Test(expected=RuntimeException.class)
	public void testInvalidReadNumber() throws java.io.IOException, InterruptedException
	{
		mapper.map(keyInvalidReadNum, fragmentInvalidReadNum, context);
	}

	@Test(expected=RuntimeException.class)
	public void testMissingLane() throws java.io.IOException, InterruptedException
	{
		fragment1.setLane(null);
		mapper.map(key1, fragment1, context);
	}

	@Test(expected=RuntimeException.class)
	public void testMissingReadNum() throws java.io.IOException, InterruptedException
	{
		fragment1.setRead(null);
		mapper.map(key1, fragment1, context);
	}

	@Test(expected=RuntimeException.class)
	public void testMissingInstrument() throws java.io.IOException, InterruptedException
	{
		fragment1.setInstrument(null);
		mapper.map(key1, fragment1, context);
	}


	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestDemuxMapper.class.getName());
	}
}
