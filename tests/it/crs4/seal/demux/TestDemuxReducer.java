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


package tests.it.crs4.seal.demux;

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.TestContext;
import it.crs4.seal.demux.Demux;
import it.crs4.seal.demux.DemuxReducer;
import it.crs4.seal.common.SequenceId;

import fi.tkk.ics.hadoop.bam.SequencedFragment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Set;
import java.util.List;
import java.io.StringReader;
import java.io.File;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.*;
import static org.junit.Assert.*;


public class TestDemuxReducer
{
	private DemuxReducer reducer;
	private TestContext<Text, SequencedFragment> context;

	private List<SequenceId> keys;
	private List<SequencedFragment> fragments;

	private File tempSampleSheet;

	private static final String sampleSheet =
"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
"\"b0396abxx\",1,\"csct_007083\",\"Human\",\"ATCACG\",\"Whole-genome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"MANU\"\n" +
"\"b0396abxx\",1,\"csct_007084\",\"Human\",\"CGATGT\",\"Whole-genome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"MANU\"\n" +
"\"b0396abxx\",1,\"csct_007085\",\"Human\",\"TTAGGC\",\"Whole-genome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"MANU\"\n" +
"\"b0396abxx\",1,\"csct_007090\",\"Human\",\"TGACCA\",\"Whole-genome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"MANU\"\n";

	private static final String sampleSheetNoIndex =
"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
"\"b0396abxx\",1,\"csct_007083\",\"Human\",,\"Whole-genome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"MANU\"\n" +
"\"b0396abxx\",2,\"csct_007084\",\"Human\",,\"Whole-genome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"MANU\"\n";

	@Before
	public void setup() throws IOException
	{
		context = new TestContext<Text, SequencedFragment>();
		keys = new ArrayList<SequenceId>();
		fragments = new ArrayList<SequencedFragment>();
	}

	@After
	public void tearDown() throws IOException
	{
		if (tempSampleSheet != null)
			tempSampleSheet.delete();
	}

	private void writeSampleSheet(String contents) throws IOException
	{
		tempSampleSheet = File.createTempFile("test_sample_sheet", "csv");
		PrintWriter out = new PrintWriter( new BufferedWriter( new FileWriter(tempSampleSheet) ) );
    out.write(contents);
    out.close();
	}

	private void setupReducer(String sampleSheet, Configuration conf) throws IOException
	{
		reducer = new DemuxReducer();

		writeSampleSheet(sampleSheet);
		reducer.setup(tempSampleSheet.getAbsolutePath(), conf == null ? new Configuration() : conf);
	}

	private void setupReadsPairedMultiplexed(int lane)
	{
		SequenceId key;
		SequencedFragment fragment;

		key = new SequenceId("machine:240:" + lane + ":1111:2222:3333", 1);
		fragment = new SequencedFragment();
		fragment.setSequence(new Text(".CCAGTACAAGCACCATGCTTAACAAAAGACTGTCCAAAATAAACATGCAA"));
		fragment.setQuality(new Text("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
		fragment.setInstrument("machine");
		fragment.setRunNumber(240);
		fragment.setLane(lane);
		fragment.setTile(1111);
		fragment.setXpos(2222);
		fragment.setYpos(3333);
		fragment.setIndexSequence("0");
		fragment.setRead(1);
		fragment.setFilterPassed(true);
		keys.add(key);
		fragments.add(fragment);

		key = new SequenceId("machine:240:" + lane + ":1111:2222:3333", 2);
		fragment = new SequencedFragment();
		fragment.setSequence(new Text("ATCACGA"));
		fragment.setQuality(new Text("bbb"));
		fragment.setInstrument("machine");
		fragment.setRunNumber(240);
		fragment.setLane(lane);
		fragment.setTile(1111);
		fragment.setXpos(2222);
		fragment.setYpos(3333);
		fragment.setIndexSequence("0");
		fragment.setRead(2);
		fragment.setFilterPassed(true);
		keys.add(key);
		fragments.add(fragment);

		key = new SequenceId("machine:240:" + lane + ":1111:2222:3333", 3);
		fragment = new SequencedFragment();
		fragment.setSequence(new Text(".CCAGTACAAGCACCATGCTTAACAAAAGACTGTCCAAAATAAACATGCAA"));
		fragment.setQuality(new Text("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
		fragment.setInstrument("machine");
		fragment.setRunNumber(240);
		fragment.setLane(lane);
		fragment.setTile(1111);
		fragment.setXpos(2222);
		fragment.setYpos(3333);
		fragment.setIndexSequence("0");
		fragment.setRead(3);
		fragment.setFilterPassed(true);
		keys.add(key);
		fragments.add(fragment);
	}

	@Test
	public void testReduce() throws IOException, InterruptedException
	{
		setupReducer(sampleSheet, null);
		setupReadsPairedMultiplexed(1);

		List<SequencedFragment> list = new ArrayList<SequencedFragment>(3);
		// the values should be in the order 2, 1, 3
		list.add(fragments.get(1));
		list.add(fragments.get(0));
		list.add(fragments.get(2));

		reducer.reduce(keys.get(1), list, context);

		assertEquals(2, context.getCounterValue("Sample reads", "csct_007083"));
		assertEquals(2, context.getNumWrites());

		Set<Text> keySet = context.getKeys();
		assertEquals(1, keySet.size());
		Text key = keySet.iterator().next();
		assertEquals("DefaultProject/csct_007083", key.toString());

		List<SequencedFragment> values = context.getValuesForKey(key);

		String indexSeq = fragments.get(1).getSequence().toString();
		indexSeq = indexSeq.substring(0, indexSeq.length() - 1);

		fragments.get(0).setIndexSequence(indexSeq);
		fragments.get(2).setIndexSequence(indexSeq);
		assertEquals(fragments.get(0), values.get(0));
		assertEquals(fragments.get(2), values.get(1));
	}

	@Test
	public void testReduceIndexNotSpecifiedL1() throws IOException, InterruptedException
	{
		setupReducer(sampleSheetNoIndex, null);
		setupReadsPairedMultiplexed(1);

		List<SequencedFragment> list = new ArrayList<SequencedFragment>(3);
		// the values should be in the order 2, 1, 3
		list.add(fragments.get(1));
		list.add(fragments.get(0));
		list.add(fragments.get(2));

		reducer.reduce(keys.get(1), list, context);

		assertEquals(2, context.getCounterValue("Sample reads", "csct_007083"));
		assertEquals(0, context.getCounterValue("Sample reads", "csct_007084"));
		assertEquals(2, context.getNumWrites());
	}

	@Test
	public void testReduceIndexNotSpecifiedL2() throws IOException, InterruptedException
	{
		setupReducer(sampleSheetNoIndex, null);
		setupReadsPairedMultiplexed(2);

		List<SequencedFragment> list = new ArrayList<SequencedFragment>(3);
		// the values should be in the order 2, 1, 3
		list.add(fragments.get(1));
		list.add(fragments.get(0));
		list.add(fragments.get(2));

		reducer.reduce(keys.get(1), list, context);

		// sample csct_007084 is in lane 2
		assertEquals(0, context.getCounterValue("Sample reads", "csct_007083"));
		assertEquals(2, context.getCounterValue("Sample reads", "csct_007084"));
		assertEquals(2, context.getNumWrites());
	}

	@Test
	public void testReduceNotMultiplexed() throws IOException, InterruptedException
	{
		Configuration conf = new Configuration();
		conf.setBoolean(Demux.CONF_NO_INDEX_READS, true);
		setupReducer(sampleSheetNoIndex, conf);

		setupReadsPairedMultiplexed(2);

		List<SequencedFragment> list = new ArrayList<SequencedFragment>(3);
		// We'll only insert reads 1 and 2 (real two, not barcode) into the list to be reduced.
		// They should be identified as sample csct_007084 only by their lane number.
		SequencedFragment read2 = fragments.get(2);
		assertEquals("BUG!  Got read " + read2.getRead() + " instead of read 3", new Integer(3), read2.getRead());
		read2.setRead(2);
		list.add(fragments.get(0));
		list.add(read2);

		reducer.reduce(keys.get(1), list, context);

		// sample csct_007084 is in lane 2
		assertEquals(0, context.getCounterValue("Sample reads", "csct_007083"));
		assertEquals(2, context.getCounterValue("Sample reads", "csct_007084"));
		assertEquals(2, context.getNumWrites());
	}

	@Test
	public void testReduceSingleRead() throws IOException, InterruptedException
	{
		setupReducer(sampleSheet, null);
		setupReadsPairedMultiplexed(1);

		List<SequencedFragment> list = new ArrayList<SequencedFragment>(3);
		// We'll only insert the barcode read and read 1.
		// They should be identified as sample csct_007083 only by their lane number.
		list.add(fragments.get(1));
		list.add(fragments.get(0));

		reducer.reduce(keys.get(1), list, context);

		// sample csct_007084 is in lane 2
		assertEquals(1, context.getCounterValue("Sample reads", "csct_007083"));
		assertEquals(0, context.getCounterValue("Sample reads", "csct_007084"));
		assertEquals(1, context.getNumWrites());
	}

	@Test
	public void testReduceSingleReadNotMultiplexed() throws IOException, InterruptedException
	{
		Configuration conf = new Configuration();
		conf.setBoolean(Demux.CONF_NO_INDEX_READS, true);
		setupReducer(sampleSheetNoIndex, conf);

		setupReadsPairedMultiplexed(1);

		List<SequencedFragment> list = new ArrayList<SequencedFragment>(3);
		// We'll only insert read 1.
		// They should be identified as sample csct_007083 only by their lane number.
		list.add(fragments.get(0));

		reducer.reduce(keys.get(0), list, context);

		// sample csct_007084 is in lane 2
		assertEquals(1, context.getCounterValue("Sample reads", "csct_007083"));
		assertEquals(0, context.getCounterValue("Sample reads", "csct_007084"));
		assertEquals(1, context.getNumWrites());
	}

	@Test
	public void testUnknownBarcode() throws IOException, InterruptedException
	{
		setupReducer(sampleSheet, null);
		setupReadsPairedMultiplexed(1);

		String barcode = "ATCANN";
		fragments.get(1).setSequence(new Text(barcode + "N"));
		List<SequencedFragment> list = new ArrayList<SequencedFragment>(3);
		// the values should be in the order 2, 1, 3
		list.add(fragments.get(1));
		list.add(fragments.get(0));
		list.add(fragments.get(2));

		reducer.reduce(keys.get(1), list, context);
		Set<Text> keySet = context.getKeys();
		assertEquals(1, keySet.size());
		assertEquals("unknown", keySet.iterator().next().toString());
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestDemuxReducer.class.getName());
	}
}
