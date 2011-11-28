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


package tests.it.crs4.seal.demux;

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.TestContext;
import it.crs4.seal.demux.DemuxReducer;
import it.crs4.seal.common.SequenceId;
import it.crs4.seal.common.SequencedFragment;

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

	private SequenceId key1;
	private SequenceId key2;
	private SequenceId key3;
	private SequencedFragment fragment1;
	private SequencedFragment fragment2;
	private SequencedFragment fragment3;

	private File tempSampleSheet;

	private static final String sampleSheet =
"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
"\"b0396abxx\",1,\"csct_007083\",\"Human\",\"ATCACG\",\"Whole-genome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"MANU\"\n" +
"\"b0396abxx\",1,\"csct_007084\",\"Human\",\"CGATGT\",\"Whole-genome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"MANU\"\n" +
"\"b0396abxx\",1,\"csct_007085\",\"Human\",\"TTAGGC\",\"Whole-genome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"MANU\"\n" +
"\"b0396abxx\",1,\"csct_007090\",\"Human\",\"TGACCA\",\"Whole-genome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"MANU\"\n";

	private void writeSampleSheet(String contents) throws IOException
	{
		tempSampleSheet = File.createTempFile("test_sample_sheet", "csv");
		PrintWriter out = new PrintWriter( new BufferedWriter( new FileWriter(tempSampleSheet) ) );
    out.write(contents);
    out.close();
	}

	@Before
	public void setup() throws IOException
	{
		writeSampleSheet(sampleSheet);

		reducer = new DemuxReducer();
		reducer.setup(tempSampleSheet.getAbsolutePath(), new Configuration());

		context = new TestContext<Text, SequencedFragment>();
		
		key1 = new SequenceId("machine:240:1:1111:2222:3333", 1);
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

		key2 = new SequenceId("machine:240:1:1111:2222:3333", 2);
		fragment2 = new SequencedFragment();
		fragment2.setSequence(new Text("ATCACGA"));
		fragment2.setQuality(new Text("bbb"));
		fragment2.setInstrument("machine");
		fragment2.setRunNumber(240);
		fragment2.setLane(1);
		fragment2.setTile(1111);
		fragment2.setXpos(2222);
		fragment2.setYpos(3333);
		fragment2.setIndexSequence("0");
		fragment2.setRead(2);
		fragment2.setFilterPassed(true);

		key3 = new SequenceId("machine:240:1:1111:2222:3333", 3);
		fragment3 = new SequencedFragment();
		fragment3.setSequence(new Text(".CCAGTACAAGCACCATGCTTAACAAAAGACTGTCCAAAATAAACATGCAA"));
		fragment3.setQuality(new Text("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
		fragment3.setInstrument("machine");
		fragment3.setRunNumber(240);
		fragment3.setLane(1);
		fragment3.setTile(1111);
		fragment3.setXpos(2222);
		fragment3.setYpos(3333);
		fragment3.setIndexSequence("0");
		fragment3.setRead(3);
		fragment3.setFilterPassed(true);
	}

	@After
	public void tearDown() throws IOException
	{
		tempSampleSheet.delete();
	}

	@Test
	public void testReduce() throws IOException, InterruptedException
	{
		List<SequencedFragment> list = new ArrayList<SequencedFragment>(3);
		// the values should be in the order 2, 1, 3
		list.add(fragment2);
		list.add(fragment1);
		list.add(fragment3);

		reducer.reduce(key2, list, context);

		assertEquals(2, context.getCounterValue("Sample reads", "csct_007083"));
		assertEquals(2, context.getNumWrites());

		Set<Text> keySet = context.getKeys();
		assertEquals(1, keySet.size());
		Text key = keySet.iterator().next();
		assertEquals("csct_007083", key.toString());

		List<SequencedFragment> values = context.getValuesForKey(key);

		String indexSeq = fragment2.getSequence().toString();
		indexSeq = indexSeq.substring(0, indexSeq.length() - 1);

		fragment1.setIndexSequence(indexSeq);
		fragment3.setIndexSequence(indexSeq);
		assertEquals(fragment1, values.get(0));
		assertEquals(fragment3, values.get(1));
	}

	@Test
	public void testUnknownBarcode() throws IOException, InterruptedException
	{
		String barcode = "ATCANN";
		fragment2.setSequence(new Text(barcode + "N"));
		List<SequencedFragment> list = new ArrayList<SequencedFragment>(3);
		// the values should be in the order 2, 1, 3
		list.add(fragment2);
		list.add(fragment1);
		list.add(fragment3);

		reducer.reduce(key2, list, context);
		Set<Text> keys = context.getKeys();
		assertEquals(1, keys.size());
		assertEquals("unknown", keys.iterator().next().toString());
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestDemuxReducer.class.getName());
	}
}
