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

package tests.it.crs4.seal.prq;

import org.junit.*;
import static org.junit.Assert.*;

import it.crs4.seal.prq.PairReadsQSeqReducer;
import it.crs4.seal.common.AbstractTaggedMapping;
import it.crs4.seal.common.WritableMapping;
import it.crs4.seal.common.ReadPair;
import it.crs4.seal.common.SequenceId;
import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.TestContext;

import org.seqdoop.hadoop_bam.SequencedFragment;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TestPairReadsQseqReducer
{
	private PairReadsQSeqReducer reducer;
	private TestContext<Text, ReadPair> context;
	private SequenceId inputKey = new SequenceId("location", 1);
	private ArrayList<Text> reads;
	private Text r1, r2;


	@Before
	public void setup()
	{
		context = new TestContext<Text, ReadPair>();
		reducer = new PairReadsQSeqReducer();
		reducer.setup(context);
		reads = new ArrayList<Text>();
		r1 = new Text("AAAAAAAAAA\tBBBBBBBBBB\t1");
		r2 = new Text("GGGGGGGGGG\tBBBBBBBBBB\t1");
	}

	@Test
	public void testNormal() throws IOException, InterruptedException
	{
		reads.add(r1);
		reads.add(r2);
		reducer.reduce(inputKey, reads, context);

		Set<Text> keys = context.getKeys();
		assertEquals(1, keys.size());
		assertEquals(inputKey.getLocation(), keys.iterator().next().toString());

		List<ReadPair> values = context.getAllValues();
		assertEquals(1, values.size());
		ReadPair pair = values.get(0);
		AbstractTaggedMapping read1 = pair.getRead1();
		AbstractTaggedMapping read2 = pair.getRead2();

		String[] fields = r1.toString().split("\t");
		assertEquals(fields[0], read1.getSequenceString());
		assertEquals(fields[1], read1.getBaseQualitiesString());

		fields = r2.toString().split("\t");
		assertEquals(fields[0], read2.getSequenceString());
		assertEquals(fields[1], read2.getBaseQualitiesString());

		assertEquals(0, context.getCounterValue("it.crs4.seal.prq.PairReadsQSeqReducer$ReadCounters", "NotEnoughBases"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.prq.PairReadsQSeqReducer$ReadCounters", "FailedFilter"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.prq.PairReadsQSeqReducer$ReadCounters", "Dropped"));
	}

	@Test
	public void testFailedNotEnoughBases() throws IOException, InterruptedException
	{
		r1 = new Text("AAANNNNNNN\tBBBBBBBBBB\t1");
		reads.add(r1);
		reads.add(r2);
		reducer.setMinBasesThreshold(4);
		reducer.setDropFailedFilter(true);
		reducer.reduce(inputKey, reads, context);

		assertEquals(1, context.getAllValues().size()); // emits 2 reads in 1 pair
		assertEquals(1, context.getCounterValue("it.crs4.seal.prq.PairReadsQSeqReducer$ReadCounters", "NotEnoughBases"));
	}

	@Test
	public void testPairFailsNotEnoughBases() throws IOException, InterruptedException
	{
		r1 = new Text("AAANNNNNNN\tBBBBBBBBBB\t1");
		r2 = new Text("GGGNNNNNNN\tBBBBBBBBBB\t1");
		reads.add(r1);
		reads.add(r2);
		reducer.setMinBasesThreshold(4);
		reducer.setDropFailedFilter(true);
		reducer.reduce(inputKey, reads, context);

		assertEquals(0, context.getAllValues().size());
		assertEquals(2, context.getCounterValue("it.crs4.seal.prq.PairReadsQSeqReducer$ReadCounters", "Dropped"));
	}

	@Test
	public void testFailedQC() throws IOException, InterruptedException
	{
		r1 = new Text("AAANNNNNNN\tBBBBBBBBBB\t0");
		reads.add(r1);
		reads.add(r2);
		reducer.setDropFailedFilter(true);
		reducer.reduce(inputKey, reads, context);

		assertEquals(1, context.getAllValues().size()); // emits 2 reads in 1 pair
		assertEquals(1, context.getCounterValue("it.crs4.seal.prq.PairReadsQSeqReducer$ReadCounters", "FailedFilter"));
	}

	@Test
	public void testPairFailsQC() throws IOException, InterruptedException
	{
		r1 = new Text("AAANNNNNNN\tBBBBBBBBBB\t0");
		r2 = new Text("GGGNNNNNNN\tBBBBBBBBBB\t0");
		reads.add(r1);
		reads.add(r2);
		reducer.setDropFailedFilter(true);
		reducer.reduce(inputKey, reads, context);

		assertEquals(0, context.getAllValues().size());
		assertEquals(2, context.getCounterValue("it.crs4.seal.prq.PairReadsQSeqReducer$ReadCounters", "FailedFilter"));
		assertEquals(2, context.getCounterValue("it.crs4.seal.prq.PairReadsQSeqReducer$ReadCounters", "Dropped"));
	}

	@Test(expected=RuntimeException.class)
	public void testUnpaired() throws IOException, InterruptedException
	{
		reads.add(r1);
		reducer.reduce(inputKey, reads, context);
	}

	@Test
	public void testUnpairedWarning() throws IOException, InterruptedException
	{
		reads.add(r1);
		reducer.setWarnOnlyIfUnpaired(true);
		reducer.reduce(inputKey, reads, context);

		List<ReadPair> values = context.getAllValues();
		assertEquals(0, values.size());

		assertEquals(1, context.getCounterValue("it.crs4.seal.prq.PairReadsQSeqReducer$ReadCounters", "Dropped"));
	}

	@Test
	public void testSingleReads() throws IOException, InterruptedException
	{
		reads.add(r1);
		reducer.setNumReadsPerTemplate(1);
		reducer.reduce(inputKey, reads, context);

		Set<Text> keys = context.getKeys();
		assertEquals(1, keys.size());
		assertEquals(inputKey.getLocation(), keys.iterator().next().toString());

		List<ReadPair> values = context.getAllValues();
		assertEquals(1, values.size());
		ReadPair pair = values.get(0);

		AbstractTaggedMapping read1 = pair.getRead1();
		String[] fields = r1.toString().split("\t");
		assertEquals(fields[0], read1.getSequenceString());

		assertNull(pair.getRead2());
	}
}
