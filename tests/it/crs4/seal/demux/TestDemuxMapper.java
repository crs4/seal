// Copyright (C) 2011 CRS4.
// 
// This file is part of ReadSort.
// 
// ReadSort is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
// 
// ReadSort is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
// for more details.
// 
// You should have received a copy of the GNU General Public License along
// with ReadSort.  If not, see <http://www.gnu.org/licenses/>.


package tests.it.crs4.seal.demux;

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.TestContext;
import it.crs4.seal.demux.DemuxMapper;
import it.crs4.seal.common.SequenceId;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.util.Map;
import java.util.Iterator;

import org.junit.*;
import static org.junit.Assert.*;

public class TestDemuxMapper
{
	private DemuxMapper mapper;
	private TestContext<SequenceId, Text> context;

	private static final Text qseq1 = 
		new Text("machine\trun id\t1\t1111\t2222\t3333\t0\t1\t.CCAGTACAAGCACCATGCTTAACAAAAGACTGTCCAAAATAAACATGCAA\tbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\t1");
	private static final Text qseq2 = 
		new Text("machine\trun id\t1\t1111\t2222\t3333\t0\t2\t.CCAGTACAAGCACCATGCTTAACAAAAGACTGTCCAAAATAAACATGCAA\tbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\t1");

	private static final Text qseqInvalidReadNum = 
		new Text("machine\trun id\t1\t1111\t2222\t3333\t0\t0\t.CCAGTACAAGCACCATGCTTAACAAAAGACTGTCCAAAATAAACATGCAA\tbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\t1");

	@Before
	public void setup()
	{
		mapper = new DemuxMapper();
		context = new TestContext<SequenceId, Text>();
	}

	@Test
	public void testMap() throws java.io.IOException, InterruptedException
	{
		mapper.map(new LongWritable(1), qseq1, context);
		assertEquals(1, context.output.size());

		mapper.map(new LongWritable(1), qseq2, context);
		assertEquals(2, context.output.size());

		Iterator< Map.Entry<SequenceId,Text> > it = context.output.entrySet().iterator();
		Map.Entry<SequenceId,Text> entry = it.next();
		SequenceId key = entry.getKey();
		assertEquals("machine\trun id\t1\t1111\t2222\t3333\t0", key.getLocation());
		assertEquals(1, key.getRead());

		assertEquals(qseq1, entry.getValue());

		entry = it.next();
		key = entry.getKey();
		assertEquals("machine\trun id\t1\t1111\t2222\t3333\t0", key.getLocation());
		assertEquals(2, key.getRead());

		assertEquals(qseq2, entry.getValue());
	}

	@Test(expected=RuntimeException.class)
	public void testInvalidReadNumber() throws java.io.IOException, InterruptedException
	{
		mapper.map(new LongWritable(1), qseqInvalidReadNum, context);
	}

	@Test(expected=RuntimeException.class)
	public void testInsufficientFields() throws java.io.IOException, InterruptedException
	{
		mapper.map(new LongWritable(1), new Text(""), context);
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestSampleSheet.class.getName());
	}
}
