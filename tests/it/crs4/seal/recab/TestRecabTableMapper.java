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

package tests.it.crs4.seal.recab;

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.TestContext;
import it.crs4.seal.common.FormatException;
import it.crs4.seal.common.ReadPair;
import it.crs4.seal.common.SamInputFormat;
import it.crs4.seal.common.Utils;
import it.crs4.seal.recab.RecabTable;
import it.crs4.seal.recab.RecabTableMapper;
import it.crs4.seal.recab.ObservationCount;
import it.crs4.seal.recab.VariantRegion;
import it.crs4.seal.recab.VariantReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.junit.*;
import static org.junit.Assert.*;

public class TestRecabTableMapper
{
	private static class DaVariantReader implements VariantReader {
		public ArrayList<VariantRegion> snpList = new ArrayList<VariantRegion>();
		public Iterator<VariantRegion> iterator = null;

		public boolean nextEntry(VariantRegion dest) throws FormatException, IOException
		{
			if (iterator == null)
				iterator = snpList.iterator();

			if (iterator.hasNext())
			{
				VariantRegion result = iterator.next();
				dest.set(result);
				return true;
			}
			else
				return false;
		}
	}

	private static final String littleSam = "LITTLE	67	chr6	1	37	3M	=	6	9	AGC	BCD	5:C	XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:3	RG:Z:test";

	private static final String littleReversedSam = "LITTLE	115	chr6	1	37	3M	=	6	9	AGC	BCD	5:C	XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:3	RG:Z:test";

	private static final String littleSamRead2 = "LITTLE	131	chr6	6	37	3M	=	1	9	AGC	BCD	5:C	XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:3	RG:Z:test";

	private static final String littleReversedSamRead2 = "LITTLE	179	chr6	6	37	3M	=	1	9	AGC	BCD	5:C	XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:3	RG:Z:test";

	private static final String littleSamWithN = "LITTLE	67	chr6	1	37	3M	=	6	9	ANC	B#D	5:C	XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:3	RG:Z:test";

	private static final String littleSamBaseQ0 = "LITTLE	67	chr6	1	37	3M	=	6	9	AGC	B!D	5:C	XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:3	RG:Z:test";

	private static final String littleSamUnmapped = "LITTLE	71	*	*	0	*	=	6	9	AGC	BCD	5:C	XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:3	RG:Z:test";

	private static final String bigSam = "ERR020229.100000/1	89	chr6	3558357	37	91M	=	3558678	400	AGCTTCTTTGACTCTCGAATTTTAGCACTAGAAGAAATAGTGAGGATTATATATTTCAGAAGTTCTCACCCAGGATATCAGAACACATTCA	5:CB:CCBCCB>:C@;BBBB??B;?>1@@=C=4ACCAB3A8=CC=C?CBC=CBCCCCCCCCCCCCC@5>?=?CAAB=3=>====5>=AC?C	XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:91	RG:Z:test";

	private static final String readAdapterLeftSam = "LEFT	145	1	3104333	37	21M	=	3104336	18	TCTGGATATAGGGAGACTCAG	GGGGGGGGGGGGGGGGGGGGG	MD:Z:0C0A0A18	RG:Z:DCW97JN1_264:1";
	private static final String readAdapterRightSam = "RIGHT	97	1	3104336	37	21M	=	3104333	-18	GGATATAGGGAGACTCAGAGA	GGGGGGGGGGGGGGGGGGGGG	MD:Z:18G1C0	RG:Z:DCW97JN1_264:1";

	private static final String deletion = "DELETE	107	chr12	1	60	3M2D2M	*	*	*	AAGTT	ABCDE	MD:Z:3^CA2	RG:Z:test";
	private static final String insertion = "INSERT	107	chr12	1	60	3M4I2M	*	*	*	AAGCTATTT	ABCDEFGHI	MD:Z:5	RG:Z:test";

	private RecabTableMapper mapper;
	private TestContext<Text, ObservationCount> context;
	private DaVariantReader reader;
	private Configuration conf;
	private SamInputFormat.SamRecordReader samReader;
	private File tempFile;

	@Before
	public void setup() throws IOException
	{
		mapper = new RecabTableMapper();
		context = new TestContext<Text, ObservationCount>();
		reader = new DaVariantReader();
		conf = new Configuration();
		samReader = null;

		tempFile = File.createTempFile("test_recab_table", ".sam");
	}

	@After
	public void teadDown() throws IOException
	{
		tempFile.delete();
	}

	private void setupReader(String sam) throws IOException
	{
		// write a temporary SAM file
		PrintWriter out = new PrintWriter( new BufferedWriter( new FileWriter(tempFile) ) );
		out.write(sam);
		out.close();

		FileSplit split = new FileSplit(new Path(tempFile.toURI().toString()), 0, sam.length(), null);

		samReader = new SamInputFormat.SamRecordReader();
		samReader.initialize(split, Utils.getTaskAttemptContext(conf));
	}

	private List<ReadPair> makeReadPairs(String sam) throws IOException
	{
		ArrayList<ReadPair> retval = new ArrayList<ReadPair>();

		setupReader(sam);

		while (samReader.nextKeyValue())
			retval.add(samReader.getCurrentValue());

		return retval;
	}

	@Test
	public void testSetup() throws IOException
	{
		mapper.setup(reader, context, conf);

		// check the counters
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "Processed"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "FilteredUnmapped"));

		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "Used"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "VariantMismatches"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "VariantBases"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "NonVariantMismatches"));
	}

	@Test
	public void testSimpleMap() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(littleSam);

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		// check counters
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "Processed"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "FilteredUnmapped"));

		assertEquals(3, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "Used"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "BadBases"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "VariantMismatches"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "VariantBases"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "NonVariantMismatches"));

		// how many pairs emitted?
		assertEquals(3, context.getNumWrites());

		// check keys
		Set<Text> keys = context.getKeys();
		assertTrue( keys.contains(prepKey("test", "33", "1", "NN")) );
		assertTrue( keys.contains(prepKey("test", "34", "2", "AG")) );
		assertTrue( keys.contains(prepKey("test", "35", "3", "GC")) );

		// check values (they should all be (1,0) )
		List<ObservationCount> counts = context.getAllValues();
		for (ObservationCount c: counts)
		{
			assertEquals(1, c.getObservations());
			assertEquals(0, c.getMismatches());
		}
	}

	@Test
	public void testReverseSimpleMap() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(littleReversedSam);

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(3, context.getNumWrites());

		List<ObservationCount> counts = context.getAllValues();
		for (ObservationCount c: counts)
		{
			assertEquals(1, c.getObservations());
			assertEquals(0, c.getMismatches());
		}

		Set<Text> keys = context.getKeys();
		assertTrue( keys.contains(prepKey("test", "35", "1", "NN")) );
		assertTrue( keys.contains(prepKey("test", "34", "2", "GC")) );
		assertTrue( keys.contains(prepKey("test", "33", "3", "CT")) );
	}

	@Test
	public void testMapRead2() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(littleSamRead2);

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(3, context.getNumWrites());

		List<ObservationCount> counts = context.getAllValues();
		for (ObservationCount c: counts)
		{
			assertEquals(1, c.getObservations());
			assertEquals(0, c.getMismatches());
		}

		Set<Text> keys = context.getKeys();
		assertTrue( keys.contains(prepKey("test", "33", "-1", "NN")) );
		assertTrue( keys.contains(prepKey("test", "34", "-2", "AG")) );
		assertTrue( keys.contains(prepKey("test", "35", "-3", "GC")) );
	}

	@Test
	public void testReverseMapRead2() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(littleReversedSamRead2);

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(3, context.getNumWrites());

		List<ObservationCount> counts = context.getAllValues();
		for (ObservationCount c: counts)
		{
			assertEquals(1, c.getObservations());
			assertEquals(0, c.getMismatches());
		}

		Set<Text> keys = context.getKeys();
		assertTrue( keys.contains(prepKey("test", "35", "-1", "NN")) );
		assertTrue( keys.contains(prepKey("test", "34", "-2", "GC")) );
		assertTrue( keys.contains(prepKey("test", "33", "-3", "CT")) );
	}

	@Test
	public void testSamWithN() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(littleSamWithN);

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(2, context.getNumWrites());
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "BadBases"));
		assertEquals(2, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "Used"));
	}

	@Test
	public void testSamWithBaseQ0() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(littleSamBaseQ0);

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(2, context.getNumWrites());
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "BadBases"));
		assertEquals(2, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "Used"));
	}

	@Test
	public void testSamWithUnmapped() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(littleSamUnmapped);

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(0, context.getNumWrites());
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "FilteredUnmapped"));
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "Processed"));
	}

	@Test
	public void testSamWithMapQ0() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(littleSam.replaceFirst("37", "0"));

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(0, context.getNumWrites());
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "FilteredMapQ"));
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "Processed"));
	}

	@Test
	public void testWithVariant() throws IOException, InterruptedException
	{
		reader.snpList.add(new VariantRegion("chr6", 2)); // falls right in the middle of the read

		List<ReadPair> pairs = makeReadPairs(littleSam);

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		// how many pairs emitted?
		assertEquals(2, context.getNumWrites());

		assertEquals(2, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "Used"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "VariantMismatches"));
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "VariantBases"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "NonVariantMismatches"));

		// check keys
		Set<Text> keys = context.getKeys();
		assertTrue( keys.contains(prepKey("test", "33", "1", "NN")) );
		assertTrue( keys.contains(prepKey("test", "35", "3", "GC")) );

		// check values (they should all be (1,0) )
		List<ObservationCount> counts = context.getAllValues();
		for (ObservationCount c: counts)
		{
			assertEquals(1, c.getObservations());
			assertEquals(0, c.getMismatches());
		}
	}

	@Test
	public void testWithVariantWithMismatch() throws IOException, InterruptedException
	{
		reader.snpList.add(new VariantRegion("chr6", 2)); // falls right in the middle of the read

		List<ReadPair> pairs = makeReadPairs(littleSam.replace("MD:Z:3", "MD:Z:1A1"));

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "VariantMismatches"));
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "VariantBases"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "NonVariantMismatches"));
	}

	@Test
	public void testWithNonVariantMismatch() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(littleSam.replace("MD:Z:3", "MD:Z:1A1"));

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "NonVariantMismatches"));
	}

	@Test
	public void testDeletion() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(deletion);

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(5, context.getNumWrites());
		assertEquals(5, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "Used"));

		Set<Text> keys = context.getKeys();
		assertEquals(5, keys.size());
		assertTrue( keys.contains(prepKey("test", "32", "1", "NN")) );
		assertTrue( keys.contains(prepKey("test", "33", "2", "AA")) );
		assertTrue( keys.contains(prepKey("test", "34", "3", "AG")) );
		assertTrue( keys.contains(prepKey("test", "35", "4", "GT")) );
		assertTrue( keys.contains(prepKey("test", "36", "5", "TT")) );
	}

	@Test
	public void testInsertion() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(insertion);

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(5, context.getNumWrites());
		assertEquals(5, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "Used"));

		Set<Text> keys = context.getKeys();
		assertEquals(5, keys.size());
		assertTrue( keys.contains(prepKey("test", "32", "1", "NN")) );
		assertTrue( keys.contains(prepKey("test", "33", "2", "AA")) );
		assertTrue( keys.contains(prepKey("test", "34", "3", "AG")) );
		assertTrue( keys.contains(prepKey("test", "39", "8", "TT")) );
		assertTrue( keys.contains(prepKey("test", "40", "9", "TT")) );
	}

	@Test(expected=RuntimeException.class)
	public void testNoReadGroup() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(littleSam.replaceFirst("\tRG:Z.*", ""));

		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);
	}

	@Test
	public void testSkipReadAdapterOnRight() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(readAdapterRightSam);
		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(18, context.getNumWrites());

		// ensure cycles 19, 20, and 21 have been filtered
		Set<Text> keys = context.getKeys();
		Set<String> cycles = new HashSet<String>();
		for (Text k: keys)
			cycles.add( k.toString().split(RecabTable.TableDelim)[2] );

		assertFalse( cycles.contains("19") );
		assertFalse( cycles.contains("20") );
		assertFalse( cycles.contains("21") );
	}

	@Test
	public void testSkipReadAdapterOnLeft() throws IOException, InterruptedException
	{
		List<ReadPair> pairs = makeReadPairs(readAdapterLeftSam);
		mapper.setup(reader, context, conf);
		mapper.map(new LongWritable(0), pairs.get(0), context);

		assertEquals(18, context.getNumWrites());

		// ensure cycles 1, 2, and 3 have been filtered
		Set<Text> keys = context.getKeys();
		Set<String> cycles = new HashSet<String>();
		for (Text k: keys)
			cycles.add( k.toString().split(RecabTable.TableDelim)[2] );

		assertFalse( cycles.contains("1") );
		assertFalse( cycles.contains("2") );
		assertFalse( cycles.contains("3") );
	}

	/**
	 * Public for re-use in other tests.
	 */
	public static Text prepKey(String rg, String quality, String cycle, String dinuc)
	{
		return new Text(
		  rg + RecabTable.TableDelim +
		  quality + RecabTable.TableDelim +
		  cycle + RecabTable.TableDelim +
		  dinuc + RecabTable.TableDelim);
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestRecabTableMapper.class.getName());
	}
}
