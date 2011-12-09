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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.TestContext;
import it.crs4.seal.common.FormatException;
import it.crs4.seal.recab.RecabTable;
import it.crs4.seal.recab.RecabTableMapper;
import it.crs4.seal.recab.ObservationCount;
import it.crs4.seal.recab.SnpDef;
import it.crs4.seal.recab.SnpReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.*;
import static org.junit.Assert.*;

public class TestRecabTableMapper
{
	private static class DaSnpReader implements SnpReader {
		public ArrayList<SnpDef> snpList = new ArrayList<SnpDef>();
		public Iterator<SnpDef> iterator = null;

		public boolean nextEntry(SnpDef dest) throws FormatException, IOException
		{
			if (iterator == null)
				iterator = snpList.iterator();

			if (iterator.hasNext())
			{
				SnpDef result = iterator.next();
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

	private static final String deletion = "DELETE	107	chr12	1	60	3M2D2M	*	*	*	AAGTT	ABCDE	MD:Z:3^CA2	RG:Z:test";
	private static final String insertion = "INSERT	107	chr12	1	60	3M4I2M	*	*	*	AAGCTATTT	ABCDEFGHI	MD:Z:5	RG:Z:test";

	private RecabTableMapper mapper;
	private TestContext<Text, ObservationCount> context;
	private DaSnpReader reader;

	@Before
	public void setup()
	{
		// mute the logger for these tests, if we can
		try {
			Log log = LogFactory.getLog(RecabTableMapper.class);
			((org.apache.commons.logging.impl.Jdk14Logger)log).getLogger().setLevel(Level.SEVERE);
		}
		catch (ClassCastException e) {
		}


		mapper = new RecabTableMapper();
		context = new TestContext<Text, ObservationCount>();
		reader = new DaSnpReader();
	}

	@Test
	public void testSetup() throws IOException
	{
		mapper.setup(reader, context, null);

		// check the counters
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "Processed"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "Unmapped"));

		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "Used"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "SnpMismatches"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "SnpBases"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "NonSnpMismatches"));
	}

	@Test
	public void testSimpleMap() throws IOException, InterruptedException
	{
		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(littleSam), context);

		// check counters
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "Processed"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "Unmapped"));

		assertEquals(3, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "Used"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "BadBases"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "SnpMismatches"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "SnpBases"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "NonSnpMismatches"));

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
		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(littleReversedSam), context);

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
		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(littleSamRead2), context);

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
		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(littleReversedSamRead2), context);

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
		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(littleSamWithN), context);

		assertEquals(2, context.getNumWrites());
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "BadBases"));
		assertEquals(2, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "Used"));
	}

	@Test
	public void testSamWithBaseQ0() throws IOException, InterruptedException
	{
		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(littleSamBaseQ0), context);

		assertEquals(2, context.getNumWrites());
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "BadBases"));
		assertEquals(2, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "Used"));
	}

	@Test
	public void testSamWithUnmapped() throws IOException, InterruptedException
	{
		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(littleSamUnmapped), context);

		assertEquals(0, context.getNumWrites());
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "Unmapped"));
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "Processed"));
	}

	@Test
	public void testSamWithMapQ0() throws IOException, InterruptedException
	{
		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(littleSam.replaceFirst("37", "0")), context);

		assertEquals(0, context.getNumWrites());
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "Unmapped"));
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$ReadCounters", "Processed"));
	}

	@Test
	public void testWithSnp() throws IOException, InterruptedException
	{
		reader.snpList.add(new SnpDef("chr6", 2)); // falls right in the middle of the read

		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(littleSam), context);

		// how many pairs emitted?
		assertEquals(2, context.getNumWrites());

		assertEquals(2, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "Used"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "SnpMismatches"));
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "SnpBases"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "NonSnpMismatches"));

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
	public void testWithSnpWithMismatch() throws IOException, InterruptedException
	{
		reader.snpList.add(new SnpDef("chr6", 2)); // falls right in the middle of the read

		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(littleSam.replace("MD:Z:3", "MD:Z:1A1")), context);

		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "SnpMismatches"));
		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "SnpBases"));
		assertEquals(0, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "NonSnpMismatches"));
	}

	@Test
	public void testWithNonSnpMismatch() throws IOException, InterruptedException
	{
		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(littleSam.replace("MD:Z:3", "MD:Z:1A1")), context);

		assertEquals(1, context.getCounterValue("it.crs4.seal.recab.RecabTableMapper$BaseCounters", "NonSnpMismatches"));
	}

	@Test
	public void testDeletion() throws IOException, InterruptedException
	{
		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(deletion), context);

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
		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(insertion), context);

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
		mapper.setup(reader, context, null);
		mapper.map(new LongWritable(0), new Text(littleSam.replaceFirst("\tRG:Z.*", "")), context);
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