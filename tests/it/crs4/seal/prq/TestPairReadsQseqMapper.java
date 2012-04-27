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

import it.crs4.seal.prq.PairReadsQSeqMapper;
import it.crs4.seal.common.SequenceId;
import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.TestContext;

import fi.tkk.ics.hadoop.bam.SequencedFragment;

import org.apache.hadoop.io.Text;
import java.io.IOException;

public class TestPairReadsQseqMapper
{
	private PairReadsQSeqMapper mapper;
	private TestContext<SequenceId, Text> context;
	private Text inputKey = new Text();
	private SequencedFragment inputFragment = new SequencedFragment();

	@Before
	public void setup()
	{
		context = new TestContext<SequenceId, Text>();
		mapper = new PairReadsQSeqMapper();
		mapper.setup();
	}

	@After
	public void teardown()
	{
	}

	@Test
	public void testMap() throws IOException, InterruptedException
	{
		inputKey.set("my key");

		inputFragment.setInstrument( "Instrument" );
		inputFragment.setRunNumber( 99 );
		inputFragment.setLane( 2 );
		inputFragment.setTile( 3 );
		inputFragment.setXpos( 4 );
		inputFragment.setYpos( 5 );
		inputFragment.setRead( 1 );
		inputFragment.setFilterPassed( true );
		inputFragment.setIndexSequence("AGCT");
		inputFragment.getSequence().set("AAAAAAAAAA");
		inputFragment.getQuality().set("BBBBBBBBBB");

		mapper.map(inputKey, inputFragment, context);

		assertEquals(1, context.getNumWrites());
		SequenceId key = context.iterator().next().getKey();
		assertEquals("Instrument_99:2:3:4:5#AGCT", key.getLocation());
		assertEquals(1, key.getRead());

		Text value = context.getValuesForKey(key).get(0);
		assertEquals("AAAAAAAAAA\tBBBBBBBBBB\t1", value.toString());
	}

	@Test
	public void testFastqMap() throws IOException, InterruptedException
	{
		// fastq record without illumina metadata
		inputKey.set("1002_554_890/1");
		inputFragment.getSequence().set("AATCGAATGTAATGGAATCGCAAGGAATTGATGTGAACGGAACGGAATGG");
		inputFragment.getQuality().set("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIEIIIIIIIIIIIIIA");
		inputFragment.setRead(1);

		mapper.map(inputKey, inputFragment, context);

		assertEquals(1, context.getNumWrites());
		SequenceId key = context.iterator().next().getKey();
		assertEquals("1002_554_890", key.getLocation());
		assertEquals(1, key.getRead());

		Text value = context.getValuesForKey(key).get(0);
		assertEquals("AATCGAATGTAATGGAATCGCAAGGAATTGATGTGAACGGAACGGAATGG\tIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIEIIIIIIIIIIIIIA\t1", value.toString());
	}
}
