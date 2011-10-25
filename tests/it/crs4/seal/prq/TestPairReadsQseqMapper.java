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

package tests.it.crs4.seal.prq;

import org.junit.*;
import static org.junit.Assert.*;

import it.crs4.seal.prq.PairReadsQSeqMapper;
import it.crs4.seal.prq.SequencedFragment;
import it.crs4.seal.common.SequenceId;
import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.TestContext;

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

		assertEquals(1, context.output.size());
		SequenceId key = context.output.keySet().iterator().next();
		assertEquals("Instrument_99:2:3:4:5#AGCT", key.getLocation());
		assertEquals(1, key.getRead());

		Text value = context.output.get(key);
		assertEquals("AAAAAAAAAA\tBBBBBBBBBB\t1", value.toString());
	}
}
