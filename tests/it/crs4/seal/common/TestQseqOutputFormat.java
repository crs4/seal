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

import it.crs4.seal.common.QseqOutputFormat.QseqRecordWriter;
import it.crs4.seal.common.SequencedFragment;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;

import org.junit.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class TestQseqOutputFormat
{
	private SequencedFragment fragment;

	private ByteArrayOutputStream outputBuffer;
	private DataOutputStream dataOutput;
	private QseqRecordWriter writer;

	@Before
	public void setup() throws IOException
	{
		fragment = new SequencedFragment();
		fragment.setInstrument("instrument");
		fragment.setRunNumber(1);
		fragment.setFlowcellId("xyz");
		fragment.setLane(2);
		fragment.setTile(1001);
		fragment.setXpos(10000);
		fragment.setYpos(9999);
		fragment.setRead(1);
		fragment.setFilterPassed(true);
		fragment.setIndexSequence("CATCAT");
		fragment.setSequence(new Text("AAAAAAAAAA"));
		fragment.setQuality(new Text("##########"));

		outputBuffer = new ByteArrayOutputStream();
		dataOutput = new DataOutputStream(outputBuffer);
		writer = new QseqRecordWriter(new Configuration(), dataOutput);
	}

	@Test
	public void testSimple() throws IOException
	{
		writer.write(null, fragment);
		writer.close(null);

		String[] fields = new String(outputBuffer.toByteArray(), "US-ASCII").split("\t");
		assertEquals(11, fields.length);

		assertEquals(fragment.getInstrument(), fields[0]);
		assertEquals(fragment.getRunNumber().toString(), fields[1]);
		assertEquals(fragment.getLane().toString(), fields[2]);
		assertEquals(fragment.getTile().toString(), fields[3]);
		assertEquals(fragment.getXpos().toString(), fields[4]);
		assertEquals(fragment.getYpos().toString(), fields[5]);
		assertEquals(fragment.getIndexSequence().toString(), fields[6]);
		assertEquals(fragment.getRead().toString(), fields[7]);
		assertEquals(fragment.getSequence().toString(), fields[8]);
		assertEquals(fragment.getQuality().toString().replace('#', 'B'), fields[9]);
		assertEquals(fragment.getFilterPassed() ? "1\n" : "0\n", fields[10]);
	}

	@Test
	public void testConvertUnknowns() throws IOException, UnsupportedEncodingException
	{
		String seq = "AAAAANNNNN";
		fragment.setSequence(new Text(seq));
		writer.write(null, fragment);
		writer.close(null);

		String[] fields = new String(outputBuffer.toByteArray(), "US-ASCII").split("\t");
		assertEquals(seq.replace("N", "."), fields[8]);
	}

	@Test
	public void testConvertUnknownsInIndexSequence() throws IOException, UnsupportedEncodingException
	{
		String index = "CATNNN";
		fragment.setIndexSequence(index);
		writer.write(null, fragment);
		writer.close(null);

		String[] fields = new String(outputBuffer.toByteArray(), "US-ASCII").split("\t");
		assertEquals(index.replace("N", "."), fields[6]);
	}

	@Test
	public void testBaseQualities() throws IOException
	{
		// ensure sanger qualities are converted to illumina
		String seq = "AAAAAAAAAA";
		String qual = "##########";

		fragment.setSequence(new Text(seq));
		fragment.setQuality(new Text(qual));

		writer.write(null, fragment);
		writer.close(null);

		String[] fields = new String(outputBuffer.toByteArray(), "US-ASCII").split("\t");
		assertEquals(qual.replace("#", "B"), fields[9]);
	}

	@Test
	public void testConfigureOutputInSanger() throws IOException
	{
		String seq = "AAAAAAAAAA";
		String qual = "##########";

		fragment.setSequence(new Text(seq));
		fragment.setQuality(new Text(qual));

		Configuration conf = new Configuration();
		conf.set("seal.qseq-output.base-quality-encoding", "sanger");
		writer.setConf(conf);

		writer.write(null, fragment);
		writer.close(null);

		String[] fields = new String(outputBuffer.toByteArray(), "US-ASCII").split("\t");
		assertEquals(qual, fields[9]);
	}

	@Test
	public void testClose() throws IOException
	{
		// doesn't really do anything but exercise the code
		writer.close(null);
	}
}
