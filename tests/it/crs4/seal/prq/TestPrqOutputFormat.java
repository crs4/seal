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

import it.crs4.seal.common.AbstractTaggedMapping;
import it.crs4.seal.common.ReadPair;
import it.crs4.seal.common.WritableMapping;
import it.crs4.seal.prq.PrqOutputFormat;
import it.crs4.seal.prq.PrqOutputFormat.PrqRecordWriter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class TestPrqOutputFormat
{
	private Text     pair_id;
	private ReadPair pair;

	private AbstractTaggedMapping read1;
	private AbstractTaggedMapping read2;

	private ByteArrayOutputStream outputBuffer;
	private DataOutputStream      dataOutput;
	private PrqRecordWriter       writer;

	@Before
	public void setup() throws IOException
	{
		read1   = new WritableMapping("read/1", "AAAAAAAAAA", "##########");
		read2   = new WritableMapping("read/2", "GGGGGGGGGG", "$$$$$$$$$$");
		pair    = new ReadPair(read1, read2);
		pair_id = new Text("pair_id");

		outputBuffer = new ByteArrayOutputStream();
		dataOutput   = new DataOutputStream(outputBuffer);
		writer       = new PrqRecordWriter(dataOutput);
	}

	private String[] getFieldsFromOutput() throws UnsupportedEncodingException
	{
		byte[] array = outputBuffer.toByteArray();
		int length = array.length;

		// trim the trailing newline
		if (array[length - 1] == '\n')
			length -= 1;

		return new String(array, 0, length, "US-ASCII").split("\t", 6);
	}

	private static String byteBufferToString(ByteBuffer buf) throws UnsupportedEncodingException
	{
		return new String(buf.array(), buf.position(), buf.limit() - buf.position(), "US-ASCII");
	}

	@Test
	public void testWriteKeyPair() throws IOException
	{
		writer.write(pair_id, pair);

		String[] fields = getFieldsFromOutput();

		assertEquals(5, fields.length);
		assertEquals(pair_id.toString(), fields[0]);

		assertEquals(byteBufferToString(read1.getSequence()), fields[1]);
		assertEquals(byteBufferToString(read1.getBaseQualities()), fields[2]);

		assertEquals(byteBufferToString(read2.getSequence()), fields[3]);
		assertEquals(byteBufferToString(read2.getBaseQualities()), fields[4]);
	}

	@Test
	public void testWritePairNoKey() throws IOException
	{
		writer.write(null, pair);

		String[] fields = getFieldsFromOutput();

		assertEquals(5, fields.length);
		assertEquals(pair.getName(), fields[0]);

		assertEquals(byteBufferToString(read1.getSequence()), fields[1]);
		assertEquals(byteBufferToString(read1.getBaseQualities()), fields[2]);

		assertEquals(byteBufferToString(read2.getSequence()), fields[3]);
		assertEquals(byteBufferToString(read2.getBaseQualities()), fields[4]);
	}

	@Test
	public void testWriteUnpairedRead1() throws IOException
	{
		pair.setRead2(null);
		writer.write(pair_id, pair);

		String[] fields = getFieldsFromOutput();

		assertEquals(5, fields.length);
		assertEquals(pair_id.toString(), fields[0]);

		assertEquals(byteBufferToString(read1.getSequence()), fields[1]);
		assertEquals(byteBufferToString(read1.getBaseQualities()), fields[2]);

		assertEquals("", fields[3]);
		assertEquals("", fields[4]);
	}

	@Test
	public void testWriteUnpairedRead2() throws IOException
	{
		pair.setRead1(null);
		writer.write(pair_id, pair);

		String[] fields = getFieldsFromOutput();

		assertEquals(5, fields.length);
		assertEquals(pair_id.toString(), fields[0]);

		assertEquals("", fields[1]);
		assertEquals("", fields[2]);

		assertEquals(byteBufferToString(read2.getSequence()), fields[3]);
		assertEquals(byteBufferToString(read2.getBaseQualities()), fields[4]);
	}

	@Test
	public void testClose() throws IOException
	{
		// doesn't really do anything but exercise the code
		writer.close(null);
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestPrqOutputFormat.class.getName());
	}
}
