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


package tests.it.crs4.seal.common;

import it.crs4.seal.common.AlignOp;
import it.crs4.seal.common.ReadPair;
import it.crs4.seal.common.SamInputFormat;
import it.crs4.seal.common.SamInputFormat.SamRecordReader;
import it.crs4.seal.common.AbstractTaggedMapping;

import org.junit.*;
import static org.junit.Assert.*;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class TestSamInputFormat
{
	public static final String oneRecord = "ERR020229.100000/1	81	chr6	3558357	37	91M	=	3558678	400	AGCTTCTTTGACTCTCGAATTTTAGCACTAGAAGAAATAGTGAGGATTATATATTTCAGAAGTTCTCACCCAGGATATCAGAACACATTCA	5:CB:CCBCCB>:C@;BBBB??B;?>1@@=C=4ACCAB3A8=CC=C?CBC=CBCCCCCCCCCCCCC@5>?=?CAAB=3=>====5>=AC?C	XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:91";

	public static final String twoRecords =
		"Read/2	153	1	10018	25	101M	=	10018	0	CTAACCCTAACCCTAACCCTACCCCTAACCCTAACCCTTAACCTAACCCTAACCCTAACCCTTAACCTAACCCTAACCCTAACCCTAACCCAACCCTAACC	##@@A;;4<2<=@.>.@7?5='?B;@B>EGEADAD?@<.>5@B<A0>>>:=>EE@EF@BGEF:ECFEAEDEEEDE>:=>:9AFEFDGFHHHHDHFHFHHHH"
		+ "\n" +
		"Read/1	117	1	10018	0	*	=	10018	0	TTAGGGCTAGGGCTAGGGCTAGGGCTAGGGTTAGGGTTAGGGCTAGGGCTAGGGCTAGGGCTAGGGCTAGGGTTAGGGTTAGGGTTAGGGTTAGGGTTAGG	##########DD=A2DBD><D:??.@D8DF>FBDFGDFFHHHADFFHEEFEEC=AFFCFF9GFEGECBGEFHHHGHHHHEHGHEHHHHGHHHHGHHGHHFH";

	public static final String unmapped = "UNMAPPED	93	*	*	0	*	=	3558678	*	AGCTT	5:CB:";

	private Configuration conf;
	private FileSplit split;
	private File tempFile;
	private File tempGz;

	private LongWritable key;
	private ReadPair pair;

	@Before
	public void setup() throws IOException
	{
		tempFile = File.createTempFile("test_sam_input_format", ".sam");
		tempGz = File.createTempFile("test_sam_input_format", ".gz");

		conf = new Configuration();

		key = null;
		pair = null;
	}

	@After
	public void tearDown()
	{
		tempFile.delete();
		tempGz.delete();
		split = null;
	}

	private void writeToTemp(String s) throws IOException
	{
		PrintWriter out = new PrintWriter( new BufferedWriter( new FileWriter(tempFile) ) );
		out.write(s);
		out.close();
	}

	private SamRecordReader createReaderForOneRecord() throws IOException
	{
		writeToTemp(oneRecord);
		split = new FileSplit(new Path(tempFile.toURI().toString()), 0, oneRecord.length(), null);

		SamRecordReader reader = new SamRecordReader();
		reader.initialize(split, new TaskAttemptContext(conf, new TaskAttemptID()));

		return reader;
	}

	@Test
	public void testReadFromStart() throws IOException, NoSuchFieldException
	{
		SamRecordReader reader = createReaderForOneRecord();

		assertEquals(0.0, reader.getProgress(), 0.01);

		boolean retval = reader.nextKeyValue();
		assertTrue(retval);
		assertEquals(new LongWritable(0), reader.getCurrentKey());

		pair = reader.getCurrentValue();

		AbstractTaggedMapping map = pair.getRead1();
		assertNotNull(map);
		assertNull(pair.getRead2());

		// test that the record has been read correctly
		assertEquals("ERR020229.100000/1", map.getName());
		assertEquals(81, map.getFlag());
		assertEquals("chr6", map.getContig());
		assertEquals(3558357, map.get5Position());
		assertEquals(37, map.getMapQ());
		assertEquals("91M", AlignOp.cigarStr(map.getAlignment()));
		assertTrue(map.isTemplateLengthAvailable());
		assertEquals(400, map.getTemplateLength());

		ByteBuffer buffer = map.getSequence();
		assertEquals("AGCTTCTTTGACTCTCGAATTTTAGCACTAGAAGAAATAGTGAGGATTATATATTTCAGAAGTTCTCACCCAGGATATCAGAACACATTCA",
			new String(buffer.array(), buffer.position(), (buffer.limit() - buffer.position())));

		buffer = map.getBaseQualities();
		assertEquals("5:CB:CCBCCB>:C@;BBBB??B;?>1@@=C=4ACCAB3A8=CC=C?CBC=CBCCCCCCCCCCCCC@5>?=?CAAB=3=>====5>=AC?C",
			new String(buffer.array(), buffer.position(), (buffer.limit() - buffer.position())));

		assertEquals(0, map.getIntTag("NM"));
		assertEquals(37, map.getIntTag("SM"));
		assertEquals(0, map.getIntTag("AM"));
		assertEquals(1, map.getIntTag("X0"));
		assertEquals(0, map.getIntTag("X1"));
		assertEquals(0, map.getIntTag("XM"));
		assertEquals(0, map.getIntTag("XO"));
		assertEquals(0, map.getIntTag("XG"));
		assertEquals("U", map.getTag("XT"));
		assertEquals("91", map.getTag("MD"));

		assertEquals(1.0, reader.getProgress(), 0.01);

		retval = reader.nextKeyValue();
		assertFalse(retval);
	}

	@Test
	public void testReadStartInMiddle() throws IOException
	{
		writeToTemp(twoRecords);
		split = new FileSplit(new Path(tempFile.toURI().toString()), 10, twoRecords.length() - 10, null);

		SamRecordReader reader = new SamRecordReader();
		reader.initialize(split, new TaskAttemptContext(conf, new TaskAttemptID()));

		assertEquals(0.0, reader.getProgress(), 0.01);

		boolean retval = reader.nextKeyValue();
		assertTrue(retval);
		assertEquals(new LongWritable(241), reader.getCurrentKey());

		pair = reader.getCurrentValue();

		assertEquals("Read", pair.getName());
		assertNull(pair.getRead2());

		AbstractTaggedMapping map = pair.getAnyRead();
		assertEquals("Read/1", map.getName());

		assertEquals(1.0, reader.getProgress(), 0.01);

		retval = reader.nextKeyValue();
		assertFalse(retval);
	}

	@Test
	public void testSliceEndsBeforeEndOfFile() throws IOException
	{
		writeToTemp(twoRecords);
		// slice ends at position 10--i.e. somewhere in the first record.  The second record should not be read.
		split = new FileSplit(new Path(tempFile.toURI().toString()), 0, 10, null);

		SamRecordReader reader = new SamRecordReader();
		reader.initialize(split, new TaskAttemptContext(conf, new TaskAttemptID()));

		boolean retval = reader.nextKeyValue();
		assertTrue(retval);
		assertEquals(new LongWritable(0), reader.getCurrentKey());

		assertFalse("SamRecordReader is reading a record that starts after the end of the slice", reader.nextKeyValue());
	}

	@Test
	public void testProgress() throws IOException
	{
		writeToTemp(twoRecords);
		split = new FileSplit(new Path(tempFile.toURI().toString()), 0, twoRecords.length(), null);

		SamRecordReader reader = new SamRecordReader();
		reader.initialize(split, new TaskAttemptContext(conf, new TaskAttemptID()));

		assertEquals(0.0, reader.getProgress(), 0.01);

		reader.nextKeyValue();
		assertEquals(0.5, reader.getProgress(), 0.01);

		reader.nextKeyValue();
		assertEquals(1.0, reader.getProgress(), 0.01);
	}

	@Test
	public void testClose() throws IOException
	{
		SamRecordReader reader = createReaderForOneRecord();
		// doesn't really do anything but exercise the code
		reader.close();
	}

	@Test
	public void testGzCompressedInput() throws IOException
	{
		// write gzip-compressed data
		GzipCodec codec = new GzipCodec();
		PrintWriter out = new PrintWriter( new BufferedOutputStream( codec.createOutputStream( new FileOutputStream(tempGz) ) ) );
		out.write(twoRecords);
		out.close();

		// now try to read it
		split = new FileSplit(new Path(tempGz.toURI().toString()), 0, twoRecords.length(), null);

		SamRecordReader reader = new SamRecordReader();
		reader.initialize(split, new TaskAttemptContext(conf, new TaskAttemptID()));

		boolean retval = reader.nextKeyValue();
		assertTrue(retval);
		assertEquals("Read/2", reader.getCurrentValue().getAnyRead().getName());

		retval = reader.nextKeyValue();
		assertTrue(retval);
		assertEquals("Read/1", reader.getCurrentValue().getAnyRead().getName());
	}

	@Test(expected=RuntimeException.class)
	public void testCompressedSplit() throws IOException
	{
		// write gzip-compressed data
		GzipCodec codec = new GzipCodec();
		PrintWriter out = new PrintWriter( new BufferedOutputStream( codec.createOutputStream( new FileOutputStream(tempGz) ) ) );
		out.write(twoRecords);
		out.close();

		// now try to read it starting from the middle
		
		SamInputFormat inputFormat = new SamInputFormat();

		split = new FileSplit(new Path(tempGz.toURI().toString()), 10, twoRecords.length(), null);
		RecordReader<LongWritable, ReadPair> reader = inputFormat.createRecordReader(split, new TaskAttemptContext(conf, new TaskAttemptID()));
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestSamInputFormat.class.getName());
	}
}
