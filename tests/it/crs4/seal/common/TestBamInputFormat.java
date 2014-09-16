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
import it.crs4.seal.common.BamInputFormat;
import it.crs4.seal.common.BamInputFormat.BamRecordReader;
import it.crs4.seal.common.AbstractTaggedMapping;
import it.crs4.seal.common.Utils;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.commons.codec.binary.Base64;

import org.seqdoop.hadoop_bam.FileVirtualSplit;

@Ignore("Bam input not ready yet")
public class TestBamInputFormat
{
	// a base64-encoded bam file with one read
/*
@HD	VN:1.0	GO:none	SO:coordinate
@SQ	SN:chr6	LN:249250621
ERR020229.100000/1	81	chr6	3558357	37	91M	=	3558678	-400	AGCTTCTTTGACTCTCGAATTTTAGCACTAGAAGAAATAGTGAGGATTATATATTTCAGAAGTTCTCACCCAGGATATCAGAACACATTCA5:CB:CCBCCB>:C@;BBBB??B;?>1@@=C=4ACCAB3A8=CC=C?CBC=CBCCCCCCCCCCCCC@5>?=?CAAB=3=>====5>=AC?C	XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1	X1:i:0	XM:i:0	XO:i:0	XG:i:0	MD:Z:91
*/
	public static final String oneRecord =
		"H4sIBAAAAAAA/wYAQkMCAGkAc3L0ZbRiYGBw8HDhDPOzMtQz4HT3t8rLz0vlDPa3Ss7PL0rJzEssSeVyCA7kDPazSs4oMuP08bMyMrE0MjUwMzLkYgTqZgVikAyDrfNtPgA/6Q/JUwAAAB+LCAQAAAAAAP8GAEJDAgDeAE2KQW7CMBBFhwV7cFzhRGPHMxISK5pkEYmlSVAXVVqVFilqL9ErEGXh43AYTsEhCI5gwdMf/a/Ru8Cd83sJ0ZKjCXzB3+P30pTwfx2G3X6fFVlRbNZ5NvKaw2kKIFed70XXOeWPW3F0cq5kL51XSnly0q+IqVaKnBCqm8mYKWamEB1zmlDAGEqMnqUpMkaW2ZKwC2RGNkHEUX4ildqgYWsJBWoMSI02qO2PO3w0FXw31dKFbrNq0uahx/0Z7q2Cpv7d5HAD4/H1G+4AAAAfiwgEAAAAAAD/BgBCQwIAGwADAAAAAAAAAAAA";

/*
@HD	VN:1.0	GO:none	SO:coordinate
@SQ	SN:1	LN:249250621
Read/2	153	1	10018	25	101M	=	10018	0	CTAACCCTAACCCTAACCCTACCCCTAACCCTAACCCTTAACCTAACCCTAACCCTAACCCTTAACCTAACCCTAACCCTAACCCTAACCCAACCCTAACC	##@@A;;4<2<=@.>.@7?5='?B;@B>EGEADAD?@<.>5@B<A0>>>:=>EE@EF@BGEF:ECFEAEDEEEDE>:=>:9AFEFDGFHHHHDHFHFHHHH
Read/1	117	1	10018	0	*	=	10018	0	TTAGGGCTAGGGCTAGGGCTAGGGCTAGGGTTAGGGTTAGGGCTAGGGCTAGGGCTAGGGCTAGGGCTAGGGTTAGGGTTAGGGTTAGGGTTAGGGTTAGG	##########DD=A2DBD><D:??.@D8DF>FBDFGDFFHHHADFFHEEFEEC=AFFCFF9GFEGECBGEFHHHGHHHHEHGHEHHHHGHHHHGHHGHHFH
*/
	public static final String twoRecords =
		"H4sIBAAAAAAA/wYAQkMCAGQAc3L0ZTRnYGBw8HDhDPOzMtQz4HT3t8rLz0vlDPa3Ss7PL0rJzEssSeVyCA7kDAYq4PTxszIysTQyNTAzMuRiBGplAmJDBlvn23wAVOTHhU0AAAAfiwgEAAAAAAD/BgBCQwIA9wBtjz1OxDAQhYcUSxWhmAg28d/YM2OnQ3ACCprtVnsDJPYIHGC7vQKnoeBi2IFAin36rHl+08z7gh+FDHA97NQVfMBxlVUdjq9vD0+w3wBMXfxFLeak/sO172JQ8aSwabzHcbzVnTa+ta2/c73ZuDD6YDkxEpLzurW9DxpvrLWDscyexYfEMnAURibm8upq2KKwUJJcRFnybD7XNWCnAN4v13iEc/8yrTjPTJfChefmT0QGOwpkNQ3OtZ7uSawEkkRSD8E6mIU5GhSJItskpWWsXco+1WO5DM7LL81I/gZTG9aYigEAAB+LCAQAAAAAAP8GAEJDAgAbAAMAAAAAAAAAAAA=";

	private Configuration conf;
	private TaskAttemptContext context;
	private InputSplit split;
	private File tempFile;

	private LongWritable key;
	private ReadPair pair;

	private BamInputFormat format;

	@Before
	public void setup() throws IOException
	{
		tempFile = File.createTempFile("test_bam_input_format", ".bam");

		conf = new Configuration();

		key = null;
		pair = null;

		format = new BamInputFormat();
		context = Utils.getTaskAttemptContext(conf);
	}

	@After
	public void tearDown()
	{
		tempFile.delete();
	}

	private long writeToTemp(String b64) throws IOException
	{
		long written = 0;

		BufferedOutputStream out = new BufferedOutputStream( new FileOutputStream(tempFile) );
		// Below, we use Base64.decodeBase64(byte[]) for compatibility with apache commons 1.3,
		// which is bundled with Hadoop 0.20
		byte[] data = Base64.decodeBase64(b64.getBytes());
		out.write(data);
		out.close();

		written += data.length;

		return written;
	}

	private FileVirtualSplit makeVirtualSplit(FileSplit fsplit) throws IOException
	{
		List<InputSplit> list = new ArrayList<InputSplit>(1);
		list.add(fsplit);
		list = format.getVirtualSplits(list, conf);
		assertEquals(1, list.size());

		return (FileVirtualSplit)list.get(0);
	}

	private BamRecordReader createReaderForData(String b64Data) throws IOException, InterruptedException
	{
		return createReaderForData(b64Data, 0, -1);
	}

	private BamRecordReader createReaderForData(String b64Data, long splitStart, long splitEnd) throws IOException, InterruptedException
	{
		long fileSize = writeToTemp(b64Data);
		if (splitEnd < 0)
			splitEnd = fileSize;

		FileSplit fsplit = new FileSplit(new Path(tempFile.toURI().toString()), splitStart, splitEnd, null);

		split = makeVirtualSplit(fsplit);
		BamRecordReader reader = (BamRecordReader)format.createRecordReader(split, context);
		reader.initialize(split, context);

		return reader;
	}

	@Test
	public void testReadFromStart() throws IOException, NoSuchFieldException, InterruptedException
	{
		BamRecordReader reader = createReaderForData(oneRecord);

		assertEquals(0.0, reader.getProgress(), 0.01);

		boolean retval = reader.nextKeyValue();
		assertTrue(retval);

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

		assertEquals("AGCTTCTTTGACTCTCGAATTTTAGCACTAGAAGAAATAGTGAGGATTATATATTTCAGAAGTTCTCACCCAGGATATCAGAACACATTCA",
			map.getSequenceString());

		assertEquals("5:CB:CCBCCB>:C@;BBBB??B;?>1@@=C=4ACCAB3A8=CC=C?CBC=CBCCCCCCCCCCCCC@5>?=?CAAB=3=>====5>=AC?C",
			map.getBaseQualitiesString());

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

		// This test on the progress is kind of arbitrary.  BamInputFormat doesn't report
		// very accurate progress due the underlying implementation in Hadoop-BAM.
		assertTrue(reader.getProgress() > 0.5);

		retval = reader.nextKeyValue();
		assertFalse(retval);
	}

	@Ignore("test doesn't work.  Need appropriate split location")
	@Test
	public void testReadStartInMiddle() throws IOException, InterruptedException
	{
		BamRecordReader reader = createReaderForData(twoRecords, 102, -1);

		assertEquals(0.0, reader.getProgress(), 0.01);

		boolean retval = reader.nextKeyValue();
		assertTrue(retval);

		pair = reader.getCurrentValue();

		assertEquals("Read", pair.getName());
		assertNull("reader didn't skip the first record", pair.getRead2());

		AbstractTaggedMapping map = pair.getAnyRead();
		assertEquals("Read/1", map.getName());

		assertEquals(1.0, reader.getProgress(), 0.21);

		retval = reader.nextKeyValue();
		assertFalse(retval);
	}

	@Test
	public void testSliceEndsBeforeEndOfFile() throws IOException, InterruptedException
	{
		BamRecordReader reader = createReaderForData(twoRecords, 0, 370);

		boolean retval = reader.nextKeyValue();
		assertTrue(retval);

		assertFalse("BamRecordReader is reading a record that starts after the end of the slice", reader.nextKeyValue());
	}

	@Test
	public void testProgress() throws IOException, InterruptedException
	{
		// getProgress() in BamRecordReader isn't really accurate, at least on a small
		// scale such as the two records we're using for this unit test.

		BamRecordReader reader = createReaderForData(twoRecords);

		assertEquals(0.0, reader.getProgress(), 0.01);

		reader.nextKeyValue();
		//assertEquals(0.5, reader.getProgress(), 0.2);

		reader.nextKeyValue();
		assertEquals(1.0, reader.getProgress(), 0.21);
	}

	@Test
	public void testClose() throws IOException, InterruptedException
	{
		BamRecordReader reader = createReaderForData(oneRecord);
		// doesn't really do anything but exercise the code
		reader.close();
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestBamInputFormat.class.getName());
	}
}
