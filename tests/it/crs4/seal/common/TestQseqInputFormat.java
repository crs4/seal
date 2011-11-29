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

import it.crs4.seal.common.QseqInputFormat.QseqRecordReader;
import it.crs4.seal.common.SequencedFragment;
import it.crs4.seal.common.FormatException;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;

import org.junit.*;
import static org.junit.Assert.*;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TestQseqInputFormat
{
	public static final String oneQseq =
		"ERR020229	10880	1	1	1373	2042	0	1	" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\t" +
		"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB	1";

	public static final String twoQseq = 
		"ERR020229	10880	1	1	1373	2042	0	1	" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\t" +
		"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB	1\n" +
		"ERR020229	10883	1	1	1796	2044	0	1	" +
		"TGAGCAGATGTGCTAAAGCTGCTTCTCCCCTAGGATCATTTGTACCTACCAGACTCAGGGAAAGGGGTGAGAATTGGGCCGTGGGGCAAGG\t" +
		"DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD	1";

	public static final String illuminaQseq = 
		"EAS139	136	2	5	1000	12850	ATCACG	1	" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\t" +	
		"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB	0";

	public static final String nQseq =
		"ERR020229	10880	1	1	1373	2042	0	1	" +
		"...........................................................................................\t" +	
		"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB	0";


	public static final String sangerQseq = 
		"EAS139	136	2	5	1000	12850	ATCACG	1	" +
		"TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT\t" +	
		"###########################################################################################	0";

	public static final String indexWithUnknown = 
		"EAS139	136	2	5	1000	12850	ATC..G	1	" +
		"TTGGATGATAGGGATTATTTGACTCGAATAT\t" +	
		"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\t0";


	private JobConf conf;
	private FileSplit split;
	private File tempQseq;

	private Text key;
	private SequencedFragment fragment;

	@Before
	public void setup() throws IOException
	{
		tempQseq = File.createTempFile("test_qseq_input_format", "qseq");
		conf = new JobConf();
		key = new Text();
		fragment = new SequencedFragment();
	}

	@After
	public void teardDown()
	{
		tempQseq.delete();
		split = null;
	}

	private void writeToTempQseq(String s) throws IOException
	{
		PrintWriter qseqOut = new PrintWriter( new BufferedWriter( new FileWriter(tempQseq) ) );
		qseqOut.write(s);
		qseqOut.close();
	}

	private QseqRecordReader createReaderForOneQseq() throws IOException
	{
		writeToTempQseq(oneQseq);
		split = new FileSplit(new Path(tempQseq.toURI().toString()), 0, oneQseq.length(), null);

		return new QseqRecordReader(conf, split);
	}

	@Test
	public void testReadFromStart() throws IOException
	{
		QseqRecordReader reader = createReaderForOneQseq();

		assertEquals(0, reader.getPos());
		assertEquals(0.0, reader.getProgress(), 0.01);

		boolean retval = reader.next(key, fragment);
		assertTrue(retval);
//System.err.println("in testReadFromStart quality: " + fragment.getQuality().toString());
		assertEquals("ERR020229:10880:1:1:1373:2042:1", key.toString());
		assertEquals("TTGGATGATAGGGATTATTTGACTCGAATATTGGAAATAGCTGTTTATATTTTTTAAAAATGGTCTGTAACTGGTGACAGGACGCTTCGAT", fragment.getSequence().toString());
		assertEquals("###########################################################################################", fragment.getQuality().toString());

		assertEquals(oneQseq.length(), reader.getPos());
		assertEquals(1.0, reader.getProgress(), 0.01);

		retval = reader.next(key, fragment);
		assertFalse(retval);
	}

	@Test
	public void testReadStartInMiddle() throws IOException
	{
		writeToTempQseq(twoQseq);
		split = new FileSplit(new Path(tempQseq.toURI().toString()), 10, twoQseq.length() - 10, null);

		QseqRecordReader reader = new QseqRecordReader(conf, split);

		assertEquals(oneQseq.length() + 1, reader.getPos()); // The start of the second record. We +1 for the \n that is not in oneQseq
		assertEquals(0.0, reader.getProgress(), 0.01);

		boolean retval = reader.next(key, fragment);
		assertTrue(retval);
		assertEquals("ERR020229:10883:1:1:1796:2044:1", key.toString());
		assertEquals("TGAGCAGATGTGCTAAAGCTGCTTCTCCCCTAGGATCATTTGTACCTACCAGACTCAGGGAAAGGGGTGAGAATTGGGCCGTGGGGCAAGG", fragment.getSequence().toString());
		assertEquals("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%", fragment.getQuality().toString());

		assertEquals(twoQseq.length(), reader.getPos()); // now should be at the end of the data
		assertEquals(1.0, reader.getProgress(), 0.01);

		retval = reader.next(key, fragment);
		assertFalse(retval);
	}

	@Test
	public void testSliceEndsBeforeEndOfFile() throws IOException
	{
		writeToTempQseq(twoQseq);
		// slice ends at position 10--i.e. somewhere in the first record.  The second record should not be read.
		split = new FileSplit(new Path(tempQseq.toURI().toString()), 0, 10, null);

		QseqRecordReader reader = new QseqRecordReader(conf, split);

		boolean retval = reader.next(key, fragment);
		assertTrue(retval);
		assertEquals("ERR020229:10880:1:1:1373:2042:1", key.toString());

		assertFalse("QseqRecordReader is reading a record that starts after the end of the slice", reader.next(key, fragment)); 
	}

	@Test
	public void testIlluminaMetaInfo() throws IOException
	{
		writeToTempQseq(illuminaQseq);
		split = new FileSplit(new Path(tempQseq.toURI().toString()), 0, illuminaQseq.length(), null);

		QseqRecordReader reader = new QseqRecordReader(conf, split);
		boolean found = reader.next(key, fragment);
		assertTrue(found);

		assertEquals("EAS139", fragment.getInstrument());
		assertEquals(136, fragment.getRunNumber().intValue());
		assertNull("flowcell id not null", fragment.getFlowcellId());
		assertEquals(2, fragment.getLane().intValue());
		assertEquals(5, fragment.getTile().intValue());
		assertEquals(1000, fragment.getXpos().intValue());
		assertEquals(12850, fragment.getYpos().intValue());
		assertEquals(1, fragment.getRead().intValue());
		assertEquals(false, fragment.getFilterPassed().booleanValue());
		assertNull("control number not null", fragment.getControlNumber());
		assertEquals("ATCACG", fragment.getIndexSequence());
	}

	@Test
	public void testNs() throws IOException
	{
		writeToTempQseq(nQseq);
		split = new FileSplit(new Path(tempQseq.toURI().toString()), 0, nQseq.length(), null);

		QseqRecordReader reader = new QseqRecordReader(conf, split);
		boolean found = reader.next(key, fragment);
		assertTrue(found);
		assertEquals("NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN", fragment.getSequence().toString());
	}

	@Test
	public void testConvertDotInIndexSequence() throws IOException
	{
		writeToTempQseq(indexWithUnknown);
		split = new FileSplit(new Path(tempQseq.toURI().toString()), 0, indexWithUnknown.length(), null);

		QseqRecordReader reader = new QseqRecordReader(conf, split);
		boolean found = reader.next(key, fragment);
		assertTrue(found);
		assertEquals("ATCNNG", fragment.getIndexSequence());
	}

	@Test(expected=FormatException.class)
	public void testSangerQualities() throws IOException
	{
		writeToTempQseq(sangerQseq);
		split = new FileSplit(new Path(tempQseq.toURI().toString()), 0, sangerQseq.length(), null);

		QseqRecordReader reader = new QseqRecordReader(conf, split);
		reader.next(key, fragment);
	}

	@Test
	public void testConfigureForSangerQualities() throws IOException
	{
		writeToTempQseq(sangerQseq);
		split = new FileSplit(new Path(tempQseq.toURI().toString()), 0, sangerQseq.length(), null);

		conf.set("bl.qseq.base-quality-encoding", "sanger");
		QseqRecordReader reader = new QseqRecordReader(conf, split);
		assertTrue(reader.next(key, fragment));
		assertEquals("###########################################################################################", fragment.getQuality().toString());
	}


	@Test
	public void testProgress() throws IOException
	{
		writeToTempQseq(twoQseq);
		split = new FileSplit(new Path(tempQseq.toURI().toString()), 0, twoQseq.length(), null);

		QseqRecordReader reader = new QseqRecordReader(conf, split);
		assertEquals(0.0, reader.getProgress(), 0.01);

		reader.next(key, fragment);
		assertEquals(0.5, reader.getProgress(), 0.01);

		reader.next(key, fragment);
		assertEquals(1.0, reader.getProgress(), 0.01);
	}

	@Test
	public void testCreateKey() throws IOException
	{
		QseqRecordReader reader = createReaderForOneQseq();
		assertTrue(reader.createKey() instanceof Text);
	}

	@Test
	public void testCreateValue() throws IOException
	{
		QseqRecordReader reader = createReaderForOneQseq();
		assertTrue(reader.createValue() instanceof SequencedFragment);
	}

	@Test
	public void testClose() throws IOException
	{
		QseqRecordReader reader = createReaderForOneQseq();
		// doesn't really do anything but exercise the code
		reader.close();
	}
}
