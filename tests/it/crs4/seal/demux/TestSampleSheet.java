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

import it.crs4.seal.demux.SampleSheet;

import java.io.StringReader;

import org.junit.*;
import static org.junit.Assert.*;

public class TestSampleSheet 
{
	private SampleSheet sheet;
	private StringReader sampleReader;

	private String sampleSheet =
		"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
		"\"81DJ0ABXX\",1,\"snia_000268\",\"Human\",\"ATCACG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",1,\"snia_000269\",\"Human\",\"CGATGT\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",2,\"snia_001611\",\"Human\",\"TTAGGC\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",2,\"snia_025487\",\"Human\",\"TGACCA\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",3,\"snia_041910\",\"Human\",\"ACAGTG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",3,\"snia_001612\",\"Human\",\"GCCAAT\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"";

	private String dupSampleSheet =
		"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
		"\"81DJ0ABXX\",1,\"snia_000268\",\"Human\",\"ATCACG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",1,\"snia_000269\",\"Human\",\"ATCACG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"";

	private String badLaneSheet =
		"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
		"\"81DJ0ABXX\",0,\"snia_000269\",\"Human\",\"ATCACG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"";

	private String lanesOOOSheet =
		"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
		"\"81DJ0ABXX\",3,\"snia_041910\",\"Human\",\"ACAGTG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",1,\"snia_000268\",\"Human\",\"ATCACG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",1,\"snia_000269\",\"Human\",\"CGATGT\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",2,\"snia_001611\",\"Human\",\"TTAGGC\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",3,\"snia_001612\",\"Human\",\"GCCAAT\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",2,\"snia_025487\",\"Human\",\"TGACCA\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"";

	@Before
	public void setup()
	{
		sheet = new SampleSheet();
		sampleReader = new StringReader(sampleSheet);
	}

	@Test
	public void testDontCrashOnEmpty()
	{
		assertNull(sheet.getSampleId(1, "aaa"));
	}

	@Test(expected=SampleSheet.FormatException.class)
	public void testDetectDuplicateIndex() throws java.io.IOException, SampleSheet.FormatException
	{
		sheet.loadTable(new StringReader(dupSampleSheet));
	}

	@Test(expected=SampleSheet.FormatException.class)
	public void testInvalidLaneNo() throws java.io.IOException, SampleSheet.FormatException
	{
		sheet.loadTable(new StringReader(badLaneSheet));
	}

	@Test
	public void testLanesOutOfOrder() throws java.io.IOException, SampleSheet.FormatException
	{
		sheet.loadTable(new StringReader(lanesOOOSheet));
		assertEquals("snia_041910", sheet.getSampleId(3, "ACAGTG"));
		assertEquals("snia_001611", sheet.getSampleId(2, "TTAGGC"));
	}

	@Test
	public void testLanesInOrder() throws java.io.IOException, SampleSheet.FormatException
	{
		sheet.loadTable(new StringReader(sampleSheet));
		assertEquals("snia_041910", sheet.getSampleId(3, "ACAGTG"));
		assertEquals("snia_001611", sheet.getSampleId(2, "TTAGGC"));
	}

	@Test
	public void testIsEmpty() throws java.io.IOException, SampleSheet.FormatException
	{
		assertTrue(sheet.isEmpty());
		sheet.loadTable(sampleReader);
		assertFalse(sheet.isEmpty());
	}

	@Test
	public void testNSamples() throws java.io.IOException, SampleSheet.FormatException
	{
		assertEquals(0, sheet.getNumSamples());
		sheet.loadTable(sampleReader);
		assertEquals(6, sheet.getNumSamples());
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestSampleSheet.class.getName());
	}
}
