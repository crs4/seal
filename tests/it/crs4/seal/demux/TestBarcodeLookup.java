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


package tests.it.crs4.seal.demux;

import it.crs4.seal.demux.BarcodeLookup;
import it.crs4.seal.demux.SampleSheet;

import java.io.StringReader;
import java.util.Collection;
import java.util.Set;

import org.junit.*;
import static org.junit.Assert.*;

public class TestBarcodeLookup
{
	private SampleSheet sheet;
	private StringReader sampleReader;
	private BarcodeLookup lookup;

	private String sampleSheet =
		"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
		"\"81DJ0ABXX\",1,\"snia_000268\",\"Human\",\"ATCACG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",1,\"snia_000269\",\"Human\",\"CGATGT\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",2,\"snia_001611\",\"Human\",\"TTAGGC\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",2,\"snia_025487\",\"Human\",\"TGACCA\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",3,\"snia_041910\",\"Human\",\"ACAGTG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",3,\"snia_001612\",\"Human\",\"GCCAAT\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"";

	private String sampleSheetOne =
		"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
		"\"81DJ0ABXX\",1,\"one\",\"Human\",\"ATCACG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"";

	private String sampleSheetTwo =
		"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
		"\"81DJ0ABXX\",1,\"one\",\"Human\",\"ATCACG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",1,\"one\",\"Human\",\"GCACTA\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"";

	// in the following sample sheet, the second record's barcode is only one substitution away from the first one.
	private String sampleSheetTooClose =
		"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
		"\"81DJ0ABXX\",1,\"first\",\"Human\",\"ATCACG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",1,\"too_close\",\"Human\",\"ATCACA\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" ;

	private String lanesOOOSheet =
		"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
		"\"81DJ0ABXX\",3,\"snia_041910\",\"Human\",\"ACAGTG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",1,\"snia_000268\",\"Human\",\"ATCACG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",1,\"snia_000269\",\"Human\",\"CGATGT\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",2,\"snia_001611\",\"Human\",\"TTAGGC\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",3,\"snia_001612\",\"Human\",\"GCCAAT\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",2,\"snia_025487\",\"Human\",\"TGACCA\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"";

	private String sampleSheetSkippingLanes =
		"\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"\n" +
		"\"81DJ0ABXX\",1,\"snia_000268\",\"Human\",\"ATCACG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",1,\"snia_000269\",\"Human\",\"CGATGT\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",3,\"snia_041910\",\"Human\",\"ACAGTG\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"\n" +
		"\"81DJ0ABXX\",3,\"snia_001612\",\"Human\",\"GCCAAT\",\"Whole-Transcriptome Sequencing Project\",\"N\",\"tru-seq multiplex\",\"ROBERTO\"";

	@Before
	public void setup()
	{
		sheet = new SampleSheet();
		lookup = new BarcodeLookup(sheet, 0);
	}

	@Test
	public void testIsEmpty() throws java.io.IOException, SampleSheet.FormatException
	{
		assertTrue(lookup.isEmpty());
		sheet.loadTable(new StringReader(sampleSheet));
		lookup.load(sheet, 0);
		assertFalse(lookup.isEmpty());
	}

	@Test
	public void testNSamples() throws java.io.IOException, SampleSheet.FormatException
	{
		assertEquals(0, lookup.getNumSamples());
		sheet.loadTable(new StringReader(sampleSheetOne));

		lookup.load(sheet, 0);
		assertEquals(1, lookup.getNumSamples());

		sheet.loadTable(new StringReader(sampleSheet));
		lookup.load(sheet, 0);
		assertEquals(6, sheet.size());
		assertEquals(6, lookup.getNumSamples());
	}

	@Test
	public void testDontCrashOnEmpty()
	{
		assertNull(lookup.getSampleId(1, "aaaaaa"));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testInvalidLaneNo()
	{
		lookup.getSampleId(0, "aaaaaa");
	}

	@Test(expected=IllegalArgumentException.class)
	public void testInvalidIndexLength()
	{
		lookup.getSampleId(1, "");
	}

	@Test
	public void testLanesOutOfOrder() throws java.io.IOException, SampleSheet.FormatException
	{
		sheet.loadTable(new StringReader(lanesOOOSheet));
		lookup.load(sheet, 0);
		BarcodeLookup.Match m;

		m = lookup.getSampleId(3, "ACAGTG");
		assertEquals("snia_041910", m.getEntry().getSampleId());
		assertEquals(0, m.getMismatches());

		m = lookup.getSampleId(2, "TTAGGC");
		assertEquals("snia_001611", m.getEntry().getSampleId());
		assertEquals(0, m.getMismatches());
	}

	@Test
	public void testLanesInOrder() throws java.io.IOException, SampleSheet.FormatException
	{
		sheet.loadTable(new StringReader(sampleSheet));
		lookup.load(sheet, 0);
		assertEquals("snia_041910", lookup.getSampleId(3, "ACAGTG").getEntry().getSampleId());
		assertEquals("snia_001611", lookup.getSampleId(2, "TTAGGC").getEntry().getSampleId());
	}

	@Test
	public void testSkippingLanes() throws java.io.IOException, SampleSheet.FormatException
	{
		sheet.loadTable(new StringReader(sampleSheetSkippingLanes));
		lookup.load(sheet, 0);
		assertEquals("snia_041910", lookup.getSampleId(3, "ACAGTG").getEntry().getSampleId());
		assertEquals("snia_000268", lookup.getSampleId(1, "ATCACG").getEntry().getSampleId());
		assertNull(lookup.getSampleId(2, "ATCACG"));
	}


	@Test(expected=IllegalArgumentException.class)
	public void testNegativeMismatchLimit() throws java.io.IOException, SampleSheet.FormatException
	{
		lookup.load(sheet, -1);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testUnreasonableMismatchLimit() throws java.io.IOException, SampleSheet.FormatException
	{
		lookup.load(sheet, 100);
	}


	@Test(expected=RuntimeException.class)
	public void testMismatchLimitTooHigh() throws java.io.IOException, SampleSheet.FormatException
	{
		sheet.loadTable(new StringReader(sampleSheetTooClose));
		lookup.load(sheet, 2);
	}

	@Test
	public void testQueryWithOneSampleInLane() throws java.io.IOException, SampleSheet.FormatException
	{
		sheet.loadTable(new StringReader(sampleSheetOne));
		lookup.load(sheet, 0);
		int lane = 1; // lane where the samples in sampleSheetTwo reside

		BarcodeLookup.Match m;
		// exact query
		m = lookup.getSampleId(lane, "RANDOM");
		assertEquals("one", m.getEntry().getSampleId());
		assertEquals(0, m.getMismatches());

		assertNull(lookup.getSampleId(lane+1, "ATCANN")); // empty lane
	}

	@Test
	public void testQueryWithOneMismatch() throws java.io.IOException, SampleSheet.FormatException
	{
		sheet.loadTable(new StringReader(sampleSheetTwo));
		lookup.load(sheet, 1); // parameter:  support 1 mismatch
		int lane = 1; // lane where the samples in sampleSheetTwo reside

		BarcodeLookup.Match m;
		// exact query
		m = lookup.getSampleId(lane, "ATCACG");
		assertEquals("one", m.getEntry().getSampleId());
		assertEquals(0, m.getMismatches());

		// various queries with 1 mismatch
		String[] queries = new String[] { "GTCACG", "AGCACG", "ATGACG", "ATCTCG", "ATCANG", "ATCACN" };
		//for (int i = 0; i < queries.length; ++i)
		for (String q: queries)
		{
			m = lookup.getSampleId(1, q);
			assertEquals("one", m.getEntry().getSampleId());
			assertEquals(1, m.getMismatches());
		}

		assertNull(lookup.getSampleId(1, "ATCANN")); // 2 mismatches
	}

	@Test
	public void testQueryWithTwoMismatches() throws java.io.IOException, SampleSheet.FormatException
	{
		sheet.loadTable(new StringReader(sampleSheetTwo));
		lookup.load(sheet, 2); // parameter:  support 1 mismatch
		int lane = 1; // lane where the samples in sampleSheetTwo reside

		BarcodeLookup.Match m;
		// exact query
		m = lookup.getSampleId(lane, "ATCACG");
		assertEquals("one", m.getEntry().getSampleId());
		assertEquals(0, m.getMismatches());

		// one mismatch
		m = lookup.getSampleId(lane, "ATCNCG");
		assertEquals("one", m.getEntry().getSampleId());
		assertEquals(1, m.getMismatches());

		// various queries with 2 mismatches
		String[] queries = new String[] { "GGCACG", "AGTACG", "GTGACG", "CTCTCG", "CTCANG", "NTCACN" };
		for (int i = 0; i < queries.length; ++i)
		{
			m = lookup.getSampleId(1, queries[i]);
			assertEquals("one", m.getEntry().getSampleId());
			assertEquals(2, m.getMismatches());
		}
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestBarcodeLookup.class.getName());
	}
}
