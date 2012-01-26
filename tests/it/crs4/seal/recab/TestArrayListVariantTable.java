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


package tests.it.crs4.seal.recab;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.StringReader;
import java.util.Set;

import org.junit.*;
import static org.junit.Assert.*;

import it.crs4.seal.recab.ArrayListVariantTable;
import it.crs4.seal.recab.RodFileVariantReader;
import it.crs4.seal.recab.VcfVariantReader;
import it.crs4.seal.common.FormatException;

public class TestArrayListVariantTable
{
	private ArrayListVariantTable emptyTable;

	@Before
	public void setup()
	{
		// mute the logger for these tests, if we can
		try {
			Log log = LogFactory.getLog(ArrayListVariantTable.class);
			((org.apache.commons.logging.impl.Jdk14Logger)log).getLogger().setLevel(Level.SEVERE);
		}
		catch (ClassCastException e) {
		}

		emptyTable = new ArrayListVariantTable();
	}

	private void loadIntoEmptyTable(String s) throws java.io.IOException, FormatException
	{
		emptyTable.load( new RodFileVariantReader(new StringReader(s)) );
	}

	@Test(expected=FormatException.class)
	public void testLoadEmpty() throws java.io.IOException
	{
		loadIntoEmptyTable("");
	}

	@Test
	public void testDontLoadCdna() throws java.io.IOException
	{
		loadIntoEmptyTable("585	1	14435	14436	rs1045951	0	-	G	G	C/T	cDNA	single	unknown	0	0	unknown	exact	3");
		assertFalse( emptyTable.isVariantLocation("1", 14435));
	}

	@Test
	public void testDontLoadNonSingle() throws java.io.IOException
	{
		loadIntoEmptyTable("585	1	14435	14436	rs1045951	0	-	G	G	C/T	genomic	insertion	unknown	0	0	unknown	exact	3");
		assertFalse( emptyTable.isVariantLocation("1", 14435));
	}

	@Test
	public void testDontLoadNonExact() throws java.io.IOException
	{
		loadIntoEmptyTable("585	1	14435	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	between	3");
		assertFalse( emptyTable.isVariantLocation("1", 14435));
	}

	@Test
	public void testDontLoadIfLongerThan1() throws java.io.IOException
	{
		loadIntoEmptyTable("585	1	14435	14437	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3");
		assertFalse( emptyTable.isVariantLocation("1", 14435));
	}

	@Test
	public void testSimple() throws java.io.IOException
	{
		loadIntoEmptyTable("585	chr1	14435	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3");
		assertTrue( emptyTable.isVariantLocation("chr1", 14435));
	}

	@Test
	public void testMultiple() throws java.io.IOException
	{
		String data = 
"585	1	10259	10260	rs72477211	0	+	C	C	A/G	genomic	single	unknown	0	0	unknown	exact	1\n" +
"585	1	14435	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3";
		loadIntoEmptyTable(data);
		assertTrue( emptyTable.isVariantLocation("1", 14435));
		assertTrue( emptyTable.isVariantLocation("1", 10259));
	}

	@Test(expected=RuntimeException.class)
	public void testPositionTooBig() throws java.io.IOException
	{
		loadIntoEmptyTable("585	1	14435	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3");
		emptyTable.isVariantLocation("1", Integer.MAX_VALUE + 1L);
	}

	@Test(expected=FormatException.class)
	public void testBadCoord() throws java.io.IOException
	{
		loadIntoEmptyTable("585	1	aaa	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3");
	}

	@Test(expected=FormatException.class)
	public void testBadCoord2() throws java.io.IOException
	{
		loadIntoEmptyTable("585	1	14435	aaa	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3");
	}

	@Test(expected=RuntimeException.class)
	public void testQueryTooBig() throws java.io.IOException
	{
		loadIntoEmptyTable("585	1	14435	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3");
		emptyTable.isVariantLocation("1", Integer.MAX_VALUE + 1L);
	}

	@Test(expected=RuntimeException.class)
	public void testCoordTooBig() throws java.io.IOException
	{
		loadIntoEmptyTable("585	1	4500000000	4500000001	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3");
	}

	@Test
	public void testSize() throws java.io.IOException
	{
		String s1 = "585	1	13023	13024	rs2462498	0	-	G	G	C/G	genomic	single	unknown	0	0	unknown	exact	3\n" +
		            "585	1	13078	13079	rs71249498	0	+	C	C	C/G	genomic	single	unknown	0	0	unknown	exact	3\n";
		String s2 = "585	2	13107	13108	rs71234146	0	+	G	G	A/G	genomic	single	unknown	0	0	unknown	exact	3\n" +
		            "585	2	13109	13110	rs71267774	0	+	G	G	A/G	genomic	single	unknown	0	0	unknown	exact	3\n";
		String s3 = "585	3	13115	13116	rs62635286	0	+	T	T	G/T	genomic	single	unknown	0	0	unknown	exact	3\n" +
		            "585	3	13117	13118	rs62028691	0	-	A	A	C/T	genomic	single	unknown	0	0	unknown	exact	3\n" +
                "585	3	13137	13138	rs12239753	0	+	T	T	C/T	genomic	single	unknown	0	0	unknown	exact	3\n";

		assertEquals(0, emptyTable.size());
		loadIntoEmptyTable(s1);
		assertEquals(2, emptyTable.size());

		loadIntoEmptyTable(s1 + s2);
		assertEquals(4, emptyTable.size());

		loadIntoEmptyTable(s1 + s2 + s3);
		assertEquals(7, emptyTable.size());
	}

	@Test
	public void testGetContigs() throws java.io.IOException
	{
		String data = 
"585	1	10259	10260	rs72477211	0	+	C	C	A/G	genomic	single	unknown	0	0	unknown	exact	1\n" +
"585	3	14435	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3\n" +
"585	5	14435	14436	rs1045951	0	-	G	G	C/T	genomic	single	unknown	0	0	unknown	exact	3";

		loadIntoEmptyTable(data);
		Set<String> contigs = emptyTable.getContigs();
		assertEquals(3, contigs.size());
		assertTrue(contigs.contains("1"));
		assertTrue(contigs.contains("3"));
		assertTrue(contigs.contains("5"));
	}

	@Test
	public void testLoadMultiBaseVariant() throws java.io.IOException
	{
		String vcfDel = 
		  "##fileformat=VCFv4.1\n"	+
		  "#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n"	+
		  "1	1	rs112766696	AC	A	.	PASS	VC=INDEL\n";
		VcfVariantReader reader = new VcfVariantReader( new StringReader(vcfDel) );
		reader.setReadSnpsOnly(false);

		emptyTable.load(reader);
		assertTrue(emptyTable.isVariantLocation("1", 1));
		assertTrue(emptyTable.isVariantLocation("1", 2));
	}

	@Test
	public void testOverlappingVariants() throws java.io.IOException
	{
		String vcf = 
		  "##fileformat=VCFv4.1\n"	+
		  "#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n"	+
		  "1	1	rs112766696	ACG	A	.	PASS	VC=INDEL\n" +
		  "1	2	rs112766696	C	A	.	PASS	VC=SNP\n";
		VcfVariantReader reader = new VcfVariantReader( new StringReader(vcf) );
		reader.setReadSnpsOnly(false);

		emptyTable.load(reader);
		assertTrue(emptyTable.isVariantLocation("1", 1));
		assertTrue(emptyTable.isVariantLocation("1", 2));
		assertTrue(emptyTable.isVariantLocation("1", 3));
		assertEquals(3, emptyTable.size());
	}

	@Test
	public void testUnsortedVariants() throws java.io.IOException
	{
		String vcf = 
		  "##fileformat=VCFv4.1\n"	+
		  "#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n"	+
		  "1	1	rs112766696	ACG	A	.	PASS	VC=INDEL\n" +
		  "1	6	rs112766696	C	A	.	PASS	VC=SNP\n" +
		  "1	5	rs112766696	C	A	.	PASS	VC=SNP\n";
		VcfVariantReader reader = new VcfVariantReader( new StringReader(vcf) );
		reader.setReadSnpsOnly(false);

		emptyTable.load(reader);
		assertTrue(emptyTable.isVariantLocation("1", 1));
		assertTrue(emptyTable.isVariantLocation("1", 5));
		assertTrue(emptyTable.isVariantLocation("1", 6));
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestArrayListVariantTable.class.getName());
	}
}
