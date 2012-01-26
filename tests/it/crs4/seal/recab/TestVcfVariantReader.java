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

import org.junit.*;
import static org.junit.Assert.*;

import java.io.StringReader;
import java.io.IOException;

import it.crs4.seal.common.FormatException;
import it.crs4.seal.recab.VcfVariantReader;
import it.crs4.seal.recab.VariantRegion;

public class TestVcfVariantReader
{

	private String vcfSample =
		"##fileformat=VCFv4.1\n"	+
		"##FILTER=<ID=NC,Description=\"Inconsistent	Genotype	Submission	For	At	Least	One	Sample\">	_level=INFO	log_to_file=null	help=false	out=org.broadinstitute.sting.gatk.io.stubs.VCFWriterStub	NO_HEADER=org.broadinstitute.sting.gatk.io.stubs.VCFWriterStub	sites_only=org.broadinstitute.sting.gatk.io.stubs.VCFWriterStub\n"	+
		"##dbSNP_BUILD_ID=132\n"	+
		"##source=dbSNP\n"	+
		"#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n"	+
		"1	10327	rs112750067	T	C	.	PASS	VC=SNP;VP=050000000005000000000100;WGT=1;dbSNPBuildID=132\n"	+
		"1	10433	rs56289060	A	AC	.	PASS	VC=INDEL\n"	+
		"1	10439	rs112766696	AC	A	.	PASS	VC=INDEL\n"	+
		"2	10440	rs112155239	C	A	.	PASS	VC=SNP";

	private String vcfNoColumnHeading =
		"##fileformat=VCFv4.1\n"	+
		"2	10440	rs112155239	C	A	.	PASS	VC=SNP";

	private String vcfNoRecords =
		"##fileformat=VCFv4.1\n" +
		"#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n";

	private String vcfMissingMagic =
		"#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n"	+
		"1	10327	rs112750067	T	C	.	PASS	VC=SNP";

	private String vcfNumberFormatError =
		"##fileformat=VCFv4.1"	+
		"#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n"	+
		"1	bla	rs112750067	T	C	.	PASS	VC=SNP";


	private String vcfDel =
		"##fileformat=VCFv4.1\n"	+
		"#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n"	+
		"1	10439	rs112766696	AC	A	.	PASS	VC=INDEL\n";

	private VcfVariantReader snpReader;
	private VariantRegion variant;

	@Before
	public void setup()
	{
		variant = new VariantRegion();
	}

	@Test
	public void testDefault() throws IOException
	{
		snpReader = new VcfVariantReader( new StringReader(vcfSample) );
		assertTrue(snpReader.getReadSnpsOnly());
	}

	@Test
	public void testFilter() throws IOException
	{
		snpReader = new VcfVariantReader( new StringReader(vcfSample) );
		int count = 0;

		while (snpReader.nextEntry(variant))
			++count;
		assertEquals(2, count);
	}

	@Test
	public void testDontFilter() throws IOException
	{
		snpReader = new VcfVariantReader( new StringReader(vcfSample) );
		snpReader.setReadSnpsOnly(false);
		int count = 0;

		while (snpReader.nextEntry(variant))
			++count;
		assertEquals(4, count);
	}

	@Test
	public void testReading() throws IOException
	{
		snpReader = new VcfVariantReader( new StringReader(vcfSample) );

		assertTrue(snpReader.nextEntry(variant));
		assertEquals("1", variant.getContigName());
		assertEquals(10327, variant.getPosition());
		assertEquals(1, variant.getLength());

		assertTrue(snpReader.nextEntry(variant));
		assertEquals("2", variant.getContigName());
		assertEquals(10440, variant.getPosition());
		assertEquals(1, variant.getLength());
	}

	@Test(expected=FormatException.class)
	public void testEmpty() throws IOException
	{
		snpReader = new VcfVariantReader( new StringReader("") );
	}

	@Test(expected=FormatException.class)
	public void testNumberFormatError() throws IOException
	{
		snpReader = new VcfVariantReader( new StringReader(vcfNumberFormatError) );
		snpReader.nextEntry(variant);
	}

	@Test(expected=FormatException.class)
	public void testNoColumnHeadings() throws IOException
	{
		snpReader = new VcfVariantReader( new StringReader(vcfNoColumnHeading) );
	}

	@Test(expected=FormatException.class)
	public void testNoRecords() throws IOException
	{
		snpReader = new VcfVariantReader( new StringReader(vcfNoRecords) );
		snpReader.nextEntry(variant);
	}

	@Test(expected=FormatException.class)
	public void testMissingMagic() throws IOException
	{
		snpReader = new VcfVariantReader( new StringReader(vcfMissingMagic) );
	}

	@Test
	public void testMultiBaseVariant() throws IOException
	{
		snpReader = new VcfVariantReader( new StringReader(vcfDel) );
		snpReader.setReadSnpsOnly(false);

		assertTrue(snpReader.nextEntry(variant));
		assertEquals("1", variant.getContigName());
		assertEquals(10439, variant.getPosition());
		assertEquals(2, variant.getLength());
	}
}
