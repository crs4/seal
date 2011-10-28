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

package tests.it.crs4.seal.recab;

import org.junit.*;
import static org.junit.Assert.*;

import java.io.StringReader;
import java.io.IOException;

import it.crs4.seal.common.FormatException;
import it.crs4.seal.recab.VcfSnpReader;
import it.crs4.seal.recab.SnpDef;

public class TestVcfSnpReader
{

	private String vcfSample =
		"##fileformat=VCFv4.1\n"	+
		"##FILTER=<ID=NC,Description=\"Inconsistent	Genotype	Submission	For	At	Least	One	Sample\">	_level=INFO	log_to_file=null	help=false	out=org.broadinstitute.sting.gatk.io.stubs.VCFWriterStub	NO_HEADER=org.broadinstitute.sting.gatk.io.stubs.VCFWriterStub	sites_only=org.broadinstitute.sting.gatk.io.stubs.VCFWriterStub\n"	+
		"##dbSNP_BUILD_ID=132\n"	+
		"##source=dbSNP\n"	+
		"#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n"	+
		"1	10327	rs112750067	T	C	.	PASS	ASP;RSPOS=10327;SAO=0;SCS=0;SSR=0;VC=SNP;VP=050000000005000000000100;WGT=1;dbSNPBuildID=132\n"	+
		"1	10433	rs56289060	A	AC	.	PASS	ASP;RSPOS=10433;SAO=0;SCS=0;SSR=0;VC=INDEL;VP=050000000005000000000200;WGT=1;dbSNPBuildID=129\n"	+
		"1	10439	rs112766696	AC	A	.	PASS	ASP;G5;G5A;GNO;OTH;RSPOS=10440;SAO=0;SCS=0;SLO;SSR=0;VC=INDEL;VP=050100000015030100000200;WGT=1;dbSNPBuildID=132\n"	+
		"2	10440	rs112155239	C	A	.	PASS	ASP;OTH;RSPOS=10440;SAO=0;SCS=0;SSR=0;VC=SNP;VP=050000000015000000000100;WGT=1;dbSNPBuildID=132";

	private String vcfNoColumnHeading =
		"##fileformat=VCFv4.1\n"	+
		"2	10440	rs112155239	C	A	.	PASS	ASP;OTH;RSPOS=10440;SAO=0;SCS=0;SSR=0;VC=SNP;VP=050000000015000000000100;WGT=1;dbSNPBuildID=132";

	private String vcfNoRecords =
		"##fileformat=VCFv4.1\n" +
		"#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n";

	private String vcfMissingMagic =
		"#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n"	+
		"1	10327	rs112750067	T	C	.	PASS	ASP;RSPOS=10327;SAO=0;SCS=0;SSR=0;VC=SNP;VP=050000000005000000000100;WGT=1;dbSNPBuildID=132";
	
	private String vcfNumberFormatError = 
		"##fileformat=VCFv4.1"	+
		"#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n"	+
		"1	bla	rs112750067	T	C	.	PASS	ASP;RSPOS=10327;SAO=0;SCS=0;SSR=0;VC=SNP;VP=050000000005000000000100;WGT=1;dbSNPBuildID=132";

	private VcfSnpReader snpReader;
	private SnpDef snp;

	@Before
	public void setup()
	{
		snp = new SnpDef();
	}

	@Test
	public void testDefault() throws IOException
	{
		snpReader = new VcfSnpReader( new StringReader(vcfSample) );
		assertTrue(snpReader.getReadSnpsOnly());
	}

	@Test
	public void testFilter() throws IOException
	{
		snpReader = new VcfSnpReader( new StringReader(vcfSample) );
		int count = 0;

		while (snpReader.nextEntry(snp))
			++count;
		assertEquals(2, count);
	}

	@Test
	public void testDontFilter() throws IOException
	{
		snpReader = new VcfSnpReader( new StringReader(vcfSample) );
		snpReader.setReadSnpsOnly(false);
		int count = 0;

		while (snpReader.nextEntry(snp))
			++count;
		assertEquals(4, count);
	}

	@Test
	public void testReading() throws IOException
	{
		snpReader = new VcfSnpReader( new StringReader(vcfSample) );

		assertTrue(snpReader.nextEntry(snp));
		assertEquals("1", snp.getContigName());
		assertEquals(10327, snp.getPosition());

		assertTrue(snpReader.nextEntry(snp));
		assertEquals("2", snp.getContigName());
		assertEquals(10440, snp.getPosition());
	}

	@Test(expected=FormatException.class)
	public void testEmpty() throws IOException
	{
		snpReader = new VcfSnpReader( new StringReader("") );
	}

	@Test(expected=FormatException.class)
	public void testNumberFormatError() throws IOException
	{
		snpReader = new VcfSnpReader( new StringReader(vcfNumberFormatError) );
		snpReader.nextEntry(snp);
	}

	@Test(expected=FormatException.class)
	public void testNoColumnHeadings() throws IOException
	{
		snpReader = new VcfSnpReader( new StringReader(vcfNoColumnHeading) );
	}

	@Test(expected=FormatException.class)
	public void testNoRecords() throws IOException
	{
		snpReader = new VcfSnpReader( new StringReader(vcfNoRecords) );
		snpReader.nextEntry(snp);
	}

	@Test(expected=FormatException.class)
	public void testMissingMagic() throws IOException
	{
		snpReader = new VcfSnpReader( new StringReader(vcfMissingMagic) );
	}
}
