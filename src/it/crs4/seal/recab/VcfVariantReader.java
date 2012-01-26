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

package it.crs4.seal.recab;

import it.crs4.seal.common.FormatException;
import it.crs4.seal.common.CutString;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.regex.*;
import java.nio.CharBuffer;

/**
 * Read SNPs from a Rod file.
 */
public class VcfVariantReader implements VariantReader
{
	protected LineNumberReader reader;
	protected CutString cutter;
	protected static final Pattern VC_SNP = Pattern.compile("\\bVC=SNP\\b");
	protected boolean snpOnly = true;
	private int firstDataLine;
	protected Matcher snpMatcher;

	public static final String MAGIC = "##fileformat=VCFv4";

	/* Sample format:
	 * http://www.1000genomes.org/wiki/Analysis/Variant%20Call%20Format/vcf-variant-call-format-version-41
	 *
	 * file starts with:
##fileformat=VCFv4.1

	 * Then you have meta info lines.  They start with: "##"
	 * Then you have a tab-delimited column heading line; e.g.
#CHROM  POS     ID      REF     ALT     QUAL    FILTER  INFO
	 * This line names the 8 mandatory columns.  If genotype data is present in the file,
	 * these are followed by a FORMAT column header, then an arbitrary number of sample IDs.
	 *
	 * At this point the data lines start.  There are 8 fixed fields per record. All data
	 * lines are tab-delimited. In all cases, missing values are specified with a dot (”.”). Fixed fields are:
	 * 1. CHROM chromosome: an identifier from the reference genome or an angle-bracketed ID String ("<ID>") pointing to a contig in the assembly file
	 * 2. POS position: The reference position, with the 1st base having position 1. Positions are sorted numerically, in increasing order, within each reference sequence CHROM.
	 * 3. ID semi-colon separated list of unique identifiers where available. If this is a dbSNP variant it is encouraged to use the rs number(s).
	 * 4. REF reference base(s): Each base must be one of A,C,G,T,N (case insensitive). Multiple bases are permitted. The value in the POS field refers to the position of the first base in the String. For InDels or larger structural variants, the reference String must include the base before the event (which must be reflected in the POS field).
	 * 5. ALT comma separated list of alternate non-reference alleles called on at least one of the samples.
	 * 6. QUAL phred-scaled quality score for the assertion made in ALT. (Numeric)
	 * 7. FILTER : PASS if this position has passed all filters, i.e. a call is made at this position. If filters have not been applied, then this field should be set to the missing value.
	 * 8. INFO additional information: INFO fields are encoded as a semicolon-separated series of short keys with optional values in the format: <key>=<data>[,data].
	 *
	 * Example:
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
1	10327	rs112750067	T	C	.	PASS	ASP;RSPOS=10327;SAO=0;SCS=0;SSR=0;VC=SNP;VP=050000000005000000000100;WGT=1;dbSNPBuildID=132
	*/

	// required columns (0-based indices):    Cut index
	// 1: chr                                     0
	// 2: pos                                     1
	// 8: info                                    7

	public VcfVariantReader(Reader in) throws IOException
	{
		CharBuffer buf = CharBuffer.allocate(MAGIC.length());
		int charsRead = in.read(buf);
		if (charsRead <= 0)
			throw new FormatException("Empty VCF file");
		else if (charsRead < MAGIC.length())
			throw new IOException("Couldn't read magic sequence from VCF file");

		buf.position(0);
		if (!MAGIC.equals(buf.toString()))
			throw new FormatException("Did not detect magic sequence " + MAGIC + " at the start of VCF file.  Did you maybe specify the wrong variants file type?");

		reader = new LineNumberReader(in);
		reader.readLine(); // read rest of the magic line and throw it away

		// see the sample format above to understand the cutting indices selected.
		cutter = new CutString("\t", 0, 1, 3, 7);
		snpMatcher = VC_SNP.matcher("");

		readHeading();
		firstDataLine = reader.getLineNumber();
	}

	/**
	 * Set whether to only read VC=SNP entries.
	 * If true (the default), only SNPs will be retrieved while other variations
	 * will be skipped.
	 * If false, all the variations will be retrieved.
	 */
	public void setReadSnpsOnly(boolean v) { snpOnly = v; }

	public boolean getReadSnpsOnly() { return snpOnly; }

	/**
	 * Skim over the heading lines.
	 * We don't actually do anything with these except verify that the column
	 * headings line exists.
	 * After calling this method, the reader should be positioned at the start
	 * of the first data row.
	 */
	private void readHeading() throws FormatException, IOException
	{
		String line = null;

		do {
			line = reader.readLine();
		}
		while (line != null && line.startsWith("##"));

		if (line == null) // premature EOF
			throw new FormatException("Unexpected end of VCF file before column headings");

		if (line.length() >= 2)
		{
			if ( !(line.charAt(0) == '#' && line.charAt(1) != '#') ) // if it's not the column heading line
				throw new FormatException("Missing column heading line.  Expected it a line " + reader.getLineNumber() + " but found: " + line);
			// else we're ok.  Headings are read and we expect the next line to be data.
		}
		else
			throw new FormatException("VCF format error at line " + reader.getLineNumber() + ".  Expected column headings.");
	}

	/**
	 * Read next entry from file and write it to dest.
	 * @return True if a record was read.  False otherwise, indicating we have reached the end of the file.
	 */
	public boolean nextEntry(VariantRegion dest) throws FormatException, IOException
	{
		boolean gotRecord = false;
		String line;

		try {
			do {
	 			line = reader.readLine();
				if (line != null)
				{
					cutter.loadRecord(line);
					String info = cutter.getField(3);

					if (!snpOnly || snpMatcher.reset(info).find())
					{
						String chr = cutter.getField(0);
						long pos = Long.parseLong(cutter.getField(1));
						// XXX:  safety check.  If this fails we have to move up to long values
						if (pos > Integer.MAX_VALUE)
							throw new RuntimeException("vcf position bigger than expected!  File a bug!!");

						dest.setContigName(chr);
						// XXX: remove the cast if we move up to long values
						dest.setPosition((int)pos);
						dest.setLength(cutter.getField(2).length());
						gotRecord = true;
					} // if (string matches)
				}
				else // line is null
				{
					if (reader.getLineNumber() == firstDataLine)
						throw new FormatException("empty Variant table");
				}
			} while (line != null && !gotRecord);
		}
		catch (CutString.FormatException e) {
			throw new FormatException("Invalid table format at line " + reader.getLineNumber() + ": " + e);
		}
		catch (NumberFormatException e) {
			throw new FormatException("Invalid coordinate at line " + reader.getLineNumber() + ": " + e);
		}

		return gotRecord;
	}
}
