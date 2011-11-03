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

import it.crs4.seal.recab.DinucCovariate;
import it.crs4.seal.recab.TextSamMapping;

import org.junit.*;
import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;

public class TestDinucCovariate
{
	private static final String dna = "AGCTTCTTTGACTCTNNCGAA";
	private static final String revComplement = "TTCGNNAGAGTCAAAGAAGCT";
	private static final String sam = "id	flag	chr6	1	37	22M	=	41	60	" + dna + "	5:CB:CCBCCB>:C@;BBBBB	RG:Z:myrg";

	private DinucCovariate cov;
	private TextSamMapping mapping;
	private ArrayList<String> answers;

	@Before
	public void setup()
	{
		cov = new DinucCovariate();
		answers = new ArrayList<String>(dna.length());
	}

	@Test
	public void testForwardRead()
	{
		String record = sam.replace("flag", "67"); // forward, read 1
		mapping = new TextSamMapping( new Text(record) );
		cov.applyToMapping(mapping);

		answers.add("N" + dna.substring(0,1));
		for (int i = 0; i < dna.length() - 1; ++i)
			answers.add(dna.substring(i, i+2));

		for (int i = 0; i < dna.length(); ++i)
			assertEquals(answers.get(i), cov.getValue(i));
	}

	@Test
	public void testReverseRead()
	{
		String record = sam.replace("flag", "83"); // reverse strand, read 1
		mapping = new TextSamMapping(new Text(record));
		cov.applyToMapping(mapping);

		answers.add("N" + revComplement.substring(0,1));
		for (int i = 0; i < dna.length() - 1; ++i)
			answers.add(revComplement.substring(i, i+2));

		for (int i = 0; i < dna.length(); ++i)
			assertEquals(answers.get(i), cov.getValue(i));
	}

	@Test(expected=RuntimeException.class)
	public void testDidntCallApplyToMapping()
	{
		cov.getValue(1);
	}

	@Test(expected=IndexOutOfBoundsException.class)
	public void testMissingReadGroup()
	{
		String record = sam.replace("flag", "67");
		mapping = new TextSamMapping( new Text(record) );
		cov.applyToMapping(mapping);
		cov.getValue(100);
	}
}
