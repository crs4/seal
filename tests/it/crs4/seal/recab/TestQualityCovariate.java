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

import it.crs4.seal.recab.QualityCovariate;
import it.crs4.seal.recab.TextSamMapping;

import org.junit.*;
import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;

import java.util.ArrayList;

public class TestQualityCovariate
{
	private static final String quality = "ABCDEFGHIJKLMNOPQRSTU";
	private static final String sam = "id	67	chr6	1	37	21M	=	41	60	AGCTTCTTTGACTCTNNCGAA	" + quality + "	RG:Z:myrg";

	private QualityCovariate cov;
	private TextSamMapping mapping;
	private ArrayList<String> answers;

	@Before
	public void setup()
	{
		cov = new QualityCovariate();
		answers = new ArrayList<String>(quality.length());
	}

	@Test
	public void testSimple()
	{
		mapping = new TextSamMapping( new Text(sam) );
		cov.applyToMapping(mapping);

		for (int i = 0; i < quality.length(); ++i)
			answers.add( String.valueOf((byte)quality.charAt(i) - 33) ); // quality is assumed to be encoded in Phred+33 Sanger format

		for (int i = 0; i < quality.length(); ++i)
			assertEquals(answers.get(i), cov.getValue(i));
	}

	@Test(expected=RuntimeException.class)
	public void testDidntCallApplyToMapping()
	{
		cov.getValue(1);
	}

	@Test(expected=IndexOutOfBoundsException.class)
	public void testOutOfBounds()
	{
		String record = sam.replace("flag", "67");
		mapping = new TextSamMapping( new Text(record) );
		cov.applyToMapping(mapping);
		cov.getValue(100);
	}
}
