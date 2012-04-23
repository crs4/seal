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

import it.crs4.seal.recab.CycleCovariate;
import it.crs4.seal.common.TextSamMapping;

import org.junit.*;
import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;

public class TestCycleCovariate
{
	private static final String sam = "ERR020229.100000	flag	chr6	3558357	37	91M	=	3558678	400	AGCTTCTTTGACTCTCGAATTTTAGCACTAGAAGAAATAGTGAGGATTATATATTTCAGAAGTTCTCACCCAGGATATCAGAACACATTCA	5:CB:CCBCCB>:C@;BBBB??B;?>1@@=C=4ACCAB3A8=CC=C?CBC=CBCCCCCCCCCCCCC@5>?=?CAAB=3=>====5>=AC?C	RG:Z:myrg";

	private CycleCovariate cov;
	private TextSamMapping mapping;
	private ArrayList<Integer> answers;

	@Before
	public void setup()
	{
		cov = new CycleCovariate();
		answers = new ArrayList<Integer>(91);
	}

	@Test
	public void testForwardRead1()
	{
		String record = sam.replace("flag", "67");
		mapping = new TextSamMapping( new Text(record) );
		cov.applyToMapping(mapping);

		for (int i = 1; i <= 91; ++i)
			answers.add(i);

		for (int i = 0; i < 91; ++i)
			assertEquals(String.valueOf(answers.get(i)), cov.getValue(i));
	}

	@Test
	public void testReverseRead1()
	{
		String record = sam.replace("flag", "83");
		mapping = new TextSamMapping(new Text(record));
		cov.applyToMapping(mapping);

		for (int i = 91; i >= 1; --i)
			answers.add(i);

		for (int i = 0; i < 91; ++i)
			assertEquals(String.valueOf(answers.get(i)), cov.getValue(i));
	}

	@Test
	public void testForwardRead2()
	{
		String record = sam.replace("flag", "131");
		mapping = new TextSamMapping( new Text(record) );
		cov.applyToMapping(mapping);

		for (int i = -1; i >= -91; --i)
			answers.add(i);

		for (int i = 0; i < 91; ++i)
			assertEquals(String.valueOf(answers.get(i)), cov.getValue(i));
	}

	@Test
	public void testReverseRead2()
	{
		String record = sam.replace("flag", "83");
		mapping = new TextSamMapping( new Text(record) );
		cov.applyToMapping(mapping);

		for (int i = 91; i >= 1; --i)
			answers.add(i);

		for (int i = 0; i < 91; ++i)
			assertEquals(String.valueOf(answers.get(i)), cov.getValue(i));
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
