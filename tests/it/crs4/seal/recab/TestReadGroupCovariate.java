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

import it.crs4.seal.recab.ReadGroupCovariate;
import it.crs4.seal.recab.TextSamMapping;

import org.junit.*;
import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;

public class TestReadGroupCovariate
{
	private static final String sam = "ERR020229.100000	89	chr6	3558357	37	91M	=	3558678	400	AGCTTCTTTGACTCTCGAATTTTAGCACTAGAAGAAATAGTGAGGATTATATATTTCAGAAGTTCTCACCCAGGATATCAGAACACATTCA	5:CB:CCBCCB>:C@;BBBB??B;?>1@@=C=4ACCAB3A8=CC=C?CBC=CBCCCCCCCCCCCCC@5>?=?CAAB=3=>====5>=AC?C	RG:Z:myrg";
	private static final String samNoRg = "ERR020229.100000	89	chr6	3558357	37	91M	=	3558678	400	AGCTTCTTTGACTCTCGAATTTTAGCACTAGAAGAAATAGTGAGGATTATATATTTCAGAAGTTCTCACCCAGGATATCAGAACACATTCA	5:CB:CCBCCB>:C@;BBBB??B;?>1@@=C=4ACCAB3A8=CC=C?CBC=CBCCCCCCCCCCCCC@5>?=?CAAB=3=>====5>=AC?C";

	private ReadGroupCovariate cov;
	private TextSamMapping mapping;

	@Before
	public void setup()
	{
		cov = new ReadGroupCovariate();
		mapping = new TextSamMapping( new Text(sam) );
	}

	@Test
	public void testSimple()
	{
		cov.applyToMapping(mapping);
		for (int i = 0; i < 91; ++i)
			assertEquals("myrg", cov.getValue(i));
	}

	@Test(expected=RuntimeException.class)
	public void testMissingReadGroup()
	{
		mapping = new TextSamMapping( new Text(samNoRg) );
		cov.applyToMapping(mapping);
	}
}
