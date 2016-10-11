// Copyright (C) 2011-2016 CRS4.
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

package tests.it.crs4.seal.common;

import it.crs4.seal.common.AlignOp;
import it.crs4.seal.common.FormatException;

import java.util.List;
import java.util.ArrayList;

import org.junit.*;
import static org.junit.Assert.*;

public class TestAlignOp
{
	@Test(expected=FormatException.class)
	public void testInvalidCigar1()
	{
		AlignOp.scanCigar("pippo");
	}

	@Test(expected=FormatException.class)
	public void testInvalidCigar2()
	{
		AlignOp.scanCigar("10");
	}

	@Test(expected=FormatException.class)
	public void testInvalidCigar3()
	{
		AlignOp.scanCigar("M");
	}

	@Test
	public void testSimpleCigar()
	{
		List<AlignOp> alignment = AlignOp.scanCigar("91M");
		assertEquals(1, alignment.size());
		assertEquals(new AlignOp(AlignOp.Type.Match, 91), alignment.get(0));
	}

	@Test
	public void testMIMCigar()
	{
		List<AlignOp> alignment = AlignOp.scanCigar("5M4I8M");
		assertEquals(3, alignment.size());
		assertEquals(new AlignOp(AlignOp.Type.Match, 5), alignment.get(0));
		assertEquals(new AlignOp(AlignOp.Type.Insert, 4), alignment.get(1));
		assertEquals(new AlignOp(AlignOp.Type.Match, 8), alignment.get(2));
	}

	@Test
	public void testMDMCigar()
	{
		List<AlignOp> alignment = AlignOp.scanCigar("5M2D3M");
		assertEquals(3, alignment.size());
		assertEquals(new AlignOp(AlignOp.Type.Match, 5), alignment.get(0));
		assertEquals(new AlignOp(AlignOp.Type.Delete, 2), alignment.get(1));
		assertEquals(new AlignOp(AlignOp.Type.Match, 3), alignment.get(2));
	}

	@Test
	public void testSMCigar()
	{
		List<AlignOp> alignment = AlignOp.scanCigar("4S8M");
		assertEquals(2, alignment.size());
		assertEquals(new AlignOp(AlignOp.Type.SoftClip, 4), alignment.get(0));
		assertEquals(new AlignOp(AlignOp.Type.Match, 8), alignment.get(1));
	}

	@Test
	public void testCigarStr()
	{
		List<AlignOp> alignment = new ArrayList<AlignOp>(2);
		alignment.add(new AlignOp(AlignOp.Type.SoftClip, 4));
		alignment.add(new AlignOp(AlignOp.Type.Match, 8));
		assertEquals("4S8M", AlignOp.cigarStr(alignment));
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestAlignOp.class.getName());
	}
}
