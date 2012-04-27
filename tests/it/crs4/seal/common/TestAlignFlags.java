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

package tests.it.crs4.seal.common;

import it.crs4.seal.common.AlignFlags;

import org.junit.*;
import static org.junit.Assert.*;

public class TestAlignFlags
{
	
	@Test
	public void testFlagSet()
	{
		int bitset = 0;
		// set unmapped
		bitset = AlignFlags.Unmapped.set(bitset);
		assertTrue(AlignFlags.Unmapped.is(bitset));
		assertFalse(AlignFlags.Paired.is(bitset));

		bitset = AlignFlags.Paired.set(bitset);
		assertTrue(AlignFlags.Unmapped.is(bitset));
		assertTrue(AlignFlags.Paired.is(bitset));
	}

	@Test
	public void testFlagClear()
	{
		int bitset = -1;

		assertTrue(AlignFlags.Paired.is(bitset));

		bitset = AlignFlags.Paired.clear(bitset);

		assertFalse(AlignFlags.Paired.is(bitset));
		assertTrue(AlignFlags.Unmapped.is(bitset));
	}
}
