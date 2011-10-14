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

import java.nio.ByteBuffer;

import org.junit.*;
import static org.junit.Assert.*;

import it.crs4.seal.recab.BytePacking;

public class TestBytePacking
{
	private ByteBuffer space;

	@Before
	public void setup()
	{
		space = ByteBuffer.allocate(10);
	}

	@Test
	public void testSimple()
	{
		// 0x27 is 0 2 1 3
		BytePacking.unpackByte(space, (byte)0x27, 0, 3);
		assertEquals(0x00020103, space.getInt(0));
	}

	@Test
	public void testFirstPart()
	{
		// 0x27 is 0 2 1 3
		BytePacking.unpackByte(space, (byte)0x27, 0, 1);
		assertEquals((byte)0x00, space.get(0));
		assertEquals((byte)0x02, space.get(1));
	}

	@Test
	public void testLastPart()
	{
		// 0x27 is 0 2 1 3
		BytePacking.unpackByte(space, (byte)0x27, 2, 3);
		assertEquals((byte)0x01, space.get(0));
		assertEquals((byte)0x03, space.get(1));
	}

	@Test
	public void testMiddlePart()
	{
		// 0x27 is 0 2 1 3
		BytePacking.unpackByte(space, (byte)0x27, 1, 2);
		assertEquals((byte)0x02, space.get(0));
		assertEquals((byte)0x01, space.get(1));
	}



}
