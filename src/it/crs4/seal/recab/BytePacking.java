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

package it.crs4.seal.recab;

import java.nio.ByteBuffer;

public class BytePacking
{
	/**
	 * Unpack a byte.
	 * @param start First pair of bits to unpack (most signif pair is 0).
	 * @param end Last pair of bits to unpack (least signif pair is 3).
	 * @return dest
	 */
	public static ByteBuffer unpackByte(ByteBuffer dest, byte packed, int start, int end)
	{
		for (int i = 6 - 2*start; i >= 6 - 2*end; i -= 2)
		{
			// for each position after the offset, we shift the packed byte
			// so that the position we want is at the least-significant 2 bits,
			// and then get the bits by and-ing with 0x03.
			dest.put( (byte)((packed >>> i) & 0x03) );
		}
		return dest;
	}

	/* =============== Untested ===============
	 * Pack some bytes and write the packed version to dest.
	 * @param dest ByteBuffer where to write the packed bytes.
	 * @param bytes Bytes to pack.
	 * @param start Index of first byte from bytes to pack.
	 * @param len Number of bytes to pack, starting from start.
	 *
	public static void packBytes(ByteBuffer dest, ByteBuffer bytes, int start, int len)
	{
		int packed = 0;
		int numPacked = 0;
		int pos = start;

		while (pos < start + len)
		{
			packed = (packed << 2) | (bytes.get(pos) & 0x03);
			++numPacked;
			if (numPacked == 4)
			{
				dest.putInt(packed);
				numPacked = 0;
				packed = 0;
			}
		}
		if (numPacked > 0)
		{
			while (numPacked > 0)
			{
				dest.put( (byte)((packed >>> (2*numPacked - 2)) & 0x3) );
				--numPacked;
			}
		}
	}
	*/
}

