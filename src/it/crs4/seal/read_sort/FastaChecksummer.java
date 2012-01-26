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

package it.crs4.seal.read_sort;
import it.crs4.seal.common.FormatException;
import it.crs4.seal.common.UnknownItemException;

import java.io.BufferedReader;
import java.io.Reader;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.*;

import org.apache.commons.codec.binary.Hex;

public class FastaChecksummer implements Iterable<FastaChecksummer.ChecksumEntry>
{
	public static class ChecksumEntry
	{
		private String name;
		private String checksum;

		public ChecksumEntry(String name, String checksum)
		{
			this.name = name;
			this.checksum = checksum;
		}

		public String getName() { return name; }
		public String getChecksum() { return checksum; }
	}

	private BufferedReader input;
	private HashMap<String, ChecksumEntry> contigHashes;

	private final String checksumAlgorithm = "MD5";
	private static final Pattern ContigNamePattern = Pattern.compile(">\\s*(\\S+).*");

	public void setInput(Reader stream)
	{
		input = new BufferedReader(stream, 4*1024*1024);
		contigHashes = null;
	}

	public void calculate() throws FormatException, java.io.IOException
	{
		if (input == null)
			throw new IllegalStateException("FastaChecksummer input not set");

 		contigHashes = new HashMap<String, ChecksumEntry>();

		String currentContig = null;
		java.security.MessageDigest hasher = null;
	 
		try {
			hasher = java.security.MessageDigest.getInstance(checksumAlgorithm); 
		}
		catch (java.security.NoSuchAlgorithmException e) {
			throw new RuntimeException("Unexpected NoSuchAlgorithmException when asking for " + checksumAlgorithm + " algorithm");
		}

		String line = input.readLine();
		if (line == null)
			throw new FormatException("empty Fasta");

		try
		{
			while (line != null)
			{
				if (line.startsWith(">")) // start a new contig
				{
					if (currentContig != null)
					{
						// Hadoop 0.20,2 ships with Apache commons version 1.3, which doesn't
						// have encodeHexString
						String cs = new String(Hex.encodeHex(hasher.digest()));
						contigHashes.put(currentContig, new ChecksumEntry(currentContig, cs));
					}

					Matcher m = ContigNamePattern.matcher(line);
					if (m.matches())
					{
						currentContig = m.group(1);
						hasher.reset();
					}
					else
						throw new FormatException("Unexpected contig name format: " + line);
				}
				else
				{
					if (currentContig == null)
						throw new FormatException("Sequence outside any fasta record (header is missing). Line: " + line);
					else
						hasher.update( line.getBytes("US-ASCII") );
				}

				line = input.readLine();
			}

			if (currentContig != null) // store the last contig
			{
				String cs = new String(Hex.encodeHex(hasher.digest()));
				contigHashes.put(currentContig, new ChecksumEntry(currentContig, cs));
			}
		}
		catch (java.io.UnsupportedEncodingException e) {
			throw new RuntimeException("Unexpected UnsupportedEncodingException! Line: " + line);
		}
	}

	public Iterator<ChecksumEntry> iterator()
	{
		if (contigHashes == null)
			throw new IllegalStateException("Checksums not calculated");

		return contigHashes.values().iterator();
	}

	public boolean hasChecksum(String contigName)
	{
		if (contigHashes == null)
			throw new IllegalStateException("Checksums not calculated");

		return contigHashes.containsKey(contigName);
	}

	public String getChecksum(String contigName) throws UnknownItemException
	{
		if (contigHashes == null)
			throw new IllegalStateException("Checksums not calculated");

		ChecksumEntry entry = contigHashes.get(contigName);
		if (entry == null)
			throw new UnknownItemException("Unknown contig name " + contigName);
		else
			return entry.getChecksum();
	}
}
