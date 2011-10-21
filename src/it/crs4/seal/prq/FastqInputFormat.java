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

package it.crs4.seal.prq;

import it.crs4.seal.common.LineReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.EOFException;

public class FastqInputFormat extends FileInputFormat<Text,SequencedFragment>
{
	public static class FastqRecordReader implements RecordReader<Text,SequencedFragment>
	{
		/*
		 * fastq format:
		 * <fastq>	:=	<block>+
		 * <block>	:=	@<seqname>\n<seq>\n+[<seqname>]\n<qual>\n
		 * <seqname>	:=	[A-Za-z0-9_.:-]+
		 * <seq>	:=	[A-Za-z\n\.~]+
		 * <qual>	:=	[!-~\n]+
		 *
		 * LP: this format is broken, no?  You can have multi-line sequence and quality strings,
		 * and the quality encoding includes '@' in its valid character range.  So how should one
		 * distinguish between \n@ as a record delimiter and and \n@ as part of a multi-line
		 * quality string?
		 *
		 * For now I'm going to assume single-line sequences.  This works for our sequencing
		 * application.  We'll see if someone complains in other applications.
		 */
			
		// start:  first valid data index
		private long start;
		// end:  first index value beyond the data block, i.e. data is in range [start,end)
		private long end;
		// pos: current position in file
		private long pos;
		private LineReader lineReader;
		private FSDataInputStream inputStream;

		private Text buffer = new Text();

		// How long can a read get?
		private static final int MAX_LINE_LENGTH = 10000;

		public FastqRecordReader(Configuration conf, FileSplit split) throws IOException
		{
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();

			FileSystem fs = file.getFileSystem(conf);
			inputStream = fs.open(split.getPath());
			positionAtFirstRecord(inputStream);
			lineReader = new LineReader(inputStream);
		}

		/*
		 * Position the input stream at the start of the first record.
		 */
		private void positionAtFirstRecord(FSDataInputStream stream) throws IOException
		{
			if (start > 0)
			{
				// Advance to the start of the first record
				// We use a temporary LineReader to read lines until we find the
				// position of the right one.  We then seek the file to that position.
				stream.seek(start);
				LineReader reader = new LineReader(stream);

				int bytesRead = 0;
				do
			 	{
					bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));
					if (bytesRead > 0 && buffer.getBytes()[0] != '@')
						start += bytesRead;
					else
						break;
				} while (bytesRead > 0);

				stream.seek(start);
			}
			// else
			//	if start == 0 we presume it starts with a valid fastq record
			pos = start;
		}
		
		/**
		 * Close this RecordReader to future operations.
		 */
		public void close() throws IOException
		{
			inputStream.close();
		}

		/**
		 * Create an object of the appropriate type to be used as a key.
		 */
		public Text createKey() 
		{
			return new Text();
		}

		/**
		 * Create an object of the appropriate type to be used as a value.
		 */
		public SequencedFragment createValue() 
		{
			return new SequencedFragment();
		}

		/**
		 * Returns the current position in the input.
		 */
		public long getPos() { return pos; }

		/**
		 * How much of the input has the RecordReader consumed i.e.
		 */
		public float getProgress() 
		{
			if (start == end)
				return 1.0f;
			else
				return Math.min(1.0f, (pos - start) / (float)(end - start));
		}

		/**
		 * Reads the next key/value pair from the input for processing.
		 */
		public boolean next(Text key, SequencedFragment value) throws IOException
		{
			key.clear();

			// ID line
			long skipped = lineReader.skip(1); // skip @
			pos += skipped;
			if (skipped == 0)
				return false; // EOF

			try
			{
				// ID
				readLineInto(key);
				// sequence
				readLineInto(value.getSequence());
				readLineInto(buffer);
				if (! buffer.toString().equals("+"))
					throw new RuntimeException("unexpected fastq line separating sequence and quality: " + buffer + ". \nSequence ID: " + key);
				readLineInto(value.getQuality());
			}
			catch (EOFException e) {
				throw new RuntimeException("unexpected end of file in fastq record " + key.toString());
			}
			return true;
		}

		private int readLineInto(Text dest) throws EOFException, IOException
		{
			int bytesRead = lineReader.readLine(dest, (int)Math.min(MAX_LINE_LENGTH, end - pos));
			if (bytesRead <= 0)
				throw new EOFException();
			pos += bytesRead;
			return bytesRead;
		}
	}

	public RecordReader<Text, SequencedFragment> getRecordReader(
	                                        InputSplit genericSplit, JobConf job,
	                                        Reporter reporter) throws IOException 
	{
		reporter.setStatus(genericSplit.toString());
		return new FastqRecordReader(job, (FileSplit)genericSplit); // cast as per example in TextInputFormat
	}
}
