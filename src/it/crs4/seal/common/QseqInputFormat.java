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

package it.crs4.seal.common;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

/**
 * Reads the Illumina qseq sequence format.
 * Key: instrument, run number, lane, tile, xpos, ypos, read number, delimited by ':' characters.
 * Value:  a SequencedFragment object representing the entry.
 */
public class QseqInputFormat extends FileInputFormat<Text,SequencedFragment>
{
	public static class QseqRecordReader extends RecordReader<Text,SequencedFragment>
	{
		/*
		 * qseq format:
		 * 11 tab-separated columns
		 *
		 * 1) Instrument
		 * 2) Run id
		 * 3) Lane number
		 * 4) Tile number
		 * 5) X pos
		 * 6) Y pos
		 * 7) Index sequence (0 for runs without multiplexing)
		 * 8) Read Number
		 * 9) Base Sequence
		 * 10) Base Quality
		 * 11) Filter: did the read pass filtering? 0 - No, 1 - Yes.
		 */
		// start:  first valid data index
		private long start;
		// end:  first index value beyond the slice, i.e. slice is in range [start,end)
		private long end;
		// pos: current position in file
		private long pos;
		// file:  the file being read
		private Path file;

		private LineReader lineReader;
		private FSDataInputStream inputStream;
		private Text currentKey = new Text();
		private SequencedFragment currentValue = new SequencedFragment();

		private Text buffer = new Text();
		private static final int NUM_QSEQ_COLS = 11;
		// for these, we have one per qseq field
		private int[] fieldPositions = new int[NUM_QSEQ_COLS]; 
		private int[] fieldLengths = new int[NUM_QSEQ_COLS]; 
		private enum BaseQualityEncoding {
			Illumina,
			Sanger
		};
		private BaseQualityEncoding qualityEncoding;

		private static final String Delim = "\t";

		// How long can a qseq line get?
		public static final int MAX_LINE_LENGTH = 20000;
		public static final String CONF_BASE_QUALITY_ENCODING = "bl.qseq.base-quality-encoding";
		public static final String CONF_BASE_QUALITY_ENCODING_DEFAULT = "illumina";

		public QseqRecordReader(Configuration conf, FileSplit split) throws IOException
		{
			setConf(conf);
			file = split.getPath();
			start = split.getStart();
			end = start + split.getLength();

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
				// Advance to the start of the first line in our slice.
				// We use a temporary LineReader to read a partial line and find the
				// start of the first one on or after our starting position.
				// In case our slice starts right at the beginning of a line, we need to back
				// up by one position and then discard the first line.
				start -= 1;
				stream.seek(start);
				LineReader reader = new LineReader(stream);
				int bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));
				start = start + bytesRead;
				stream.seek(start);
			}
			// else
			//	if start == 0 we're starting at the beginning of a line
			pos = start;
		}

		protected void setConf(Configuration conf)
		{
			String encoding = conf.get(CONF_BASE_QUALITY_ENCODING, CONF_BASE_QUALITY_ENCODING_DEFAULT);

			if ("illumina".equals(encoding))
				qualityEncoding = BaseQualityEncoding.Illumina;
			else if ("sanger".equals(encoding))
				qualityEncoding = BaseQualityEncoding.Sanger;
			else
				throw new RuntimeException("Unknown " + CONF_BASE_QUALITY_ENCODING + " value " + encoding);
		}

		/**
		 * Added to use mapreduce API.
		 */
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
		{
		}

		/**
		 * Added to use mapreduce API.
		 */
		public Text getCurrentKey()
		{
			return currentKey;
		}

		/**
		 * Added to use mapreduce API.
		 */
		public SequencedFragment getCurrentValue()
	 	{
			return currentValue;
		}

		/**
		 * Added to use mapreduce API.
		 */
		public boolean nextKeyValue() throws IOException, InterruptedException
		{
			return next(currentKey, currentValue);
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

		public String makePositionMessage()
		{
			return file.toString() + ":" + pos;
		}

		/**
		 * Reads the next key/value pair from the input for processing.
		 */
		public boolean next(Text key, SequencedFragment value) throws IOException
		{
			if (pos >= end)
				return false; // past end of slice

			int bytesRead = lineReader.readLine(buffer, MAX_LINE_LENGTH);
			pos += bytesRead;
			if (bytesRead == MAX_LINE_LENGTH)
				throw new RuntimeException("found abnormally large line (length " + bytesRead + ") at " + makePositionMessage() + ": " + Text.decode(buffer.getBytes(), 0, 500));
			else if (bytesRead <= 0)
				return false; // EOF
			else
			{
				scanQseqLine(buffer, key, value);
				return true;
			}
		}

		private void setPositionsAndLengths(Text line)
		{
			int pos = 0; // the byte position within the record
			int fieldno = 0; // the field index within the record
			while (pos < line.getLength() && fieldno < NUM_QSEQ_COLS) // iterate over each field
			{
				int endpos = line.find(Delim, pos); // the field's end position
				if (endpos < 0)
					endpos = line.getLength();

				fieldPositions[fieldno] = pos;
				fieldLengths[fieldno] = endpos - pos;

				pos = endpos + 1; // the next starting position is the current end + 1
				fieldno += 1;
			}

			if (fieldno != NUM_QSEQ_COLS)
				throw new FormatException("found " + fieldno + " fields instead of 11 at " + makePositionMessage() + ". Line: " + line);
		}

		private void scanQseqLine(Text line, Text key, SequencedFragment fragment)
		{
			setPositionsAndLengths(line);

			// Build the key.  We concatenate all fields from 0 to 5 (machine to y-pos)
			// and then the read number, replacing the tabs with colons.
			key.clear();
			// append up and including field[5]
			key.append(line.getBytes(), 0, fieldPositions[5] + fieldLengths[5]);
			// replace tabs with :
			byte[] bytes = key.getBytes();
			int temporaryEnd = key.getLength();
			for (int i = 0; i < temporaryEnd; ++i)
				if (bytes[i] == '\t')
					bytes[i] = ':';
			// append the read number
			key.append(line.getBytes(), fieldPositions[7] - 1, fieldLengths[7] + 1); // +/- 1 to catch the preceding tab.
			// convert the tab preceding the read number into a :
			key.getBytes()[temporaryEnd] = ':';

			// now the fragment
			try
			{
				fragment.clear();
				fragment.setInstrument( Text.decode(line.getBytes(), fieldPositions[0], fieldLengths[0]) );
				fragment.setRunNumber( Integer.parseInt(Text.decode(line.getBytes(), fieldPositions[1], fieldLengths[1])) );
				//fragment.setFlowcellId();
				fragment.setLane( Integer.parseInt(Text.decode(line.getBytes(), fieldPositions[2], fieldLengths[2])) );
				fragment.setTile( Integer.parseInt(Text.decode(line.getBytes(), fieldPositions[3], fieldLengths[3])) );
				fragment.setXpos( Integer.parseInt(Text.decode(line.getBytes(), fieldPositions[4], fieldLengths[4])) );
				fragment.setYpos( Integer.parseInt(Text.decode(line.getBytes(), fieldPositions[5], fieldLengths[5])) );
				fragment.setRead( Integer.parseInt(Text.decode(line.getBytes(), fieldPositions[7], fieldLengths[7])) );
				fragment.setFilterPassed( line.getBytes()[fieldPositions[10]] != '0' );
				//fragment.setControlNumber();
				fragment.setIndexSequence(Text.decode(line.getBytes(), fieldPositions[6], fieldLengths[6]).replace('.', 'N'));
			}
			catch (CharacterCodingException e) {
				throw new FormatException("Invalid character format at " + makePositionMessage() + "; line: " + line);
			}
			
			fragment.getSequence().append(line.getBytes(), fieldPositions[8], fieldLengths[8]);
			bytes = fragment.getSequence().getBytes();
			// replace . with N
			for (int i = 0; i < fieldLengths[8]; ++i)
				if (bytes[i] == '.')
					bytes[i] = 'N';


			fragment.getQuality().append(line.getBytes(), fieldPositions[9], fieldLengths[9]);
			if (qualityEncoding == BaseQualityEncoding.Illumina)
			{
				// convert illumina to sanger scale
				bytes = fragment.getQuality().getBytes();
				for (int i = 0; i < fieldLengths[9]; ++i)
				{
					if (bytes[i] < 64)
						throw new FormatException("qseq base quality score too low.  Are they encoded in sanger format?  Position: " + makePositionMessage() + "; line: " + line);
					bytes[i] -= 31; // 64 - 33 = 31: difference between illumina and sanger encoding.
				}
			}
		}
	}

	public RecordReader<Text, SequencedFragment> createRecordReader(
	                                        InputSplit genericSplit,
	                                        TaskAttemptContext context) throws IOException, InterruptedException
	{
		context.setStatus(genericSplit.toString());
		return new QseqRecordReader(context.getConfiguration(), (FileSplit)genericSplit); // cast as per example in TextInputFormat
	}
}
