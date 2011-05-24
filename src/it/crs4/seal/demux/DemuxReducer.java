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

package it.crs4.seal.demux;

import it.crs4.seal.common.CutText;
import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.SequenceId;
import it.crs4.seal.demux.SampleSheet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Iterator;

public class DemuxReducer
{
	private static final Log LOG = LogFactory.getLog(DemuxReducer.class);
	private SampleSheet sampleSheet;
	private CutText laneReadSeqCutter;
	private CutText readCutter;
	private static final String QseqDelim = "\t";
	private static final byte[] QseqDelimByte = { 9 }; // tab character

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public DemuxReducer()
	{
		sampleSheet = new SampleSheet();
		laneReadSeqCutter = new CutText(QseqDelim, 2, 7, 8); // extract the 2nd and 7th, 8th (0-based) columns
		readCutter = new CutText(QseqDelim, 7);
	}

	public void setup(String localSampleSheetPath, Configuration conf) throws IOException
	{
		// load the sample sheet
		Path path = new Path(localSampleSheetPath).makeQualified(FileSystem.getLocal(conf));
		try {
			sampleSheet = DemuxUtils.loadSampleSheet(path, conf);
		}
		catch (SampleSheet.FormatException e) {
			throw new RuntimeException("Error loading sample sheet.  Message: " + e.getMessage());
		}
	}

	public void reduce(SequenceId key, Iterable<Text> qseqs, IMRContext<Text,Text> context) throws IOException, InterruptedException
	{
		Iterator<Text> qseqs_it = qseqs.iterator();
		Text value;

		try
		{
			value	= qseqs_it.next(); // this should be read 2
			laneReadSeqCutter.loadRecord(value);
			String read = laneReadSeqCutter.getField(1);
			if ( !read.equals("2") )
				throw new RuntimeException("Missing read 2 in multiplexed input for location " + key.getLocation());

			int lane;
			try {
				lane = Integer.parseInt(laneReadSeqCutter.getField(0));
			}
			catch (NumberFormatException e) {
				throw new RuntimeException("bad value for lane number (found: " + laneReadSeqCutter.getField(0) + ")");
			}
			String indexSeq = laneReadSeqCutter.getField(2);
			if (indexSeq.length() != 7)
				throw new RuntimeException("Unexpected bar code sequence length " + indexSeq.length() + " (expected 7)");

			String sampleId = sampleSheet.getSampleId(lane, indexSeq.substring(0,6));
			if (sampleId == null)
				sampleId = "unknown";

			outputKey.set(sampleId);

			value = qseqs_it.next(); // should be read 1
			readCutter.loadRecord(value);
			if (! "1".equals(readCutter.getField(0)) )
				throw new RuntimeException("Expected read 1 but found read " + readCutter.getField(0));

			// write out the first read
			context.write(outputKey, value);
			context.increment("Sample reads", sampleId, 1);

			// all following reads need to be adjusted, decreasing their read number by 1

			while (qseqs_it.hasNext())
			{
				value = qseqs_it.next();
				outputValue = shiftReadNumber(value); // actually re-uses outputValue internally and returns it
				context.write(outputKey, outputValue);
				context.increment("Sample reads", sampleId, 1);
			}
		}
		catch (CutText.FormatException e) {
			throw new RuntimeException("Qseq record too short.  Complete message: " + e.getMessage());
		}
		catch (CharacterCodingException e) {
			throw new RuntimeException("Error encoding text.  Message:  " + e.getMessage());
		}
	}

	private Text shiftReadNumber(Text qseq) throws CharacterCodingException, CutText.FormatException
	{
		readCutter.loadRecord(qseq);
		int readPos = readCutter.getFieldPos(0);
		int readNum;
		try {
			readNum	= Integer.parseInt(readCutter.getField(0));
		}
		catch (NumberFormatException e) {
			throw new RuntimeException("bad value for lane number (found: " + readCutter.getField(0) + ")");
		}
		if (readNum < 3)
			throw new RuntimeException("Expected a read number of at least 3, but found " + readNum);

		readNum -= 1; // shift read number down by 1

		outputValue.clear();
		outputValue.append(qseq.getBytes(), 0, readPos); // will append up to and including the tab before the read number
		// encode the new read number and append it to the output qseq
		ByteBuffer encodedReadNum = Text.encode( String.valueOf(readNum) );
		// The docs for Text.encode specify that the returned ByteBuffer has an array() and its length is limit()
		outputValue.append(encodedReadNum.array(), encodedReadNum.arrayOffset(), encodedReadNum.limit());
		// append the delimiter
		outputValue.append(QseqDelimByte, 0, QseqDelimByte.length);
		// finish with the rest of the record
		int startOfTheRest = readPos + readCutter.getField(0).length() + QseqDelim.length();
		outputValue.append(qseq.getBytes(), startOfTheRest, qseq.getLength() - startOfTheRest);

		return outputValue;
	}
}
