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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * Output format for Illumina qseq format.
 * Records are lines of tab-separated fields.  Each record consists of
 *   - Machine name
 *   - Run number
 *   - Lane number
 *   - Tile number
 *   - X coordinate of the spot. Integer (can be negative).
 *   - Y coordinate of the spot. Integer (can be negative).
 *   - Index
 *   - Read Number
 *   - Sequence
 *   - Quality
 *   - Filter
 */

public class QseqOutputFormat extends TextOutputFormat<NullWritable, SequencedFragment>
{
	public static class QseqRecordWriter implements RecordWriter<NullWritable,SequencedFragment> 
	{
    protected static final byte[] newLine;
    protected static final String delim = "\t";
		static {
			try {
				newLine = "\n".getBytes("UTF-8");
			} catch (java.io.UnsupportedEncodingException e) {
				throw new RuntimeException("UTF-8 enconding not supported!");
			}
		}

		protected StringBuilder sBuilder = new StringBuilder(800);
		protected DataOutputStream out;

    public QseqRecordWriter(DataOutputStream out) 
		{
			this.out = out;
		}

    public void write(NullWritable ignored_key, SequencedFragment seq) throws IOException 
		{
			sBuilder.delete(0, sBuilder.length()); // clear
			
			sBuilder.append( seq.getInstrument() == null ? "" : seq.getInstrument() ).append(delim);
			sBuilder.append( seq.getRunNumber() == null ? "" : seq.getRunNumber().toString() ).append(delim);
			sBuilder.append( seq.getLane() == null ? "" : seq.getLane().toString() ).append(delim);
			sBuilder.append( seq.getTile() == null ? "" : seq.getTile().toString() ).append(delim);
			sBuilder.append( seq.getXpos() == null ? "" : seq.getXpos().toString() ).append(delim);
			sBuilder.append( seq.getYpos() == null ? "" : seq.getYpos().toString() ).append(delim);
			sBuilder.append( seq.getIndexSequence() == null ? "" : seq.getIndexSequence() ).append(delim);
			sBuilder.append( seq.getRead() == null ? "" : seq.getRead().toString() ).append(delim);
			// here we also replace 'N' with '.'
			sBuilder.append( seq.getSequence() == null ? "" : seq.getSequence().toString().replace('N', '.')).append(delim);
			sBuilder.append( seq.getQuality() == null ? "" : seq.getQuality().toString() ).append(delim);
			sBuilder.append((seq.getFilterPassed() == null || seq.getFilterPassed() ) ? 1 : 0);

			try {
				ByteBuffer buf = Text.encode(sBuilder.toString());
				out.write(buf.array(), 0, buf.limit());
			} catch (java.nio.charset.CharacterCodingException e) {
				throw new RuntimeException("Error encoding qseq record: " + seq);
			}
      out.write(newLine, 0, newLine.length);
    }
    
    public void close(Reporter report) throws IOException 
		{
      out.close();
    }
  }

  public RecordWriter<NullWritable,SequencedFragment> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
	 	throws IOException 
	{
    Path dir = getWorkOutputPath(job);
    FileSystem fs = dir.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
    return new QseqRecordWriter(fileOut);
  }
}
