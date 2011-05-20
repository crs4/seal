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

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.DataOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

public class DemuxOutputFormat extends FileOutputFormat<Text, Text> 
{
	protected static class DemuxMultiFileLineRecordWriter extends RecordWriter<Text,Text>
	{
		/*
		 * Much of this code has been taken from TextOutputFormat.LineRecordWriter
		 */
    private static final String utf8 = "UTF-8";
    private static final byte[] newline;
    static {
      try {
        newline = "\n".getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    protected HashMap<Text,DataOutputStream> outputs;
		protected FileSystem fs;
		protected Path defaultFile;

    public DemuxMultiFileLineRecordWriter(FileSystem fs, Path defaultFile) 
		{
			this.fs = fs;
			this.defaultFile = defaultFile;
			outputs = new HashMap<Text,DataOutputStream>(20);
    }

    public synchronized void write(Text key, Text value) throws IOException , InterruptedException
		{
      if (value == null)
        return;

			if (key == null)
				throw new RuntimeException("trying to output a null key.  I don't know where to put that.");

			DataOutputStream out = getOutputStream(key);
			out.write(value.getBytes(), 0, value.getLength());
      out.write(newline);
    }

		protected DataOutputStream getOutputStream(Text key) throws IOException, InterruptedException 
		{
			DataOutputStream outFile = outputs.get(key);
			if (outFile == null)
			{
				// create it
				Path dir = new Path(defaultFile.getParent(), key.toString());
				Path file = new Path(dir, defaultFile.getName());
				if (!fs.exists(dir))
					fs.mkdirs(dir);
				outFile = fs.create(file, false); // should not already exist, since we didn't find it in our hash map
				outputs.put(key, outFile);
			}
			return outFile;
		}

    public synchronized void close(TaskAttemptContext context) throws IOException 
		{
			for (DataOutputStream out: outputs.values())
				out.close();
    }
	}

  public RecordWriter<Text,Text> getRecordWriter(TaskAttemptContext job) throws IOException
	{
		Configuration conf = job.getConfiguration();
		Path defaultFile = getDefaultWorkFile(job, "");
		FileSystem fs = defaultFile.getFileSystem(conf);
		return new DemuxMultiFileLineRecordWriter(fs, defaultFile);
  }
}
