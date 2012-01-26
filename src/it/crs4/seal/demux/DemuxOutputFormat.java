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

package it.crs4.seal.demux;

import it.crs4.seal.common.QseqOutputFormat.QseqRecordWriter;
import it.crs4.seal.common.SequencedFragment;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

public class DemuxOutputFormat extends FileOutputFormat<Text, SequencedFragment> 
{
	protected static class DemuxMultiFileLineRecordWriter extends RecordWriter<Text,SequencedFragment> implements Configurable
	{
		protected HashMap<Text,QseqRecordWriter> outputs;
		protected FileSystem fs;
		protected Path defaultFile;
		protected Configuration conf;

		public DemuxMultiFileLineRecordWriter(Configuration conf, FileSystem fs, Path defaultFile) 
		{
			this.fs = fs;
			this.defaultFile = defaultFile;
			this.conf = conf;
			outputs = new HashMap<Text,QseqRecordWriter>(20);
		}

		public void setConf(Configuration conf) { this.conf = conf; }
		public Configuration getConf() { return conf; }


		public void write(Text key, SequencedFragment value) throws IOException , InterruptedException
		{
			if (value == null)
				return;

			if (key == null)
				throw new RuntimeException("trying to output a null key.  I don't know where to put that.");

			QseqRecordWriter qseqWriter = getOutputStream(key);
			qseqWriter.write(null, value);
		}

		protected QseqRecordWriter getOutputStream(Text key) throws IOException, InterruptedException 
		{
			QseqRecordWriter writer = outputs.get(key);
			if (writer == null)
			{
				// create it
				Path dir = new Path(defaultFile.getParent(), key.toString());
				Path file = new Path(dir, defaultFile.getName());
				if (!fs.exists(dir))
					fs.mkdirs(dir);
				// now create a new file (which should not already exist, since we didn't find it in our hash map)
				// and wrap it in a qseq record writer
				writer = new QseqRecordWriter(conf, fs.create(file, false));
				outputs.put(key, writer); // insert the record writer into our map
			}
			return writer;
		}

		public synchronized void close(TaskAttemptContext context) throws IOException 
		{
			for (QseqRecordWriter out: outputs.values())
				out.close(null);
		}
	}

	public RecordWriter<Text,SequencedFragment> getRecordWriter(TaskAttemptContext job) throws IOException
	{
		Configuration conf = job.getConfiguration();
		Path defaultFile = getDefaultWorkFile(job, "");
		FileSystem fs = defaultFile.getFileSystem(conf);
		return new DemuxMultiFileLineRecordWriter(conf, fs, defaultFile);
	}
}
