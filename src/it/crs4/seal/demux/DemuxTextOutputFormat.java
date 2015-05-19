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

import it.crs4.seal.common.SealToolParser; // for OUTPUT_FORMAT_CONF

import org.seqdoop.hadoop_bam.QseqOutputFormat.QseqRecordWriter;
import org.seqdoop.hadoop_bam.FastqOutputFormat.FastqRecordWriter;
import org.seqdoop.hadoop_bam.SequencedFragment;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;

public class DemuxTextOutputFormat extends FileOutputFormat<Text, SequencedFragment>
{
	protected static class DemuxMultiFileLineRecordWriter extends RecordWriter<Text,SequencedFragment> implements Configurable
	{
		protected static final String DEFAULT_OUTPUT_FORMAT = "qseq";
		protected HashMap<Text, DataOutputStream> outputs;
		protected FileSystem fs;
		protected Path outputPath;
		protected Configuration conf;
		protected boolean isCompressed;
		protected CompressionCodec codec;

		protected Text currentKey = null;
		protected ByteArrayOutputStream buffer = new ByteArrayOutputStream(2000);
		protected RecordWriter<Text, SequencedFragment> formatter;

		protected enum OutputFormatType {
			Qseq,
			Fastq;
		};

		protected OutputFormatType outputFormat;


		public DemuxMultiFileLineRecordWriter(TaskAttemptContext task, Path defaultFile) throws IOException
		{
			conf = task.getConfiguration();
			outputPath = defaultFile;
			this.fs = outputPath.getFileSystem(conf);
			isCompressed = FileOutputFormat.getCompressOutput(task);

			if (isCompressed)
			{
				Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(task, GzipCodec.class);
				codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
				outputPath = outputPath.suffix(codec.getDefaultExtension());
			}

			outputs = new HashMap<Text, DataOutputStream>(20);

			// XXX:  I don't think there's a better way to pass the desired output format
			// into this object.  If we go through the configuration object, we might as
			// well re-use the OUTPUT_FORMAT_CONF property set by the SealToolParser.
			String oformatName = conf.get(SealToolParser.OUTPUT_FORMAT_CONF, DEFAULT_OUTPUT_FORMAT);
			if ("qseq".equalsIgnoreCase(oformatName)) {
				this.outputFormat = OutputFormatType.Qseq;
				this.formatter = new QseqRecordWriter(conf, buffer);
			}
			else if ("fastq".equalsIgnoreCase(oformatName)) {
				this.outputFormat = OutputFormatType.Fastq;
				this.formatter = new FastqRecordWriter(conf, buffer);
			}
			else
				throw new RuntimeException("Unexpected output format " + oformatName);
		}

		public void setConf(Configuration conf) { this.conf = conf; }
		public Configuration getConf() { return conf; }

		public void write(Text key, SequencedFragment value) throws IOException , InterruptedException
		{
			if (value == null)
				return;

			if (key == null)
				throw new RuntimeException("trying to output a null key.  I don't know where to put that.");

			if (currentKey == null) { // first value we get
				currentKey = new Text(key);
			}
			else if (!currentKey.equals(key)) { // new key.  Write batch
				writeBuffer();
				currentKey = new Text(key);
			}
			formatter.write(null, value); // formatter writes into the buffer
		}

		protected void writeBuffer() throws IOException, InterruptedException
		{
			if (currentKey == null || buffer.size() == 0)
				return;

			buffer.writeTo(getOutputStream(currentKey));
			buffer.reset();
		}

		protected DataOutputStream makeOutputStream(Path outputPath) throws IOException
		{
			DataOutputStream ostream;

			if (isCompressed)
			{
				FSDataOutputStream fileOut = fs.create(outputPath, false);
				ostream = new DataOutputStream(codec.createOutputStream(fileOut));
			}
			else
				ostream = fs.create(outputPath, false);

			return ostream;
		}

		protected DataOutputStream getOutputStream(Text key) throws IOException, InterruptedException
		{
			DataOutputStream ostream = outputs.get(key);
			if (ostream == null)
			{
				// create it
				Path dir = new Path(outputPath.getParent(), key.toString());
				Path file = new Path(dir, outputPath.getName());
				if (!fs.exists(dir))
					fs.mkdirs(dir);
				// now create a new ostream that will write to the desired file path
				// (which should not already exist, since we didn't find it in our hash map)
				ostream = makeOutputStream(file);
				outputs.put(key, ostream); // insert the record writer into our map
			}
			return ostream;
		}

		public synchronized void close(TaskAttemptContext context) throws IOException, InterruptedException
		{
			writeBuffer();
			for (DataOutputStream out: outputs.values())
				out.close();
		}
	}

	public RecordWriter<Text,SequencedFragment> getRecordWriter(TaskAttemptContext job) throws IOException
	{
		Path defaultFile = getDefaultWorkFile(job, "");
		return new DemuxMultiFileLineRecordWriter(job, defaultFile);
	}
}
