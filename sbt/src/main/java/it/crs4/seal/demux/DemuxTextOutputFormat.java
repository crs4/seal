// Copyright (C) 2011-2016 CRS4.
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
import java.util.HashMap;

public class DemuxTextOutputFormat extends FileOutputFormat<DestinationReadIdPair, SequencedFragment>
{
	protected static class DemuxTextRecordWriter extends DemuxRecordWriter
	{
		protected static final String DEFAULT_OUTPUT_FORMAT = "qseq";

		protected HashMap<String, DataOutputStream> outputs;

		protected FileSystem fs;
		protected Path outputPath;
		protected boolean isCompressed;
		protected CompressionCodec codec;

		protected RecordWriter<Text, SequencedFragment> formatter;
		protected ByteArrayOutputStream buffer = new ByteArrayOutputStream(2000);


		public DemuxTextRecordWriter(TaskAttemptContext task, Path defaultFile) throws IOException
		{
			final Configuration conf = task.getConfiguration();

			// XXX:  I don't think there's a better way to pass the desired output format
			// into this object.  If we go through the configuration object, we might as
			// well re-use the OUTPUT_FORMAT_CONF property set by the SealToolParser.
			String oformatName = conf.get(SealToolParser.OUTPUT_FORMAT_CONF, DEFAULT_OUTPUT_FORMAT);
			if ("qseq".equalsIgnoreCase(oformatName)) {
				formatter = new QseqRecordWriter(conf, buffer);
			}
			else if ("fastq".equalsIgnoreCase(oformatName)) {
				formatter = new FastqRecordWriter(conf, buffer);
			}
			else
				throw new RuntimeException("Unexpected output format " + oformatName);
		
			outputPath = defaultFile;
			this.fs = outputPath.getFileSystem(conf);
			isCompressed = FileOutputFormat.getCompressOutput(task);

			if (isCompressed) {
				outputPath = outputPath.suffix(getCompressionSuffix(task));
				Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(task, GzipCodec.class);
				codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			}

			outputs = new HashMap<String, DataOutputStream>(20);
		}

		public void addToBuffer(String readId, SequencedFragment value)
		{
			try {
				formatter.write(null, value);
			}
			catch (IOException e) {
				throw new RuntimeException(e.getMessage());
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e.getMessage());
			}
		}

		public void writeBuffer() throws IOException, InterruptedException
		{
			if (currentDestination == null || buffer.size() == 0)
				return;

			buffer.writeTo(getOutputStream(currentDestination));
			buffer.reset();
		}

		public void close(TaskAttemptContext task) throws IOException, InterruptedException
		{
			writeBuffer();
			for (DataOutputStream out: outputs.values())
				out.close();
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

		protected DataOutputStream getOutputStream(String destination) throws IOException, InterruptedException
		{
			DataOutputStream ostream = outputs.get(destination);
			if (ostream == null)
			{
				// create it
				Path dir = new Path(outputPath.getParent(), destination);
				Path file = new Path(dir, outputPath.getName());
				if (!fs.exists(dir))
					fs.mkdirs(dir);
				// now create a new ostream that will write to the desired file path
				// (which should not already exist, since we didn't find it in our hash map)
				ostream = makeOutputStream(file);
				outputs.put(destination, ostream); // insert the record writer into our map
			}
			return ostream;
		}
	}

	public RecordWriter<DestinationReadIdPair,SequencedFragment> getRecordWriter(TaskAttemptContext job) throws IOException
	{
		Path defaultFile = getDefaultWorkFile(job, "");
		return new DemuxTextRecordWriter(job, defaultFile);
	}
}
