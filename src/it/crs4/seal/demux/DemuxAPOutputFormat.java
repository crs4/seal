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

import org.seqdoop.hadoop_bam.SequencedFragment;
import org.bdgenomics.formats.avro.Fragment;
import org.bdgenomics.formats.avro.Sequence;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import parquet.avro.AvroParquetOutputFormat;
import parquet.hadoop.codec.CodecConfig;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.ParquetRecordWriter;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

public class DemuxAPOutputFormat extends FileOutputFormat<Text, SequencedFragment>
{
	protected static class DemuxMultiFileAPWriter extends RecordWriter<Text,SequencedFragment>
	{
		protected HashMap<Text, RecordWriter<Void, IndexedRecord>> outputs;
		protected TaskAttemptContext task;
		protected Path outputPath;
		protected Text currentKey = null;
		protected Fragment buffer;
		protected AvroParquetOutputFormat avroFormat;
		protected StringBuilder sBuilder = new StringBuilder(400);

		public DemuxMultiFileAPWriter(TaskAttemptContext task, Path defaultFile) throws IOException
		{
			this.task = task;
			final Configuration conf = task.getConfiguration();
			outputPath = defaultFile;

			boolean isCompressed = FileOutputFormat.getCompressOutput(task);
			if (isCompressed)
			{
				Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(task, GzipCodec.class);
				CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
				outputPath = outputPath.suffix("." + codec.getDefaultExtension());
			}

			outputPath = outputPath.suffix(".parquet");

			avroFormat = new AvroParquetOutputFormat();
			parquet.avro.AvroWriteSupport.setSchema(conf, Fragment.getClassSchema());

			outputs = new HashMap<Text, RecordWriter<Void, IndexedRecord>>(20);
			buffer = Fragment.newBuilder().build();
		}

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
			// in all cases, add the new value to the "buffer" Fragment

			addSequenceToBuffer(value);
		}

		public synchronized void close(TaskAttemptContext context) throws IOException, InterruptedException
		{
			writeBuffer();
			for (RecordWriter<Void, IndexedRecord> out: outputs.values())
				out.close(context);
		}

		protected void writeBuffer() throws IOException, InterruptedException
		{
			if (currentKey == null)
				return;

			getOutputWriter(currentKey).write(null, buffer);

			clearBuffer();
		}

		protected RecordWriter<Void, IndexedRecord> getOutputWriter(Text key) throws IOException, InterruptedException
		{
			RecordWriter<Void, IndexedRecord> writer = outputs.get(key);
			if (writer == null)
			{
				// create it
				final FileSystem fs = outputPath.getFileSystem(task.getConfiguration());
				final Path dir = new Path(outputPath.getParent(), key.toString());
				final Path file = new Path(dir, outputPath.getName());

				if (!fs.exists(dir))
					fs.mkdirs(dir);
				// now create a new writer that will write to the desired file path
				// (which should not already exist, since we didn't find it in our hash map)
				writer = avroFormat.getRecordWriter(task, file);
				outputs.put(key, writer); // insert the record writer into our map
			}

			return writer;
		}

		protected void clearBuffer()
		{
			buffer.setReadName(null);
			buffer.setInstrument(null);
			List<Sequence> list = buffer.getSequences();
			if (list != null)
				list.clear();
		}

		protected void addSequenceToBuffer(SequencedFragment read)
		{
			// we assume all SequencedFragments added to the same Fragment have the same
			// read id and instrument.  Therefore, we avoid recalculating the read id if
			// it's already set.  On the other hand, the instrument is just a reference
			// copy so we just do it.

			if (buffer.getReadName() == null)
				buffer.setReadName(makeReadId(read));
			buffer.setInstrument(read.getInstrument());

			List<Sequence> list = buffer.getSequences();
			if (list == null) {
				list = new ArrayList<Sequence>(2);
				buffer.setSequences(list);
			}

			list.add(new Sequence(read.getSequence().toString(), read.getQuality().toString()));
		}

		protected String makeReadId(SequencedFragment seq)
		{
			// Code from hadoop-bam's FastqOutputFormat
			String delim = ":";
			sBuilder.delete(0, sBuilder.length()); // clear

			sBuilder.append( seq.getInstrument() == null ? "" : seq.getInstrument() ).append(delim);
			sBuilder.append( seq.getRunNumber()  == null ? "" : seq.getRunNumber().toString() ).append(delim);
			sBuilder.append( seq.getFlowcellId() == null ? "" : seq.getFlowcellId() ).append(delim);
			sBuilder.append( seq.getLane()       == null ? "" : seq.getLane().toString() ).append(delim);
			sBuilder.append( seq.getTile()       == null ? "" : seq.getTile().toString() ).append(delim);
			sBuilder.append( seq.getXpos()       == null ? "" : seq.getXpos().toString() ).append(delim);
			sBuilder.append( seq.getYpos()       == null ? "" : seq.getYpos().toString() );

			return sBuilder.toString();
		}
	}

	public RecordWriter<Text,SequencedFragment> getRecordWriter(TaskAttemptContext task) throws IOException
	{
		Path defaultFile = getDefaultWorkFile(task, "");
		return new DemuxMultiFileAPWriter(task, defaultFile);
	}
}
