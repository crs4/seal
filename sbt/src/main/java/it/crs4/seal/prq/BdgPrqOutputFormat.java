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

package it.crs4.seal.prq;

import it.crs4.seal.common.AbstractTaggedMapping;
import it.crs4.seal.common.ReadPair;

import it.crs4.formats.avro.Fragment;
import it.crs4.formats.avro.Sequence;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.avro.generic.IndexedRecord;

import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Output format for the AvroParquet format using the BDG schema.
 *
 */
public class BdgPrqOutputFormat extends FileOutputFormat<Text, ReadPair>
{
	public static class BdgPrqRecordWriter extends RecordWriter<Text,ReadPair>
	{
		private TaskAttemptContext task;
		private RecordWriter<Void, IndexedRecord> avroRecordWriter;
		private Fragment.Builder fragBuilder;
		private Sequence.Builder seqBuilder;

		public BdgPrqRecordWriter(TaskAttemptContext task, Path outputPath) throws IOException, InterruptedException
		{
			final AvroParquetOutputFormat avroFormat = new AvroParquetOutputFormat();
			avroRecordWriter = avroFormat.getRecordWriter(task, outputPath);
			fragBuilder = Fragment.newBuilder();
			seqBuilder = Sequence.newBuilder();
		}

		public void write(Text readName, ReadPair pair) throws IOException, InterruptedException
		{
			fragBuilder.setReadName(readName.toString());

			List<Sequence> list = fragBuilder.getSequences();
			if (list == null) {
				list = new ArrayList<Sequence>(2);
				fragBuilder.setSequences(list);
			}

			ByteBuffer bb;
			AbstractTaggedMapping map = pair.getRead1();

			// read 1
			bb = map.getSequence();
			seqBuilder.setBases(new String(bb.array(), bb.position(), map.getLength()));
			bb = map.getBaseQualities();
			seqBuilder.setQualities(new String(bb.array(), bb.position(), map.getLength()));
			list.add(seqBuilder.build());

			// read 2
			map = pair.getRead2();

			bb = map.getSequence();
			seqBuilder.setBases(new String(bb.array(), bb.position(), map.getLength()));
			bb = map.getBaseQualities();
			seqBuilder.setQualities(new String(bb.array(), bb.position(), map.getLength()));
			list.add(seqBuilder.build());
				
			avroRecordWriter.write(null, fragBuilder.build());
			list.clear(); // clear the list, since we cache it within fragBuilder
		}

		public void close(TaskAttemptContext context) throws IOException, InterruptedException
		{
			avroRecordWriter.close(context);
		}
	}

	@Override
	public RecordWriter<Text, ReadPair> getRecordWriter(TaskAttemptContext task) throws IOException, InterruptedException
	{
		org.apache.parquet.avro.AvroWriteSupport.setSchema(task.getConfiguration(), Fragment.SCHEMA$);

		return new BdgPrqRecordWriter(task, getDefaultWorkFile(task, ""));
	}
}
