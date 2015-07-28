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

import org.seqdoop.hadoop_bam.SequencedFragment;
import org.bdgenomics.formats.avro.Fragment;
import org.bdgenomics.formats.avro.Sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import parquet.avro.AvroParquetOutputFormat;

import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

public class DemuxAPOutputFormat extends FileOutputFormat<DestinationReadIdPair, SequencedFragment>
{
	protected static class DemuxAPRecordWriter extends DemuxRecordWriter
	{
		protected HashMap<String, RecordWriter<Void, IndexedRecord>> outputs;

		protected TaskAttemptContext task;
		protected Path outputPath;

		protected Fragment.Builder fragBuilder;
		protected Sequence.Builder seqBuilder;
		protected StringBuilder sBuilder = new StringBuilder(400);

		public DemuxAPRecordWriter(TaskAttemptContext task, Path defaultFile) throws IOException
		{
			this.task = task;
			final Configuration conf = task.getConfiguration();
			outputPath = defaultFile;

			if (FileOutputFormat.getCompressOutput(task))
				outputPath = outputPath.suffix(getCompressionSuffix(task));

			outputPath = outputPath.suffix(".parquet");

			outputs = new HashMap<String, RecordWriter<Void, IndexedRecord>>(20);
			fragBuilder = Fragment.newBuilder();
			seqBuilder = Sequence.newBuilder();
		}

		protected void addToBuffer(String readId, SequencedFragment read)
		{
			fragBuilder.setReadName(readId);
			fragBuilder.setInstrument(read.getInstrument());

			List<Sequence> list = fragBuilder.getSequences();
			if (list == null) {
				list = new ArrayList<Sequence>(2);
				fragBuilder.setSequences(list);
			}

			seqBuilder.setBases(read.getSequence().toString());
			seqBuilder.setQualities(read.getQuality().toString());
			list.add(seqBuilder.build());
		}

		public static void printFragment(Fragment f, java.io.PrintStream out) {
				out.println("Here's the fragment");
				out.println("\tread name: " + f.getReadName());
				out.println("\tinstrument: " + f.getInstrument());
				List<Sequence> list = f.getSequences();
				out.println("\tn sequences:  " + list.size());
				for (Sequence s : list) {
					out.println("\t\ts:  " + s.getBases());
					out.println("\t\tq:  " + s.getQualities());
				}
		}

		protected void writeBuffer() throws IOException, InterruptedException
		{
			if (currentDestination == null)
				return;

			Fragment f = fragBuilder.build();
			getOutputWriter(currentDestination).write(null, f);
			clearBuffer();
		}

		public void close(TaskAttemptContext task) throws IOException, InterruptedException
		{
			writeBuffer();
			for (RecordWriter<Void, IndexedRecord> out: outputs.values())
				out.close(task);
		}

		protected RecordWriter<Void, IndexedRecord> getOutputWriter(String destination) throws IOException, InterruptedException
		{
			RecordWriter<Void, IndexedRecord> writer = outputs.get(destination);
			if (writer == null)
			{
				// create it
				final FileSystem fs = outputPath.getFileSystem(task.getConfiguration());
				final Path dir = new Path(outputPath.getParent(), destination);
				final Path file = new Path(dir, outputPath.getName());

				if (!fs.exists(dir))
					fs.mkdirs(dir);
				// now create a new writer that will write to the desired file path
				// (which should not already exist, since we didn't find it in our hash map)
				AvroParquetOutputFormat avroFormat = new AvroParquetOutputFormat();
				writer = avroFormat.getRecordWriter(task, file);
				outputs.put(destination, writer); // insert the record writer into our map
			}

			return writer;
		}

		protected void clearBuffer()
		{
			fragBuilder.clearReadName();
			fragBuilder.clearInstrument();
			fragBuilder.clearSequences();
			seqBuilder.clearBases();
			seqBuilder.clearQualities();
		}
	}

	public RecordWriter<DestinationReadIdPair,SequencedFragment> getRecordWriter(TaskAttemptContext task) throws IOException
	{
		parquet.avro.AvroWriteSupport.setSchema(task.getConfiguration(), Fragment.SCHEMA$);

		Path defaultFile = getDefaultWorkFile(task, "");
		return new DemuxAPRecordWriter(task, defaultFile);
	}
}
