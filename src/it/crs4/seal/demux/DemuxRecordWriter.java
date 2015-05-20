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

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public abstract class DemuxRecordWriter extends RecordWriter<Text, SequencedFragment>
{
	protected Text currentKey = null;

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

		addToBuffer(value);
	}

	public String getCompressionSuffix(TaskAttemptContext task)
	{
		String suffix;

		boolean isCompressed = FileOutputFormat.getCompressOutput(task);
		if (isCompressed)
		{
			Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(task, GzipCodec.class);
			CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, task.getConfiguration());
			suffix = codec.getDefaultExtension();
		}
		else
			suffix = "";

		return suffix;
	}

	public abstract void writeBuffer() throws IOException, InterruptedException;
	public abstract void addToBuffer(SequencedFragment value);
}
