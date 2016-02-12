/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// based on TeraSort from the Hadoop examples

package it.crs4.seal.tsv_sort;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A text output format that writes value and "\n".
 */
public class TextValueOutputFormat extends TextOutputFormat<Text,Text> {

	static class ValueRecordWriter extends LineRecordWriter<Text,Text> {

		public ValueRecordWriter(DataOutputStream out) {
			super(out);
		}

		public void write(Text ignored_key, Text value) throws IOException {
			super.write(null, value);
		}
	}

  public RecordWriter<Text,Text> getRecordWriter(TaskAttemptContext task)
	  throws IOException
	{
		Configuration conf = task.getConfiguration();
		boolean isCompressed = getCompressOutput(task);

		CompressionCodec codec = null;
		String extension = "";

		if (isCompressed)
		{
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(task, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			extension = codec.getDefaultExtension();
		}

		Path file = getDefaultWorkFile(task, extension);
		FileSystem fs = file.getFileSystem(conf);

		DataOutputStream output;

		if (isCompressed)
		{
			FSDataOutputStream fileOut = fs.create(file, false);
			output = new DataOutputStream(codec.createOutputStream(fileOut));
		}
		else
			output = fs.create(file, false);

		return new ValueRecordWriter(output);
	}
}
