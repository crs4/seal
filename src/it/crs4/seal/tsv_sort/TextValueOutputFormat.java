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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * A text output format that writes value and "\n".
 */
public class TextValueOutputFormat extends TextOutputFormat<Text,Text> {

	static class ValueRecordWriter extends LineRecordWriter<Text,Text> {
		private static final byte[] newLine = "\n".getBytes();

		public ValueRecordWriter(DataOutputStream out, JobConf conf) {
			super(out);
		}

		public void write(Text ignored_key, Text value) throws IOException {
			out.write(value.getBytes(), 0, value.getLength());
			out.write(newLine, 0, newLine.length);
		}

		public void close() throws IOException {
			((FSDataOutputStream)out).sync();
			super.close(null);
		}
	}

	public RecordWriter<Text,Text> getRecordWriter(FileSystem ignored,
	                                               JobConf job,
	                                               String name,
	                                               Progressable progress
	                                               ) throws IOException {
		Path dir = getWorkOutputPath(job);
		FileSystem fs = dir.getFileSystem(job);
		FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
		return new ValueRecordWriter(fileOut, job);
	}
}
