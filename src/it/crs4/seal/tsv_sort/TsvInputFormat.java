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

import it.crs4.seal.common.CutText;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An input format that reads the configured field as the key and the whole 
 * line as the value.  Both key and value are represented as Text.
 */
public class TsvInputFormat extends FileInputFormat<Text,Text> {

  static final String PARTITION_FILENAME = "_partition.lst";

  private static JobConf lastConf = null;
  private static InputSplit[] lastResult = null;

  static class TsvRecordReader implements RecordReader<Text,Text> {

		public static final String COLUMN_KEYS_CONF = "tsv-input.column-keys";
		public static final String DELIM_CONF = "tsv-input.delim";
		public static final String DELIM_DEFALT = "\t";

	
		private static final Log LOG = LogFactory.getLog(TsvRecordReader.class);

    private LineRecordReader in;
    private LongWritable junk = new LongWritable();
    private Text line = new Text();
    private CutText cutter;
		private int[] keyFields;
		private int[] fieldsOrder;
		private static final Pattern RangeSelectorPatter = Pattern.compile("(\\d)-(\\d)|(\\d)");

    public TsvRecordReader(Configuration job, 
                            FileSplit split) throws IOException {
      in = new LineRecordReader(job, split);
			cutter = new CutText( conf.get(DELIM_CONF, DELIM_DEFALT), fields);
    }

		private int[] setupKeyFields(Configuration conf) {
			ArrayList<Integer> fields = new ArrayList<Integer>();
			String keyFieldSelector = conf.get(COLUMN_KEYS_CONF, "1");
			String[] groups = keyFieldSelector.split(",");
			for (String g: groups) {
				String[] range = g.split("-");
				if (range.length > 1)
			}
		}

    public void close() throws IOException {
      in.close();
    }

    public Text createKey() {
      return new Text();
    }

    public Text createValue() {
      return new Text();
    }

    public long getPos() throws IOException {
      return in.getPos();
    }

    public float getProgress() throws IOException {
      return in.getProgress();
    }

    public boolean next(Text key, Text value) throws IOException {
      if (in.next(junk, value)) {
				cutter.loadRecord(value);
				key.clear();
				for (int i = 0; i < 


          byte[] bytes = line.getBytes();
          key.set(bytes, 0, keyLength);
          value.set(bytes, keyLength, line.getLength() - keyLength);
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public RecordReader<Text, Text> 
      getRecordReader(InputSplit split,
                      JobConf job, 
                      Reporter reporter) throws IOException {
    return new TeraRecordReader(job, (FileSplit) split);
  }

  @Override
  public InputSplit[] getSplits(JobConf conf, int splits) throws IOException {
    if (conf == lastConf) {
      return lastResult;
    }
    lastConf = conf;
    lastResult = super.getSplits(conf, splits);
    return lastResult;
  }
}
