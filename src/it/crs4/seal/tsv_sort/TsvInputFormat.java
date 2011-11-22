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
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
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
 * An input format that reads the configured fields as the key and the whole 
 * line as the value.  Both key and value are represented as Text.
 */
public class TsvInputFormat extends FileInputFormat<Text,Text> implements Configurable {

	private static final Log LOG = LogFactory.getLog(TsvInputFormat.class);

	public static final String COLUMN_KEYS_CONF = "tsv-input.key-columns"; // empty selects the entire value as the key
	public static final String DELIM_CONF = "tsv-input.delim";
	public static final String DELIM_DEFALT = "\t";

	protected static final Pattern RangeSelectorPatter = Pattern.compile("(\\d)-(\\d)|(\\d)");

  protected static JobConf lastConf = null;
  protected static InputSplit[] lastResult = null;

	protected int[] keyFields = null;
	protected Configuration conf;
	protected String cachedKeyFieldSelector;

	/**
	 * Scan the config parameter COLUMN_KEYS_CONF and set keyFields.
	 */
	private void setupKeyFields(Configuration conf) 
	{
		String keyFieldSelector = conf.get(COLUMN_KEYS_CONF, "");
		if (keyFieldSelector.equals(cachedKeyFieldSelector))
			return; // no need to redo the work

		ArrayList<Integer> fields = new ArrayList<Integer>();

		if (keyFieldSelector.isEmpty())
		{
			LOG.info("key column(s) property not specified (" + COLUMN_KEYS_CONF + ").  Using entire line as the key.");
		}
		else
		{
			String[] groups = keyFieldSelector.split(",");
			for (String g: groups) 
			{
				Matcher m = RangeSelectorPatter.matcher(g);
				if (m.matches())
				{
					if (m.group(1) == null) // specified a simple column number
						fields.add(Integer.parseInt(m.group(0)));
					else
					{
						int start = Integer.parseInt(m.group(1));
						int end = Integer.parseInt(m.group(2));
						if (start <= end)
						{
							for (int i = start; i <= end; ++i)
								fields.add(i);
						}
						else
							throw new IllegalArgumentException("key field specification contains a range with start > end: " + keyFieldSelector);
					}
				}
				else
					throw new IllegalArgumentException("Invalid key column specification syntax " + keyFieldSelector);
			}
		}
		keyFields = new int[fields.size()];
		for (int i = 0; i < keyFields.length; ++i)
			keyFields[i] = fields.get(i);

		// cache the processed keyFieldSelector value
		cachedKeyFieldSelector = keyFieldSelector;
	}

	@Override
	public void setConf(Configuration conf)
	{
		this.conf = conf;
		setupKeyFields(conf);
	}

	@Override
	public Configuration getConf() { return conf; }

  @Override
  public RecordReader<Text, Text> 
      getRecordReader(InputSplit split,
                      JobConf job, 
                      Reporter reporter) throws IOException {
		setConf(job);
    return new TsvRecordReader(job, (FileSplit) split, keyFields);
  }

	/**
	 * Implements caching getSplits.
	 */
  @Override
  public InputSplit[] getSplits(JobConf conf, int splits) throws IOException {
    if (conf == lastConf) {
      return lastResult;
    }
    lastConf = conf;
    lastResult = super.getSplits(conf, splits);
    return lastResult;
  }

  static class TsvRecordReader implements RecordReader<Text,Text> 
	{
		private static final Log LOG = LogFactory.getLog(TsvRecordReader.class);

    private LineRecordReader in;
    private LongWritable junk = new LongWritable();
    private Text line = new Text();
    private CutText cutter;
		private StringBuilder builder;

    public TsvRecordReader(Configuration job, 
                            FileSplit split,
														int[] keyFields) throws IOException {
      in = new LineRecordReader(job, split);
			if (keyFields.length == 0)
			{
				cutter = null;
				builder = null;
			}
			else
			{
				cutter = new CutText( job.get(DELIM_CONF, DELIM_DEFALT), keyFields);
				builder = new StringBuilder(1000);
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
			boolean found = false;

			try {
				if (in.next(junk, value)) 
				{
					found = true;
					if (cutter == null) // whole value is the key
						key.set(value);
					else
					{
						builder.delete(0, builder.length());

						cutter.loadRecord(value);
						int nFields = cutter.getNumFields();
						for (int i = 0; i < nFields; ++i)
							builder.append(cutter.getField(i));

						key.set(builder.toString());
					}
				}
			} catch (CutText.FormatException e) {
				throw new RuntimeException("format problem with line: " + value);
			}
			return found;
    }
  }

}
