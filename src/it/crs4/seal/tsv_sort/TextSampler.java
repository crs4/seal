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

public class TextSampler implements IndexedSortable {

	public static final int MAX_SLICES_SAMPLED = 20;
	public static final int SAMPLE_SIZE_DEFAULT = 100000;
  static final String SAMPLE_SIZE_CONF = "textsampler.sample.size";

	private ArrayList<Text> records = new ArrayList<Text>();

	public int compare(int i, int j) {
		Text left = records.get(i);
		Text right = records.get(j);
		return left.compareTo(right);
	}

	public void swap(int i, int j) {
		Text left = records.get(i);
		Text right = records.get(j);
		records.set(j, left);
		records.set(i, right);
	}

	public void addKey(Text key) {
		records.add(new Text(key));
	}

	/**
	 * Find the split points for a given sample. The sample keys are sorted
	 * and down sampled to find even split points for the partitions. The
	 * returned keys should be the start of their respective partitions.
	 * @param numPartitions the desired number of partitions
	 * @return an array of size numPartitions - 1 that holds the split points
	 */
	Text[] createPartitions(int numPartitions) {
		int numRecords = records.size();
		System.out.println("Making " + numPartitions + " from " + numRecords + 
											 " records");
		if (numPartitions > numRecords) {
			throw new IllegalArgumentException
				("Requested more partitions than input keys (" + numPartitions +
				 " > " + numRecords + ")");
		}
		new QuickSort().sort(this, 0, records.size());
		float stepSize = numRecords / (float) numPartitions;
		System.out.println("Step size is " + stepSize);
		Text[] result = new Text[numPartitions-1];
		for(int i=1; i < numPartitions; ++i) {
			result[i-1] = records.get(Math.round(stepSize * i));
		}
		return result;
	}
  
  /**
   * Use the input splits to take samples of the input and generate sample
   * keys. By default reads 100,000 keys from 20 locations in the input, sorts
   * them and picks N-1 keys to generate N equally sized partitions.
	 * @param inFormat The input to sample
   * @param conf the job to sample
   * @param partFile where to write the output file to
   * @throws IOException if something goes wrong
   */
  public static void writePartitionFile(FileInputFormat<Text,Text> inFormat, JobConf conf, 
                                        Path partFile) throws IOException {
    TextSampler sampler = new TextSampler();
    Text key = new Text();
    Text value = new Text();
    int partitions = conf.getNumReduceTasks();
    long sampleSize = conf.getLong(SAMPLE_SIZE_CONF, SAMPLE_SIZE_DEFAULT);
    InputSplit[] splits = inFormat.getSplits(conf, conf.getNumMapTasks());
    int samples = Math.min(MAX_SLICES_SAMPLED, splits.length);
    long recordsPerSample = sampleSize / samples;
    int sampleStep = splits.length / samples;
    long records = 0;
    // take N samples from different parts of the input
    for(int i=0; i < samples; ++i) {
      RecordReader<Text,Text> reader = 
        inFormat.getRecordReader(splits[sampleStep * i], conf, null);
      while (reader.next(key, value)) {
        sampler.addKey(key);
        records += 1;
        if ((i+1) * recordsPerSample <= records) {
          break;
        }
      }
    }
    FileSystem outFs = partFile.getFileSystem(conf);
    if (outFs.exists(partFile)) {
      outFs.delete(partFile, false);
    }
    SequenceFile.Writer writer = 
      SequenceFile.createWriter(outFs, conf, partFile, Text.class, 
                                NullWritable.class);
    NullWritable nullValue = NullWritable.get();
    for(Text split : sampler.createPartitions(partitions)) {
      writer.append(split, nullValue);
    }
    writer.close();
  }
}
