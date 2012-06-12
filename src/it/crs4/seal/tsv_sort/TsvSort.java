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

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Generates the sampled split points, launches the job, and waits for it to
 * finish.
 */
public class TsvSort extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(TsvSort.class);

	static final String PARTITION_SYMLINK = "_partition.lst";

	/**
	 * A partitioner that splits text keys into roughly equal partitions
	 * in a global sorted order.
	 */
	static class TotalOrderPartitioner extends Partitioner<Text,Text> implements Configurable {
		private TrieNode trie;
		private Text[] splitPoints;
		private Configuration conf;

		/**
		 * A generic trie node
		 */
		static abstract class TrieNode {
			private int level;
			TrieNode(int level) {
		 		this.level = level;
			}
			abstract int findPartition(Text key);
			abstract void print(PrintStream strm) throws IOException;
			int getLevel() {
				return level;
			}
		}

		/**
		 * An inner trie node that contains 256 children based on the next
		 * character.
		 */
		static class InnerTrieNode extends TrieNode {
			private TrieNode[] child = new TrieNode[256];

			InnerTrieNode(int level) {
				super(level);
			}
			int findPartition(Text key) {
				int level = getLevel();
				if (key.getLength() <= level) {
					return child[0].findPartition(key);
				}
				return child[key.getBytes()[level]].findPartition(key);
			}
			void setChild(int idx, TrieNode child) {
				this.child[idx] = child;
			}
			void print(PrintStream strm) throws IOException {
				for(int ch=0; ch < 255; ++ch) {
					for(int i = 0; i < 2*getLevel(); ++i) {
						strm.print(' ');
					}
					strm.print(ch);
					strm.println(" ->");
					if (child[ch] != null) {
						child[ch].print(strm);
					}
				}
			}
		}

		/**
		 * A leaf trie node that does string compares to figure out where the given
		 * key belongs between lower..upper.
		 */
		static class LeafTrieNode extends TrieNode {
			int lower;
			int upper;
			Text[] splitPoints;
			LeafTrieNode(int level, Text[] splitPoints, int lower, int upper) {
				super(level);
				this.splitPoints = splitPoints;
				this.lower = lower;
				this.upper = upper;
			}
			int findPartition(Text key) {
				for(int i=lower; i<upper; ++i) {
					if (splitPoints[i].compareTo(key) >= 0) {
						return i;
					}
				}
				return upper;
			}
			void print(PrintStream strm) throws IOException {
				for(int i = 0; i < 2*getLevel(); ++i) {
					strm.print(' ');
				}
				strm.print(lower);
				strm.print(", ");
				strm.println(upper);
			}
		}


		/**
		 * Read the cut points from the given sequence file.
		 * @param fs the file system
		 * @param p the path to read
		 * @param conf the config
		 * @return the strings to split the partitions on
		 * @throws IOException
		 */
		private static Text[] readPartitions(FileSystem fs, Path p, Configuration conf) throws IOException {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
			List<Text> parts = new ArrayList<Text>();
			Text key = new Text();
			NullWritable value = NullWritable.get();
			while (reader.next(key, value)) {
				parts.add(key);
				key = new Text();
			}
			reader.close();
			return parts.toArray(new Text[parts.size()]);
		}

		/**
		 * Given a sorted set of cut points, build a trie that will find the correct
		 * partition quickly.
		 * @param splits the list of cut points
		 * @param lower the lower bound of partitions 0..numPartitions-1
		 * @param upper the upper bound of partitions 0..numPartitions-1
		 * @param prefix the prefix that we have already checked against
		 * @param maxDepth the maximum depth we will build a trie for
		 * @return the trie node that will divide the splits correctly
		 */
		private static TrieNode buildTrie(Text[] splits, int lower, int upper,
		                                  Text prefix, int maxDepth) {
			int depth = prefix.getLength();
			if (depth >= maxDepth || lower == upper) {
				return new LeafTrieNode(depth, splits, lower, upper);
			}
			InnerTrieNode result = new InnerTrieNode(depth);
			Text trial = new Text(prefix);
			// append an extra byte on to the prefix
			trial.append(new byte[1], 0, 1);
			int currentBound = lower;
			for(int ch = 0; ch < 255; ++ch) {
				trial.getBytes()[depth] = (byte) (ch + 1);
				lower = currentBound;
				while (currentBound < upper) {
					if (splits[currentBound].compareTo(trial) >= 0) {
						break;
					}
					currentBound += 1;
				}
				trial.getBytes()[depth] = (byte) ch;
				result.child[ch] = buildTrie(splits, lower, currentBound, trial, maxDepth);
			}
			// pick up the rest
			trial.getBytes()[depth] = 127;
			result.child[255] = buildTrie(splits, currentBound, upper, trial, maxDepth);
			return result;
		}

		public Configuration getConf() {
			return conf;
		}

		public void setConf(Configuration conf) {
			this.conf = conf;
			try {
				FileSystem fs = FileSystem.getLocal(conf);
				Path partFile = new Path(TsvSort.PARTITION_SYMLINK);
				splitPoints = readPartitions(fs, partFile, conf);
				trie = buildTrie(splitPoints, 0, splitPoints.length, new Text(), 2);
			} catch (IOException ie) {
				throw new IllegalArgumentException("can't read partitions file", ie);
			}
		}

		public TotalOrderPartitioner() {
		}

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return trie.findPartition(key);
		}
	}

	public int run(String[] args) throws Exception {
		LOG.info("starting");

		TsvSortOptionParser parser = new TsvSortOptionParser();
		parser.parse(getConf(), args);

		LOG.info("Using " + parser.getNReduceTasks() + " reduce tasks");

		Job job = new Job(getConf());

		job.setJobName("TsvSort " + parser.getInputPaths().get(0));
		job.setJarByClass(TsvSort.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TsvInputFormat.class);
		job.setOutputFormatClass(TextValueOutputFormat.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);

		// output path
		FileOutputFormat.setOutputPath(job, parser.getOutputPath());

		FileSystem fs = parser.getOutputPath().getFileSystem(job.getConfiguration());
		/*
		 *
		 * Pick a random name for the partition file in the same directory as the
		 * output path.  So, TsvSort /user/me/input /user/me/output
		 * results in the partition file being placed in /user/me/_partition.lst.12340921387402174
		 *
		 * Why not place it directly in the input path?
		 *
		 *   We wouldn't be able to run two sorts on the same data at the same time.
		 *   We've received complaints about this in the past, so it has been a
		 *   limit in practice.
		 *
		 * Why not place it directly in the output path?
		 *
		 *   We'd have to create the output path before the output format did.
		 *   For this to work we'd have to disable the FileOutputFormat's default check
		 *   that verifies that the output directory doesn't exist.  This means that we'd
		 *   need some other way to ensure that we're not writing to the same path where
		 *   some other job wrote.
		 */
		Path partitionFile;
		Random rnd = new Random();
		do {
			partitionFile = new Path(parser.getOutputPath().getParent(), String.format("_partition.lst.%012d", Math.abs(rnd.nextLong())));
		} while (fs.exists(partitionFile)); // this is still subject to a race condition between it and another instance of this program
		partitionFile = partitionFile.makeQualified(fs);

		URI partitionUri = new URI(partitionFile.toString() + "#" + PARTITION_SYMLINK);

		// input paths
		for (Path p: parser.getInputPaths())
			TsvInputFormat.addInputPath(job, p);

		LOG.info("sampling input");
		TextSampler.writePartitionFile(new TsvInputFormat(), job, partitionFile);
		LOG.info("created partitions");
		try {
			DistributedCache.addCacheFile(partitionUri, job.getConfiguration());
			DistributedCache.createSymlink(job.getConfiguration());

			int retcode = job.waitForCompletion(true) ? 0 : 1;
			LOG.info("done");
			return retcode;
		}
		finally {
			LOG.debug("deleting partition file " + partitionFile);
			fs.delete(partitionFile, false);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TsvSort(), args);
		System.exit(res);
	}
}
