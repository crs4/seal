
package it.crs4.seal.utilities;

import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.seqdoop.hadoop_bam.SequencedFragment;

/**
 * For qseq datasets with interleaved reads.
 */
public class InterleavedQseqReader implements QseqReader
{
	protected Path inputPath;
	protected BufferedReader input;
	protected ArrayList<SequencedFragment> outputItems;
	protected int numReads;

	public InterleavedQseqReader(String inputPath, Configuration conf, int numReads) throws IOException {
		if (numReads <= 0 || numReads > 3)
			throw new IllegalArgumentException("Invalid number of reads specified (got: " + numReads + ")");

		this.numReads = numReads;

		Path path = new Path(inputPath);
		FileSystem fs = path.getFileSystem(conf);
		input = new BufferedReader(new InputStreamReader(fs.open(path)));

		// We re-use the same items
		outputItems = new ArrayList<SequencedFragment>(numReads);
		for (int i = 0; i < numReads; ++i)
			outputItems.add(new SequencedFragment());

		advance();
	}

	public void advance() throws IOException {
		for (int readNumber = 1; readNumber <= numReads; ++readNumber) {
			String line = input.readLine();
			if (line == null) {
				System.err.println("null line.  EOF");
				outputItems = null;
				if (readNumber != 1) {
					// first input file longer than others
					System.err.println("WARNING:  file finished on read " + readNumber);
				}
				return;
			}

			QseqLineParser.parseQseqLine(line, outputItems.get(readNumber - 1));
			if (outputItems.get(readNumber - 1).getRead() != readNumber) {
				throw new RuntimeException("Read number not as expected!  Expected read number " +
						readNumber + ". Got line\n\t" + line);
			}
		}
	}

	public boolean done() {
		return outputItems == null;
	}

	public List<SequencedFragment> getCurrent() {
		return outputItems;
	}

	public void close() throws IOException {
		input.close();
	}
}
