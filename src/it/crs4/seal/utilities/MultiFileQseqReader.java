
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
 * For qseq datasets with one file per read number.
 */
public class MultiFileQseqReader  implements QseqReader
{
	protected List<Path> inputPaths;
	protected ArrayList<BufferedReader> inputs;
	protected ArrayList<SequencedFragment> outputItems;

	public MultiFileQseqReader(List<String> inputPaths, Configuration conf) throws IOException {
		if (inputPaths.isEmpty())
			throw new IllegalArgumentException("empty input argument list");

		inputs = new ArrayList<BufferedReader>(3);
		for (String p: inputPaths) {
			Path path = new Path(p);
			FileSystem fs = path.getFileSystem(conf);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
			inputs.add(reader);
		}

		// We re-use the same items
		outputItems = new ArrayList<SequencedFragment>(inputs.size());
		for (int i = 0; i < inputs.size(); ++i)
			outputItems.add(new SequencedFragment());

		advance();
	}

	public void advance() throws IOException {
		for (int i = 0; i < inputs.size(); ++i) {
			BufferedReader input = inputs.get(i);
			String line = input.readLine();
			if (line == null) {
				outputItems = null;
				if (i != 0) {
					// first input file longer than others
					System.err.println("WARNING:  file " + i + " finished before the previous one(s)");
				}
				return;
			}

			QseqLineParser.parseQseqLine(line, outputItems.get(i));
		}
	}

	public boolean done() {
		return outputItems == null;
	}

	public List<SequencedFragment> getCurrent() {
		return outputItems;
	}

	public void close() throws IOException {
		for (Reader r: inputs)
			r.close();
	}
}

