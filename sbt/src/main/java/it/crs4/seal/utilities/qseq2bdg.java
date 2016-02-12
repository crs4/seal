
package it.crs4.seal.utilities;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.avro.AvroParquetWriter;
import org.apache.avro.Schema;

import org.seqdoop.hadoop_bam.SequencedFragment;

import org.bdgenomics.formats.avro.Fragment;
import org.bdgenomics.formats.avro.Sequence;

public class qseq2bdg extends Configured implements Tool
{
	public static final String PROP_QSEQ_FORMAT    = "qseq2bdg.qseq.format";
	public static final String PROP_QSEQ_NUM_READS = "qseq2bdg.qseq.num.reads";
	public static final String QSEQ_INTERLEAVED    = "interleaved";
	public static final String QSEQ_MULTIFILE      = "multifile";

	private Path outputFileName;
	private Schema schema;
	private QseqReader reader;
	private int nRecords = 0;

	public qseq2bdg()
	{
		outputFileName = null;
		schema = null;
		reader = null;
		nRecords = 0;
	}

	public void loadSchema(File schemaFile) throws IOException {
		schema = new Schema.Parser().parse(schemaFile);
	}

	public void setReader(QseqReader reader) {
		this.reader = reader;
	}

	public void convertDataset(QseqReader reader, Path outputPath) throws IOException
	{
		nRecords = 0;
		AvroParquetWriter<Fragment> writer = new AvroParquetWriter<Fragment>(outputPath, schema);
		System.err.println("Created writer");

		while (!reader.done()) {
			Fragment frag = convertToFragment(reader.getCurrent());
			writer.write(frag);
			nRecords += 1;
			if (nRecords % 100 == 0) {
				System.err.println("Converted fragment:");
				System.err.println("\tread name: " + frag.getReadName());
				System.err.println("\tInstrument: " + frag.getInstrument());
				System.err.println("\tSequences: ");
				for (Sequence s: frag.getSequences()) {
					System.err.println("\t\t" + s.getBases() + " --- " + s.getQualities());
				}
			}
			reader.advance();
		}
		System.err.println("Reader is done.  Closing everything");

		reader.close();
		writer.close();
		System.err.println("Wrote " + nRecords + " Fragment records");
	}

	private static String makeReadId(SequencedFragment seq)
	{
		StringBuilder sBuilder = new StringBuilder(100);

		sBuilder.append(seq.getInstrument()).append(':');
		sBuilder.append(seq.getRunNumber()) .append(':');
		sBuilder.append(seq.getLane())      .append(':');
		sBuilder.append(seq.getTile())      .append(':');
		sBuilder.append(seq.getXpos())      .append(':');
		sBuilder.append(seq.getYpos());
		return sBuilder.toString();
	}

	private Fragment convertToFragment(List<SequencedFragment> qseq) {
		Fragment.Builder fragBuilder = Fragment.newBuilder();
		Sequence.Builder seqBuilder = Sequence.newBuilder();

		SequencedFragment first = qseq.get(0);
		// we blindly assume metadata is identical for all reads;

		fragBuilder.setReadName(makeReadId(first));
		fragBuilder.setInstrument(first.getInstrument());

		ArrayList<Sequence> sequences = new ArrayList<Sequence>(qseq.size());

		for (SequencedFragment seq : qseq) {
			seqBuilder.setBases(seq.getSequence().toString());
			seqBuilder.setQualities(seq.getQuality().toString());
			sequences.add(seqBuilder.build());
		}

		fragBuilder.setSequences(sequences);

		return fragBuilder.build();
	}

	public int run(String[] args) throws Exception
	{
		if (args.length <= 2) {
			String usage = "Usage:  qseq2bdg " +
			  "[ -D" + PROP_QSEQ_FORMAT + "=" + QSEQ_INTERLEAVED + " -D" + PROP_QSEQ_NUM_READS + "=n " +
			  " | -D" + PROP_QSEQ_FORMAT + "=" + QSEQ_MULTIFILE + " ] " +
			  " SCHEMA READ1.qseq [ READ2.qseq [ READ3.qseq ] ] <OUTPUT>";
			System.err.println(usage);
			ToolRunner.printGenericCommandUsage(System.err);
			return 2;
		}

		Path outputFileName = new Path(args[args.length - 1]);
		File schemaFile = new File(args[0]);

		List<String> inputPaths = new ArrayList<String>(args.length - 2);
		for (int i = 1; i < args.length - 1; ++i)
			inputPaths.add(args[i]);

		System.err.println("Reading schema from " + schemaFile);
		System.err.println("Reading input from: ");
		for (int i = 1; i <= inputPaths.size(); ++i)
			System.err.println("\t" + i + ": " + inputPaths.get(i - 1));
		System.err.println("Writing output to: " + outputFileName);

		System.err.println("Loading schema file " + schemaFile);
		loadSchema(schemaFile);

		Configuration conf = getConf();
		QseqReader reader;
		if (conf.get(PROP_QSEQ_FORMAT, QSEQ_INTERLEAVED).equals(QSEQ_INTERLEAVED)) {
			int numReads = conf.getInt(PROP_QSEQ_NUM_READS, 2);
			System.err.println(PROP_QSEQ_FORMAT + " is " + QSEQ_INTERLEAVED);
			System.err.println(PROP_QSEQ_NUM_READS + " is " + numReads);
			reader = new InterleavedQseqReader(inputPaths.get(0), getConf(), numReads);
		}
		else if (QSEQ_MULTIFILE.equals(conf.get(PROP_QSEQ_FORMAT))) {
			System.err.println(PROP_QSEQ_FORMAT + " is " + QSEQ_MULTIFILE);
			System.err.println("Got " + inputPaths.size() + " input files.  Assuming " + inputPaths.size() + " reads");
			reader = new MultiFileQseqReader(inputPaths, getConf());
		}
		else {
			throw new IllegalArgumentException("Unrecognized qseq format " + conf.get(PROP_QSEQ_FORMAT) +
			            " specified with " + PROP_QSEQ_FORMAT + " property");
		}

		convertDataset(reader, outputFileName);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(null, new qseq2bdg(), args);
	}
}
