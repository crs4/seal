
package it.crs4.seal.utilities;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.parquet.avro.AvroParquetOutputFormat;

import org.seqdoop.hadoop_bam.QseqInputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;

import it.crs4.formats.avro.Fragment;
import it.crs4.formats.avro.Sequence;

import it.crs4.seal.common.GroupByLocationComparator;
import it.crs4.seal.common.SequenceId;
import it.crs4.seal.common.SequenceIdLocationPartitioner;
import it.crs4.seal.demux.Demux;

public class qseq2bdg_mr extends Configured implements Tool
{
	private static final Log LOG = LogFactory.getLog(qseq2bdg_mr.class);

	private Path outputFileName;
	private QseqReader reader;
	private int nRecords = 0;

	public qseq2bdg_mr()
	{
		outputFileName = null;
		reader = null;
		nRecords = 0;
	}

	public static class qseq2bdg_reducer extends Reducer<SequenceId, SequencedFragment, NullWritable, Fragment>
	{
		private Fragment.Builder fragBuilder;
		private Sequence.Builder seqBuilder;
		ArrayList<Sequence> sequences = new ArrayList<Sequence>(5);

		@Override
		public void setup(Context context) {
			fragBuilder = Fragment.newBuilder();
			seqBuilder = Sequence.newBuilder();
		}

		@Override
		public void reduce(SequenceId seqId, Iterable<SequencedFragment> values, Context context)
		    throws IOException, InterruptedException
		{
			sequences.clear();

			fragBuilder.setReadName(seqId.getLocation());

			for (SequencedFragment seq : values) { // they're sorted by read number
				fragBuilder.setInstrument(seq.getInstrument());

				seqBuilder.setBases(seq.getSequence().toString());
				seqBuilder.setQualities(seq.getQuality().toString());
				sequences.add(seqBuilder.build());
			}

			fragBuilder.setSequences(sequences);

			context.write(null, fragBuilder.build());
		}
	}

	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length < 2) {
			String usage = "Usage:  qseq2bdg_mr INPUT+ OUTPUT";
			System.err.println(usage);
			ToolRunner.printGenericCommandUsage(System.err);
			return 2;
		}

		Path outputPath = new Path(args[args.length - 1]);

		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		Configuration conf = job.getConfiguration();

		// point to input data
		LOG.info("Input paths:");
		for (int i = 0; i < args.length - 1; ++i) {
			LOG.info(String.format("\t%s", args[i]));
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}

		LOG.info(String.format("Writing output to: %s", outputPath));

		job.setInputFormatClass(QseqInputFormat.class);
		job.setMapperClass(Demux.Map.class);
		job.setMapOutputKeyClass(SequenceId.class);
		job.setMapOutputValueClass(SequencedFragment.class);

		job.setPartitionerClass(SequenceIdLocationPartitioner.class);
		job.setGroupingComparatorClass(GroupByLocationComparator.class);

		job.setReducerClass(qseq2bdg_reducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Fragment.class);

		// set the output format
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setOutputPath(job, outputPath);
		AvroParquetOutputFormat.setSchema(job, Fragment.SCHEMA$);

		if (job.getNumReduceTasks() < 1) {
			LOG.warn(String.format("Number of reduce tasks set to %d.  Resetting to 1", job.getNumReduceTasks()));
			job.setNumReduceTasks(1);
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new qseq2bdg_mr(), args);
	}
}
