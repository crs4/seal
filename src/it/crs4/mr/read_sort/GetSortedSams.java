package it.crs4.mr.read_sort;

import it.crs4.mr.read_sort.BwaRefAnnotation;

import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.cli.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class GetSortedSams extends Configured implements Tool
{
	private String userInput;
	private String userOutput;
	private String userAnnotation;

	private BwaRefAnnotation refAnnotation;

	private Path annotationPath;
	private Path[] inputPaths;
	private Path outputPath;


	private Path getQualifiedPath(String simplePath) throws IOException
	{
		Path path = new Path(simplePath);
		return path.makeQualified(path.getFileSystem(getConf()));
	}

	/**
	 * Scan command line and set configuration values appropriately.
	 * Calls System.exit in case of a command line error.
	 */
	private void scanOptions(String[] args)
	{
		Configuration conf = getConf();

		Option ann = OptionBuilder
			              .withDescription("annotation file (.ann) of the BWA reference used to create the SAM data")
			              .hasArg()
			              .withArgName("ref.ann")
										.withLongOpt("annotations")
			              .create("ann");
		Options options = new Options();
		options.addOption(ann);

		CommandLineParser parser = new GnuParser();

		try {
			CommandLine line = parser.parse( options, args );

			if (line.hasOption("ann"))
				userAnnotation = line.getOptionValue("ann");
			else
				throw new ParseException("You must provide the path the reference annotation file (<ref>.ann)");

			// remaining args
			String[] otherArgs = line.getArgs();
			if (otherArgs.length == 2) 
			{
				userInput = otherArgs[0];
				userOutput = otherArgs[1];
			}
			else
				throw new ParseException("You must provide HDFS input and output paths");
		}
		catch( ParseException e ) 
		{
			System.err.println("Usage error: " + e.getMessage());
			// XXX: redirect System.out to System.err since the simple version of 
			// HelpFormatter.printHelp prints to System.out, and we're on a way to
			// a fatal exit.
			System.setOut(System.err);
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "TODO [options] <in> <out>", options);
			System.exit(1);
		}
	}

	private void loadAnnotations() throws IOException
	{
		Path annPath = getQualifiedPath(userAnnotation);
		FileSystem fs = annPath.getFileSystem(getConf());
		FSDataInputStream in = fs.open(annPath);
		refAnnotation = new BwaRefAnnotation(new InputStreamReader(in));
	}

	public int run(String[] args) throws Exception
	{
		scanOptions(args);

		System.err.println("scanned arguments");
		System.err.println("userInput: " + userInput);
		System.err.println("userOutput: " + userOutput);
		System.err.println("userAnnotation: " + userAnnotation);

		Configuration conf = getConf();

		loadAnnotations();

		Path srcPath = new Path(userInput);
    FileSystem srcFs = srcPath.getFileSystem(conf);

		Path destPath = getQualifiedPath(userOutput);
		FileSystem destFs = destPath.getFileSystem(conf);

    Path[] sources = FileUtil.stat2Paths(srcFs.globStatus(srcPath), srcPath); // glob returns sorted paths

		System.err.println("ready to copy.  Sources:");
		for (int i = 0; i < sources.length; ++i)
			System.err.println(sources[i]);
		System.err.println("ready to copy.  Dest:" + destPath);

    for(int i = 0; i < sources.length; ++i)
			FileUtil.copyMerge(srcFs, sources[i], destFs, destPath, false, conf, null);

		return 0;
	}

	public static void main(String[] args)
	{
		int res = 0;
		try
		{
			res = ToolRunner.run(new GetSortedSams(), args);
		}
		catch (Exception e)
		{
			System.err.println("Error executing GetSortedSams: " + e.getMessage());
			System.exit(1);
		}
	}
}
