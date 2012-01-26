// Copyright (C) 2011-2012 CRS4.
// 
// This file is part of Seal.
// 
// Seal is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
// 
// Seal is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
// for more details.
// 
// You should have received a copy of the GNU General Public License along
// with Seal.  If not, see <http://www.gnu.org/licenses/>.

package it.crs4.seal.read_sort;

import it.crs4.seal.common.BwaRefAnnotation;
import it.crs4.seal.common.SealToolRunner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;


import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;


public class MergeAlignments extends Configured implements Tool
{
	private String userInput;
	private String userOutput;
	private String userReferenceRoot;
	private String userAnnotation;
	private String sortOrder = "coordinate";
	private String genomeAssemblyId;

	private BwaRefAnnotation refAnnotation;

	private Path referenceRootPath;
	private Path annotationPath;
	private Path[] inputPaths;
	private Path outputPath;

	private boolean generatedMd5 = false;
	private FastaChecksummer checksums;

	private Map<String, String> readGroupFields;

	private static final Log log = LogFactory.getLog(MergeAlignments.class); // log must not go to standard output.

	private Path getQualifiedPath(String simplePath) throws IOException
	{
		Path path = new Path(simplePath);
		return path.makeQualified(path.getFileSystem(getConf()));
	}

	private Map<String, Option> defineRGOptions()
	{
 		Map<String, Option> readGroupOptions = new HashMap<String, Option>();

		readGroupOptions.put("ID", 
			              OptionBuilder
			              .withDescription("Read group id")
			              .hasArg()
			              .withArgName("ID")
			              .withLongOpt("rg-id")
			              .create("rgid"));

		readGroupOptions.put("SM",
			              OptionBuilder
			              .withDescription("Read group sample")
			              .hasArg()
			              .withArgName("sample")
			              .withLongOpt("rg-sm")
			              .create("rgsm"));

		readGroupOptions.put("LB",
			              OptionBuilder
			              .withDescription("Read group library")
			              .hasArg()
			              .withArgName("library")
			              .withLongOpt("rg-lb")
			              .create("rglb"));

		readGroupOptions.put("PU",
			              OptionBuilder
			              .withDescription("Read group platform unit")
			              .hasArg()
			              .withArgName("pu")
			              .withLongOpt("rg-pu")
			              .create("rgpu"));

		readGroupOptions.put("CN",
			              OptionBuilder
			              .withDescription("Read group center")
			              .hasArg()
			              .withArgName("center")
			              .withLongOpt("rg-cn")
			              .create("rgcn"));

		readGroupOptions.put("DT",
			              OptionBuilder
			              .withDescription("Read group date")
			              .hasArg()
			              .withArgName("date")
			              .withLongOpt("rg-dt")
			              .create("rgdt"));

		readGroupOptions.put("PL",
			              OptionBuilder
			              .withDescription("Read group platform")
			              .hasArg()
			              .withArgName("platform")
			              .withLongOpt("rg-pl")
			              .create("rgpl"));

		return readGroupOptions;
	}

	private Map<String, String> parseReadGroupOptions(Map<String, Option> readGroupOptions, CommandLine args) throws ParseException
	{
		HashMap<String, String> fields = new HashMap<String, String>();

		for (Map.Entry<String, Option> pair: readGroupOptions.entrySet())
		{
			String fieldName = pair.getKey();
			Option opt = pair.getValue();
			if (args.hasOption(opt.getOpt()))
			{
				fields.put(fieldName, args.getOptionValue( opt.getOpt() ));
			}
		}

		if (!fields.isEmpty())
		{
			if (!fields.containsKey("ID") || !fields.containsKey("SM"))
				throw new ParseException("If you specify read group tags (RG) you must specify at least id and sample");
		}
		return fields;
	}

	/**
	 * Scan command line and set configuration values appropriately.
	 * Calls System.exit in case of a command line error.
	 */
	private void scanOptions(String[] args)
	{
		Options options = new Options();

		Option ref = OptionBuilder
			              .withDescription("root path to the reference used to create the SAM data")
			              .hasArg()
			              .withArgName("REF_PATH")
			              .withLongOpt("reference")
			              .create("ref");
		options.addOption(ref);


		Option ann = OptionBuilder
			              .withDescription("annotation file (.ann) of the BWA reference used to create the SAM data (not required if you specify " + ref.getOpt() + ")")
			              .hasArg()
			              .withArgName("ref.ann")
			              .withLongOpt("annotations")
			              .create("ann");
		options.addOption(ann);


		Option md5 = OptionBuilder
			              .withDescription("generated MD5 checksums for reference contigs")
			              .withLongOpt("md5")
			              .create("md5");
		options.addOption(md5);


		Option optSortOrder = OptionBuilder
			              .withDescription("Sort order.  Can be one of: unsorted, queryname, coordinate.  Default:  coordinate")
			              .hasArg()
			              .withArgName("sort order")
			              .withLongOpt("sort-order")
			              .create("so");
		options.addOption(optSortOrder);

		Option as = OptionBuilder
			              .withDescription("Genome assembly identifier (@SQ AS:xxxx tag)")
			              .hasArg()
			              .withArgName("ASSEMBLY_ID")
			              .withLongOpt("sq-assembly")
			              .create("sqas");
		options.addOption(as);


		// read group options
		Map<String, Option> readGroupOptions = defineRGOptions();
		for (Option opt: readGroupOptions.values())
			options.addOption(opt);

		CommandLineParser parser = new GnuParser();

		try 
		{
			CommandLine line = parser.parse( options, args );

			if (line.hasOption(ref.getOpt()))
				userReferenceRoot = line.getOptionValue(ref.getOpt());

			if (line.hasOption(md5.getOpt()))
				generatedMd5 = true;

			if (line.hasOption(ann.getOpt()))
				userAnnotation = line.getOptionValue(ann.getOpt());

			if (line.hasOption(as.getOpt())) // TODO: validate this input
				genomeAssemblyId = line.getOptionValue(as.getOpt());

			if (line.hasOption(optSortOrder.getOpt()))
			{
				String value = line.getOptionValue(optSortOrder.getOpt());
				if ( "unordered".equals(value) || "queryname".equals(value) || "coordinate".equals(value) )
					sortOrder = value;
				else
					throw new ParseException("Invalid sort order.  Sort order must be one of: unsorted, queryname, coordinate.");
			}

			// remaining args
			String[] otherArgs = line.getArgs();
			if (otherArgs.length == 1 || otherArgs.length == 2) 
			{
				userInput = otherArgs[0];
				if (otherArgs.length == 1)
					userOutput = null;
				else
					userOutput = otherArgs[1];
			}
			else
				throw new ParseException("You must provide an HDFS input path and, optionally, an output path.");

			readGroupFields = parseReadGroupOptions(readGroupOptions, line);

			// option validation
			if (generatedMd5 && userReferenceRoot == null)
				throw new ParseException("You must specify the path the reference if you want to generate MD5 checksums");
			if (userReferenceRoot == null && userAnnotation == null)
				throw new ParseException("You must provide the path to the reference or at least its annotation file (<ref>.ann)");
		}
		catch( ParseException e ) 
		{
			System.err.println("Usage error: " + e.getMessage());
			// XXX: redirect System.out to System.err since the simple version of 
			// HelpFormatter.printHelp prints to System.out, and we're on a way to
			// a fatal exit.
			System.setOut(System.err);
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "MergeAlignments [options] -ann <ref>.ann <in> [<out>]", options);
			System.exit(1);
		}
	}

	private void configureMerge() throws ParseException, IOException
	{
		// convert user-provided paths to Path objects and make them qualified
		if (userAnnotation != null)
			annotationPath = getQualifiedPath(userAnnotation);

		if (userReferenceRoot != null)
		{
			referenceRootPath = getQualifiedPath(userReferenceRoot);

			if (annotationPath == null)
				annotationPath = referenceRootPath.suffix(".ann");
			else
			{
				if (!annotationPath.equals(referenceRootPath.suffix(".ann")))
					log.warn("specified annotation file does not follow the same naming pattern as the specified reference");
			}
		}

		// here we load the reference annotations
		loadAnnotations();
		log.info("Reference annotations read");
	}

	private void calculateChecksums() throws IOException
	{
		if (generatedMd5)
		{
			log.info("Calculating reference checksum...");
			log.info("Reference fasta path: " + referenceRootPath.toString());

			checksums = new FastaChecksummer();
			FileReader reader = new FileReader( new File(referenceRootPath.toUri()) );
			try {
				checksums.setInput(reader);
				checksums.calculate();
			}
			finally {
				reader.close();
			}

			log.info("checksum complete");
		}
	}

	private void loadAnnotations() throws IOException
	{
		FileSystem fs = annotationPath.getFileSystem(getConf());
		log.info("Reading reference annotations from " + annotationPath);
		try
		{
			FSDataInputStream in = fs.open(annotationPath);
			refAnnotation = new BwaRefAnnotation(new InputStreamReader(in));
		}
		catch (IOException e)
		{
			log.fatal("Can't read annotation file " + annotationPath + " on filesystem " + fs.getUri());
			throw e;
		}
	}

	private static class SourcePathFilter implements org.apache.hadoop.fs.PathFilter
	{
		public boolean accept(Path p)
		{
			boolean decision = true;
			if (p.getName().toString().startsWith("_"))
				decision = false;

			return decision;
		}
	}

	private Path[] getSourcePaths() throws Exception
	{
		Path srcPath = new Path(userInput);
    FileSystem srcFs = srcPath.getFileSystem(getConf());
		if (srcFs.exists(srcPath))
		{
			FileStatus stat = srcFs.getFileStatus(srcPath);
			if (stat.isDir())
			{
				String msg = "source path " + srcPath + " is a directory.  Globbing with ";
				srcPath = new Path(srcPath, "*");
				log.info(msg + srcPath);
			}
		}

		// Glob source path.  The returned paths are already sorted.  We filter out paths starting 
		// with '_' (see SourcePathFilter).
		// If the path doesn't contain a glob patter, and it doesn't exist, the function will return null.
		Path[] sources = FileUtil.stat2Paths(srcFs.globStatus(srcPath, new SourcePathFilter()));
		if (sources == null)
			throw new IllegalArgumentException("Source path " + srcPath.makeQualified(srcFs) + " doesn't exist");

		if (log.isDebugEnabled())
		{
			log.debug("Sources:");
			for (int i = 0; i < sources.length; ++i)
				log.debug(sources[i]);
		}

		if (sources.length == 0)
			throw new IllegalArgumentException("no source files selected");

		log.info("Merging " + sources.length + " files.");

		return sources;
	}

	private void writeSamHeader(OutputStream rawOut) throws IOException
	{
		Writer out = 
			new BufferedWriter(
					new OutputStreamWriter(rawOut));
		out.write("@HD\tVN:1.0\tSO:" + sortOrder + "\n");

		// prep @SQ format string
		StringBuilder formatBuilder = new StringBuilder(50);
		formatBuilder.append("@SQ\tSN:%s\tLN:%d");

		if (genomeAssemblyId != null)
			formatBuilder.append("\tAS:").append(genomeAssemblyId);
		if (referenceRootPath != null)
			formatBuilder.append("\tUR:").append(referenceRootPath.toUri());
		if (generatedMd5)
			formatBuilder.append("\tM5:%s");

		formatBuilder.append("\n");

		String format = formatBuilder.toString();
		if (generatedMd5)
		{
			for (BwaRefAnnotation.Contig c: refAnnotation)
				out.write( String.format(format, c.getName(), c.getLength(), checksums.getChecksum(c.getName())) );
		}
		else
		{
			for (BwaRefAnnotation.Contig c: refAnnotation)
				out.write( String.format(format, c.getName(), c.getLength()) );
		}
		
		// @PG:  Seal name and version
		String version = "not available";
		Package pkg = this.getClass().getPackage();
		if (pkg != null && pkg.getImplementationVersion() != null)
			version = pkg.getImplementationVersion();
		else
			log.warn("Could not get package version");

		out.write("@PG\tID:seal\tVN:" +  version + "\n");

		// @RG, if provided
		if (!readGroupFields.isEmpty())
		{
			StringBuilder rgBuilder = new StringBuilder("@RG");
			for (Map.Entry<String, String> pair: readGroupFields.entrySet())
				rgBuilder.append("\t").append(pair.getKey()).append(":").append(pair.getValue());
			rgBuilder.append("\n");

			out.write( rgBuilder.toString() );
		}

		out.flush();
	}

	private void copyMerge(Path[] sources, OutputStream out) throws IOException 
	{
		Configuration conf = getConf();
    
		for (int i = 0; i < sources.length; ++i)
		{
			FileSystem fs = sources[i].getFileSystem(conf);
			InputStream in = fs.open(sources[i]);
			try 
			{
				IOUtils.copyBytes(in, out, conf, false);
			}
			finally {
				in.close();
			} 
		}
	}


	/**
	 * Make an OutputStream to write to the user selected userOutput.
	 */
	private OutputStream getOutputStream() throws IOException
	{
		OutputStream out;
		if (userOutput == null) // Write to stdout
		{
			log.info("writing to standard out");
			out = System.out;
		}
		else
		{
			Path destPath = getQualifiedPath(userOutput);
			FileSystem destFs = destPath.getFileSystem(getConf());
			if (destFs.exists(destPath))
				throw new RuntimeException("Output destination " + destPath + " exists.  Please remove it or change the output path.");

			log.info("Merging sources to " + destPath);
			out = destFs.create(destPath);
		}
		return out;
	}

	public int run(String[] args) throws Exception
	{
		scanOptions(args);

		// get input paths.  Check for paths that don't exist.
		Path[] sources = getSourcePaths();
		// ensure that annotation options make sense and load the reference annotations
		configureMerge();
		// Get the output stream.  This make create a new output file.
		// Do this before calculateChecksums so that any errors wrt to the output
		// path are found before spending minutes checksumming the reference.
		OutputStream destFile = getOutputStream();

		try
		{
			// calculate the reference checksums, if necessary
			calculateChecksums();
			writeSamHeader(destFile);
			copyMerge(sources, destFile);
		}
		finally {
			destFile.close();
		}
		log.info("Finished");

		return 0;
	}

	public static void main(String[] args)
	{
		int res = 0;
		try
		{
			res = new SealToolRunner().run(new MergeAlignments(), args);
		}
		catch (Exception e)
		{
			System.err.println("Error executing MergeAlignments: " + e.getMessage());
			res = 1;
		}
		System.exit(res);
	}
}
