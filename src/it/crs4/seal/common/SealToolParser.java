// Copyright (C) 2011 CRS4.
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

package it.crs4.seal.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.cli.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

public class SealToolParser {

	private Options options;
	private Option opt_nReducers;
	private Integer nReducers;

	protected ArrayList<Path> inputs;
	private Path outputDir;

	public SealToolParser(Options moreOpts)
	{
		options = new Options(); // empty
		opt_nReducers = OptionBuilder
			              .withDescription("Number of reduce tasks to use.")
			              .hasArg()
			              .withArgName("INT")
										.withLongOpt("num-reducers")
			              .create("r");
		options.addOption(opt_nReducers);

		if (moreOpts != null)
		{
			for (Object opt: moreOpts.getOptions())
				options.addOption((Option)opt);
		}

		nReducers = null;
		inputs = new ArrayList<Path>(10);
		outputDir = null;
	}

	public CommandLine parseOptions(Configuration conf, String[] args) throws ParseException, IOException
	{
		// parse the command line
		CommandLine line = new GenericOptionsParser(conf, options, args).getCommandLine();

		////////////////////// number of reducers //////////////////////
		if (line.hasOption(opt_nReducers.getOpt()))
		{
			String rString = line.getOptionValue(opt_nReducers.getOpt());
			try
			{
				int r = Integer.parseInt(rString);
				if (r >= 0)
					nReducers = r;
				else
					throw new ParseException("Number of reducers must be greater than 0 (got " + rString + ")");
			}
			catch (NumberFormatException e)
			{
				throw new ParseException("Invalid number of reducers '" + rString + "'");
			}
		}

		////////////////////// positional arguments //////////////////////
		String[] otherArgs = line.getArgs();
		if (otherArgs.length < 2) // require at least two:  one input and one output
			throw new ParseException("You must provide input and output paths");
		else 
		{
			//
			FileSystem fs;
			for (int i = 0; i < otherArgs.length - 1; ++i) {
				Path p = new Path(otherArgs[i]);
				fs = p.getFileSystem(conf);
				p = p.makeQualified(fs);
				if (fs.exists(p))
					inputs.add(p);
				else
					throw new ParseException("Input path " + p.toString() + " doesn't exist");
			}
			// now the last one, should be the output path
			outputDir = new Path(otherArgs[otherArgs.length - 1]);
			fs = outputDir.getFileSystem(conf);
			outputDir = outputDir.makeQualified(fs);
			if (fs.exists(outputDir))
				throw new ParseException("Output path " + outputDir.toString() + " already exists.  Won't overwrite");
		}

		return line;
	}

	public Integer getNReducers() { return nReducers; }

	public Path getOutputPath()
	{
		return outputDir;
	}

	public static class InputPathList implements Iterable<Path> {
		private Iterator<Path> it;

		public InputPathList(Iterator<Path> i) {
			it = i;
		}

		public Iterator<Path> iterator() {
			return it;
		}
	}

	public InputPathList getInputPaths()
	{
		return new InputPathList(inputs.iterator());
	}

	public int getNumInputPaths()
	{
		return inputs.size();
	}

	public void defaultUsageError(String toolName) 
	{
		defaultUsageError(toolName, null);
	}

	/**
	 * Prints help and exits with code 3.
	 */
	public void defaultUsageError(String toolName, String msg) 
	{
		System.err.print("Usage error");
		if (msg != null)
			System.err.println(":  " + msg);
		System.err.print("\n");
		// XXX: redirect System.out to System.err since the simple version of 
		// HelpFormatter.printHelp prints to System.out, and we're on a way to
		// a fatal exit.
		System.setOut(System.err);
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("hadoop " + toolName + " [options] <in>+ <out>", options);
		System.exit(3);
	}
}
