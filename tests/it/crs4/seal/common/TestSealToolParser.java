// Copyright (C) 2011 CRS4.
// 
// This file is part of ReadSort.
// 
// ReadSort is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
// 
// ReadSort is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
// for more details.
// 
// You should have received a copy of the GNU General Public License along
// with ReadSort.  If not, see <http://www.gnu.org/licenses/>.


package tests.it.crs4.seal.common;

import java.util.ArrayList;
import java.io.IOException;

import java.net.URI;
import java.io.File;

import org.junit.*;
//import org.junit.runners.Suite;
import static org.junit.Assert.*;

import it.crs4.seal.common.SealToolParser;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.cli.*;

public class TestSealToolParser {

	private Configuration conf;
	private Options emptyOpts;
	private Options someOpts;
	private Option test = OptionBuilder.hasArg().withArgName("value").create("t");
	private SealToolParser defaultparser;

	ArrayList<URI> inputFiles;
	URI outputFile;
	URI existingOutputFile;

	@Before
	public void setUp() throws IllegalArgumentException, IOException, SecurityException, java.net.URISyntaxException
	{
		conf = new Configuration();
		emptyOpts = new Options();
		someOpts = new Options();
		someOpts.addOption(test);
		defaultparser = new SealToolParser(null);

		// create temporary local input and output files
		inputFiles = new ArrayList<URI>();
		for (int i = 0; i < 5; ++i)
			inputFiles.add(File.createTempFile("input", "demux-test").toURI());

		outputFile = new URI("file:/tmp/this_file_cant_exist_heheheheheheheheeheheh");
		existingOutputFile = File.createTempFile("input", "demux-test").toURI();
	}

	@After
	public void tearDown() throws IllegalArgumentException, IOException, SecurityException
	{
		for (URI u: inputFiles)
			new File(u).delete();

		new File(existingOutputFile).delete();
	}

	@Test
	public void testConstructorNullOpts()
	{
		SealToolParser parser = new SealToolParser(null);
	}

	@Test
	public void testConstructorEmptyOpts()
	{
		SealToolParser parser = new SealToolParser(emptyOpts);
	}

	@Test
	public void testConstructorWithOpts()
	{
		SealToolParser parser = new SealToolParser(someOpts);
	}

	@Test(expected=ParseException.class)
	public void testParseNoArgs() throws ParseException, IOException
	{
		defaultparser.parseOptions(conf, new String[0]);
	}

	@Test(expected=ParseException.class)
	public void testParseOneArgs() throws ParseException, IOException
	{
		defaultparser.parseOptions(conf, new String[]{"one"});
	}

	@Test(expected=ParseException.class)
	public void testParseInputOutputNotExisting() throws ParseException, IOException
	{
		CommandLine line = defaultparser.parseOptions(conf, new String[]{ "input", "output" });
	}

	@Test
	public void testParseSimpleInputOutput() throws IllegalArgumentException, IOException, SecurityException, ParseException
	{
		CommandLine line = defaultparser.parseOptions(conf, new String[]{ inputFiles.get(0).toString(), outputFile.toString() });
		assertEquals(outputFile, defaultparser.getOutputPath().toUri());

		assertEquals(1, defaultparser.getNumInputPaths());

		URI[] parsedInputs = new URI[defaultparser.getNumInputPaths()];

		int i = 0;
		for (Path p: defaultparser.getInputPaths())
		{
			parsedInputs[i] = p.toUri();
			i += 1;
		}

		assertArrayEquals(new URI[] { inputFiles.get(0) }, parsedInputs);
	}

	@Test
	public void testParseManyInputsOutput() throws IllegalArgumentException, IOException, SecurityException, ParseException
	{
		ArrayList<String> args = new ArrayList<String>(10);
		for (URI u: inputFiles)
			args.add(u.toString());
		args.add(outputFile.toString());

		CommandLine line = defaultparser.parseOptions(conf, args.toArray(new String[]{}));
		assertEquals(outputFile, defaultparser.getOutputPath().toUri());

		assertEquals(inputFiles.size(), defaultparser.getNumInputPaths());

		URI[] parsedInputs = new URI[defaultparser.getNumInputPaths()];

		// copy the input paths to a URI array
		int i = 0;
		for (Path p: defaultparser.getInputPaths())
		{
			parsedInputs[i] = p.toUri();
			i += 1;
		}
		assertArrayEquals(inputFiles.toArray(), parsedInputs);
	}

	@Test
	public void testParseWithOpt() throws ParseException, IOException
	{
		SealToolParser parser = new SealToolParser(someOpts);
		CommandLine line = parser.parseOptions(conf, 
			new String[]{"-t", "myarg", inputFiles.get(0).toString(), outputFile.toString() });

		assertTrue( line.hasOption("t") );
		assertEquals("myarg", line.getOptionValue("t"));
		assertNotNull(parser.getOutputPath());
		assertEquals(outputFile, parser.getOutputPath().toUri());
		assertEquals(1, parser.getNumInputPaths());
	}

	@Test(expected=ParseException.class)
	public void testExitingOutput() throws ParseException, IOException
	{
		CommandLine line = defaultparser.parseOptions(conf, new String[]{ inputFiles.get(0).toString(), existingOutputFile.toString() });
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestSealToolParser.class.getName());
	}
}
