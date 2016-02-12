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

package it.crs4.seal.common;

import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.QseqInputFormat;
import org.seqdoop.hadoop_bam.QseqOutputFormat;

import it.crs4.seal.common.BamInputFormat;
//import it.crs4.seal.common.BamOutputFormat;
import it.crs4.seal.common.SamInputFormat;
//import it.crs4.seal.common.SamOutputFormat;
import it.crs4.seal.prq.PrqOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Map;
import java.util.HashMap;
import java.util.NoSuchElementException;

public class FormatNameMap
{
	private static final Map<String, Class<? extends FileInputFormat<?,?>> > inputFormatMap =
		new HashMap<String, Class<? extends FileInputFormat<?,?>> >();
	private static final Map<String, Class<? extends FileOutputFormat<?,?>> > outputFormatMap =
		new HashMap<String, Class<? extends FileOutputFormat<?,?>> >();

	static {
		inputFormatMap.put("fastq", FastqInputFormat.class);
		inputFormatMap.put("qseq",  QseqInputFormat.class);
		inputFormatMap.put("sam",   SamInputFormat.class);
		inputFormatMap.put("bam",   BamInputFormat.class);

		outputFormatMap.put("fastq", FastqOutputFormat.class);
		outputFormatMap.put("qseq",  QseqOutputFormat.class);
		outputFormatMap.put("prq",   PrqOutputFormat.class);
		//outputFormatMap.put("sam", SamOutputFormat.class);
		//outputFormatMap.put("bam", BamOutputFormat.class);
	}

	public static Class<? extends FileOutputFormat<?,?>> getOutputFormat(String name)
		throws NoSuchElementException
	{
		Class<? extends FileOutputFormat<?,?> > theClass = outputFormatMap.get(name);
		if (theClass == null)
			throw new NoSuchElementException("Unrecognized output format name '" + name + "'");
		return theClass;
	}

	public static Class<? extends FileInputFormat<?,?>> getInputFormat(String name)
		throws NoSuchElementException
	{
		Class<? extends FileInputFormat<?,?> > theClass = inputFormatMap.get(name);
		if (theClass == null)
			throw new NoSuchElementException("Unrecognized input format name '" + name + "'");
		return theClass;
	}
}
