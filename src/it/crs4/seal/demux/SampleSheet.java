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

package it.crs4.seal.demux;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Comparator;
import java.util.Collection;
import java.util.Collections;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.IOException;
import java.util.regex.*;

public class SampleSheet implements Iterable<SampleSheet.Entry>
{
	public static class FormatException extends Exception {
		private static final long serialVersionUID = 1L;
		public FormatException(String msg) {
			super(msg);
		}
	}

	// defaults
	private static final int InitNumEntries = 100;
	private static final int BAR_CODE_LENGTH = 6;
	private static final Pattern QuotePattern = Pattern.compile("^\"|\"$");
	private static final String ExpectedHeading = "\"FCID\",\"Lane\",\"SampleID\",\"SampleRef\",\"Index\",\"Description\",\"Control\",\"Recipe\",\"Operator\"";
	// private fields
	/* The table an ArrayList, where value at i corresponds to lane i+1.
	 * Each position contains a HashMap that maps DNA tags to sample names, for that lane.
	 */
	private ArrayList<Entry> table;
	private Matcher quoteMatcher = QuotePattern.matcher("");

	public SampleSheet()
	{
	 // Create an empty table for consistency.  It will be trashed when
	 // a file is loaded.
		table = new ArrayList<Entry>(0);
 	}

	public SampleSheet(Reader in) throws IOException, FormatException
	{
		loadTable(in);
	}

	public void loadTable(Reader in) throws IOException, FormatException
	{
		table = new ArrayList<Entry>(InitNumEntries);

		String line = null;
		LineNumberReader input = new LineNumberReader(in);

		line = input.readLine();
		if (line == null)
			throw new FormatException("Empty sample sheet");
		if (!line.startsWith(ExpectedHeading))
			throw new FormatException("Unexpected heading!  Expected:\n" + ExpectedHeading + "\nFound:\n" + line);

		line = input.readLine();
		while (line != null)
		{
			insertRecord(line);
			line = input.readLine();
		}

		if (table.size() > 1)
		{
			// Check for duplicates barcodes in the same lane
			// start by sorting the table by lane
			Collections.sort(table, new Comparator<Entry>() {
				@Override
				public int compare(Entry a, Entry b) {
					return a.getLane() - b.getLane();
				}
			});
			HashSet<String> samplesInLane = new HashSet<String>();
			int currentLane = table.get(0).getLane();
			for (Entry e: table) // table is an ArrayList of Entries
			{
				if (e.getLane() == currentLane)
				{
					if (samplesInLane.contains(e.getIndex()))
						throw new FormatException("index " + e.getIndex() + " appears twice for the same lane " + currentLane);
					else
						samplesInLane.add(e.getIndex());
				}
				else
				{
					// lane change
					samplesInLane.clear();
					samplesInLane.add(e.getIndex());
					currentLane = e.getLane();
				}
			}
		}
	}

	private void insertRecord(String line) throws FormatException
	{
		String[] fields = line.split(",");
		if (fields.length < 9)
			throw new FormatException("Too few columns in sample sheet.  Expecing at least 9 but found " + fields.length + ". Line: " + line);

		// Format is CSV with these columns:  "FCID","Lane","SampleID","SampleRef","Index","Description","Control","Recipe","Operator"
		// All text fields are quoted (all except Lane)

		// remove external quotes from all string fields
		quoteMatcher.reset(fields[0]);
		fields[0] = quoteMatcher.replaceAll("");
		for (int i = 2; i < fields.length; ++i)
		{
			quoteMatcher.reset(fields[i]);
			fields[i] = quoteMatcher.replaceAll("");
		}

		Entry entry;
		try {
			entry = Entry.createEntry(fields);
		}
		catch (IllegalArgumentException e) {
			throw new FormatException(e.getMessage() + ". Line: " + line);
		}

		table.add(entry);
	}

	public Set<String> getSamplesInLane(int lane)
	{
		if (lane <= 0)
			throw new IllegalArgumentException("Invalid negative lane number " + lane);

		HashSet<String> samples = new HashSet<String>(table.size() / 8);
		for (Entry e: this)
		{
			if (lane == e.getLane())
				samples.add(e.getSampleId());
		}

		return samples;
	}

	public Set<String> getSamples()
	{
		HashSet<String> uniqueSamples = new HashSet<String>(table.size());

		for (Entry e: table)
			uniqueSamples.add(e.getSampleId());

		return uniqueSamples;
	}

	public int size() { return table.size(); }
	public boolean isEmpty() { return table.isEmpty(); }

	public Iterator<Entry> iterator() { return new SIterator(table); }

	/**
	 * Decorates our Entry table's iterator to disable the remove() method.
	 */
	protected static class SIterator implements Iterator<Entry> {
		private Iterator<Entry> base;
		public SIterator(List<Entry> table) {
			base = table.iterator();
		}

		public boolean hasNext() { return base.hasNext(); }
		public Entry next() { return base.next(); }
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	public static class Entry
	{
		private String flowcellId;
		private int lane;
		private String sampleId;
		private String sampleRef;
		private String index;
		private String description;
		private String control;
		private String recipe;
		private String operator;

		public String getFlowcellId() { return flowcellId; }
		public int getLane() { return lane; }
		public String getSampleId() { return sampleId; }
		public String getSampleRef() { return sampleRef; }
		public String getIndex() { return index; }
		public String getDescription() { return description; }
		public String getControl() { return control; }
		public String getRecipe() { return recipe; }
		public String getOperator() { return operator; }

		public String toString() {
			StringBuilder builder = new StringBuilder(150);
			builder
				.append(flowcellId).append(",")
				.append(lane).append(",")
				.append(sampleId).append(",")
				.append(sampleRef).append(",")
				.append(index).append(",")
				.append(description).append(",")
				.append(control).append(",")
				.append(recipe).append(",")
				.append(operator);
			return builder.toString();
		}

		public static Entry createEntry(String[] fields)
		{
			if (fields.length < 9)
				throw new IllegalArgumentException("Too few fields to build a sample sheet entry.  Expecing at least 9 but found " + fields.length + ".");

			Entry e = new Entry();
			e.setFlowcellId(fields[0]);
			e.setLane(Integer.parseInt(fields[1]));
			e.setSampleId(fields[2]);
			e.setSampleRef(fields[3]);
			e.setIndex(fields[4]);
			e.setDescription(fields[5]);
			e.setControl(fields[6]);
			e.setRecipe(fields[7]);
			e.setOperator(fields[8]);
			return e;
		}

		protected void setFlowcellId(String v) { flowcellId = v; }

		protected void setLane(int v) {
			if (v <= 0)
				throw new IllegalArgumentException("Invalid lane number: " + lane + ". Expecting a number > 0");
			lane = v;
		}

		protected void setSampleId(String v) { sampleId = v; }

		protected void setSampleRef(String v) { sampleRef = v; }

		protected void setIndex(String v) {
			if (v != null)
			{
				if (v.isEmpty())
					throw new IllegalArgumentException("Invalid blank bar code sequence");
				if (v.length() != BAR_CODE_LENGTH)
					throw new IllegalArgumentException("Unexpected length for bar code sequence '" + v + "' (length " + v.length() + ", expected " + BAR_CODE_LENGTH + ")");

				index = v.toUpperCase();
			}
			else
				index = v;
		}

		protected void setDescription(String v) { description = v; }

		protected void setControl(String v) { control = v; }

		protected void setRecipe(String v) { recipe = v; }

		protected void setOperator(String v) { operator = v; }
	}
}
