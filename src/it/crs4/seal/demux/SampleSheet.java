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
import java.util.EnumMap;
import java.util.EnumSet;
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

	/** Expected length of tag sequence. */
	public static final int BAR_CODE_LENGTH = 6;

	// defaults
	private static final int InitNumEntries = 100;
	private static final Pattern QuotePattern = Pattern.compile("^\"|\"$");
	private static final String ExpectedHeading = "fcid,lane,sampleid,sampleref,index,description,control,recipe,operator";

	private enum Heading {
		fcid,
		lane,
		sampleid,
		sampleref,
		index,
		description,
		control,
		recipe,
		operator,
		sampleproject;
	}

	private static final EnumSet<Heading> RequiredColumns;
	static {
		RequiredColumns = EnumSet.of( Heading.lane, Heading.sampleid, Heading.index );
	}

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

	/**
	 * Scans the heading line and returns a Map from (normalized) column name to index.
	 *
	 * @exception FormatException Thrown if the heading line doesn't contain the required columns.
	 */
	private static EnumMap<Heading, Integer> getColumnIndices(String headingLine) throws FormatException
	{
		// Verify that heading is as expected, ignoring quotes and case
		String[] headingStrings = headingLine.replaceAll("\"", "").split(",");

		if (headingStrings.length <= 1)
			throw new FormatException("Bad sample sheet format.  Expecting a heading such as " + ExpectedHeading);

		EnumMap<Heading, Integer> columns = new EnumMap<Heading, Integer>(Heading.class);
		for (int idx = 0; idx < headingStrings.length; ++idx)
		{
			try {
				Heading h = Heading.valueOf(headingStrings[idx].toLowerCase());
				columns.put(h, idx);
			}
			catch (IllegalArgumentException e) {
				throw new FormatException("Unrecognized sample sheet heading '" + headingStrings[idx] + "'");
			}
		}

		if (!columns.keySet().containsAll(RequiredColumns))
		{
			EnumSet<Heading> missingColumns = RequiredColumns.clone();
			missingColumns.removeAll(columns.keySet());
			throw new FormatException("sample sheet is missing required columns " + missingColumns.toString());
		}

		return columns;
	}

	public void loadTable(Reader in) throws IOException, FormatException
	{
		table = new ArrayList<Entry>(InitNumEntries);

		String line = null;
		LineNumberReader input = new LineNumberReader(in);

		line = input.readLine(); // First line.  Should be the table header.
		if (line == null)
			throw new FormatException("Empty sample sheet");

		EnumMap<Heading, Integer> columnMap = getColumnIndices(line);

		line = input.readLine();
		while (line != null)
		{
			insertRecord(columnMap, line);
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

	private void insertRecord(final EnumMap<Heading, Integer> columns, String line) throws FormatException
	{
		String[] fields = line.split(",");
		if (fields.length != columns.size())
			throw new FormatException("Number of fields in sample sheet row different from heading.  Expecing " + columns.size() + " fields but found " + fields.length + ". Line: " + line);

		// Format is CSV with at least the columns specified by RequiredColumns.
		// E.g., "FCID","Lane","SampleID","SampleRef","Index","Description","Control","Recipe","Operator"
		// All text fields are quoted (all except Lane)

		// remove external quotes and whitespace from all string fields (even spaces within the quotes)
		for (int i = 0; i < fields.length; ++i)
		{
			quoteMatcher.reset(fields[i]);
			fields[i] = quoteMatcher.replaceAll("").trim();
		}

		Entry entry;
		try {
			entry = Entry.createEntry(columns, fields);
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
		private String project;

		public String getFlowcellId() { return flowcellId; }
		public int getLane() { return lane; }
		public String getSampleId() { return sampleId; }
		public String getSampleRef() { return sampleRef; }
		public String getIndex() { return index; }
		public String getDescription() { return description; }
		public String getControl() { return control; }
		public String getRecipe() { return recipe; }
		public String getOperator() { return operator; }
		public String getProject() { return project; }

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
				.append(operator).append(",")
				.append(project);
			return builder.toString();
		}

		public static Entry createEntry(final EnumMap<Heading, Integer> columnIndices, String[] fields)
		{
			Entry e = new Entry();
			Integer idx;

			idx = columnIndices.get(Heading.fcid);
			e.setFlowcellId(  idx == null ? null : fields[idx] );

			idx = columnIndices.get(Heading.sampleid);
			e.setSampleId(    idx == null ? null : fields[idx] );

			idx = columnIndices.get(Heading.sampleref);
			e.setSampleRef(   idx == null ? null : fields[idx] );

			idx = columnIndices.get(Heading.index);
			e.setIndex(       idx == null ? null : fields[idx] );

			idx = columnIndices.get(Heading.description);
			e.setDescription( idx == null ? null : fields[idx] );

			idx = columnIndices.get(Heading.control);
			e.setControl(     idx == null ? null : fields[idx] );

			idx = columnIndices.get(Heading.recipe);
			e.setRecipe(      idx == null ? null : fields[idx] );

			idx = columnIndices.get(Heading.operator);
			e.setOperator(    idx == null ? null : fields[idx] );

			idx = columnIndices.get(Heading.lane);
			e.setLane(        idx == null ? null : Integer.parseInt( fields[idx]) );

			idx = columnIndices.get(Heading.sampleproject);
			e.setProject(     idx == null ? null : fields[idx] );

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
				if (!v.isEmpty() && v.length() != BAR_CODE_LENGTH)
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
		protected void setProject(String v) { project = v; }
	}
}
