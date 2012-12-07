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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.Collections;

public class BarcodeLookup
{
	// defaults
	protected static final int InitNumLanes = 8;
	protected static final int InitNumIndexTags = 12;
	protected static final int MaxReasonableMismatches = 10;

	// protected fields
	/*
	 * The table an ArrayList, where value at i corresponds to lane i+1.
	 * Each position contains a HashMap that maps DNA tags to Match objects, for that lane.
	 */
	protected ArrayList< HashMap<String, Match> > table;
	/*
	 * Some lanes only contain one sample.  For these lanes, getSampleId() will
	 * always return the only Match object available.  Since getting it out of
	 * the HashMap in the table is a little contrived, we cache those in the
	 * following array and return them when needed.  For lanes that are empty or
	 * contain more than one sample the loneSampleCache contains null.
	 */
	protected ArrayList< Match > loneSampleCache;
	protected int nSamples = 0;
	protected SubstitutionGenerator gen = new SubstitutionGenerator();

	public BarcodeLookup()
	{
		// Create an empty table for consistency.  It will be trashed when
		// a samplesheet is loaded.
		table = new ArrayList< HashMap<String, Match> >(0);
	}

	public BarcodeLookup(SampleSheet sheet, int maxMismatches)
	{
		load(sheet, maxMismatches);
	}

	public void load(SampleSheet sheet, int maxMismatches)
	{
		if (maxMismatches < 0)
			throw new IllegalArgumentException("maximum number of acceptable mismatches must not be negative (got " + maxMismatches + ")");
		if (maxMismatches > MaxReasonableMismatches)
		{
			throw new IllegalArgumentException("maximum number of acceptable mismatches is too high (got " + maxMismatches +
					", limit set to " + MaxReasonableMismatches +
					"). If you need to change the limit modify the value of MaxReasonableMismatches in the code and recompile.");
		}

		// Determine which lanes contain more than one sample.
		ArrayList<Integer> laneSampleCount = getLaneSampleCount(sheet);

		// now construct the barcode table
		table = new ArrayList< HashMap<String, Match> >(0);
		nSamples = 0;
		for (SampleSheet.Entry e: sheet)
		{
			// for lanes that only have one sample, only insert the exact match
			if (laneSampleCount.get(e.getLane() - 1) <= 1)
				insertRecord(e, 0);
			else
				insertRecord(e, maxMismatches);
		}

		// cache Match objects for lanes with only one sample.
		// Instantiate the cache array and fill with nulls.
		loneSampleCache = new ArrayList<Match>(laneSampleCount.size());
		loneSampleCache.addAll(Collections.nCopies(laneSampleCount.size(), (Match)null));

		for (int index = 0; index < table.size(); ++index)
		{
			if (table.get(index).size() == 1)
				loneSampleCache.set(index, table.get(index).values().iterator().next());
		}
	}

	/**
	 * Get the sample id corresponding to the given lane and index sequence.
	 *
	 * If this BarcodeLookup was loaded specifying a mismatch limit, this method
	 * will apply that setting and return a sample id that is within the given
	 * number of mismatches.
	 *
	 * In addition,
	 * <b>if in the loaded sample sheet a sample appears alone in a lane, it will
	 * be returned for any query on that lane (regardless of index sequence).
	 * For such queries the returned Match object will specify 0 mismatches</b>.
	 */
	public Match getSampleId(int lane, String indexSeq)
	{
		if (lane <= 0)
			throw new IllegalArgumentException("Invalid negative lane number " + lane);
		if (indexSeq.isEmpty())
			throw new IllegalArgumentException("Invalid blank index");

		// turn tag to uppercase
		indexSeq = indexSeq.toUpperCase();

		int index = lane - 1;
		if (index < table.size())
		{
			Map<String, Match> map = table.get(index);
			if (map.size() == 1)
				return loneSampleCache.get(index);
			else
				return map.get(indexSeq); // will return null if the indexSeq isn't in the Map
		}
		else
			return null;
	}

	/**
	 * Count the number of samples in each lane.
	 *
	 * @return An array where element i corresponds to lane i+1.
	 */
	protected static ArrayList<Integer> getLaneSampleCount(SampleSheet sheet)
	{
		if (sheet.isEmpty())
			return new ArrayList<Integer>(0);

		// Since we can't know the number of lanes without scanning the
		// sample sheet, we allocate the upper bound--i.e., the number
		// of entries in the sample sheet.
		ArrayList<Integer> laneSampleCount = new ArrayList<Integer>(sheet.size());

		for (SampleSheet.Entry e: sheet)
		{
			int index = e.getLane() - 1;
			if (index >= laneSampleCount.size())
				laneSampleCount.addAll(Collections.nCopies(index - laneSampleCount.size() + 1, 0));
			laneSampleCount.set(index, laneSampleCount.get(index) + 1);
		}

		return laneSampleCount;
	}

	public int getNumSamples() { return nSamples; }
	public boolean isEmpty() { return nSamples == 0; }

	public static class Match
	{
		private SampleSheet.Entry entry;
		private int mismatches;

		public Match(SampleSheet.Entry e, int mismatches)
		{
			entry = e;
			this.mismatches = mismatches;
		}

		public SampleSheet.Entry getEntry() { return entry; }

		public int getMismatches() { return mismatches; }

		public String toString() {
			if (entry == null)
				return "(NULL,)";
			else
				return "(" + entry.getSampleId() + "," + mismatches + ")";
		}
	}

	protected void insertRecord(SampleSheet.Entry entry, int maxMismatches)
	{
		int lane = entry.getLane();

		// grow the array if necessary for this entry's lane
		int index = lane - 1;
		if (table.size() < lane)
		{
			int grow_by = lane - table.size();
			for (int i = 0; i < grow_by; ++i)
				table.add(new HashMap<String, Match>(InitNumIndexTags));
		}

		gen.insertSubstitutions(entry, maxMismatches, table.get(index));

		// and finally insert
		nSamples += 1;
	}

	/**
	 * Implements a DFS that generates tags with substitution errors.
	 */
	protected static class SubstitutionGenerator
	{
		private static final char[] Alphabet = { 'A', 'C', 'G', 'T', 'N' };
		/**
		 * Limit on the size of the tag we're willing to handle with this method.
		 * The number of variants we generate goes up exponentially with the size
		 * of the tag.
		 * 4 substitutions ^ 8 tag bases * 8 bytes = 524288 bytes, just for string data.
		 * 9 tag bases gives 2359296 bytes.
		 */
		private static final int MaxTagLength = 8;

		/* Search state variables, set by genSubstitutions(), used as "globals" in generate()
		 * and then reset to null before genSubstitutions exits.
		 */
		/* Entry on which we're working */
		private SampleSheet.Entry entry;
		/* original Tag from Entry, cached as a character array */
		private char[] originalTag;
		/* current tag with substitutions (state of the DFS) */
		private char[] alteredTag;
		/* */
		private Map<String, Match> results;
		/* match objects are cached and reused, so all altered tags for the same entry with the
		 * same number of mismatches use the same Match object.
		 */
		private List<Match> matches;

		public void insertSubstitutions(SampleSheet.Entry e, int maxSubstitutions, Map<String, Match> resultMap)
		{
			String tag = e.getIndex();
			if (maxSubstitutions >= tag.length())
			{
				throw new IllegalArgumentException("Maximum number of allowed substitutions is too big.  (requested " +
						maxSubstitutions + " for a barcode " + tag + " of length " + tag.length() + ")");
			}
			if (maxSubstitutions < 0)
				throw new IllegalArgumentException("maxSubstitutions must be greater than or equal to zero (got " + maxSubstitutions + ")");

			if (tag.length() > MaxTagLength)
				throw new RuntimeException("tag length of " + tag.length() + " is above the compile-time limit of " + MaxTagLength + " bases");

			try {
				matches = new ArrayList<Match>(5);
				entry = e;
				alteredTag = tag.toCharArray();
				originalTag = tag.toCharArray();
				results = resultMap;
				insertMatch(tag, 0); // insert original tag
				// then generate substitutions
				if (maxSubstitutions > 0)
					generate(0, 0, maxSubstitutions);
			}
			finally {
				matches = null;
				entry = null;
				alteredTag = null;
				originalTag = null;
				results = null;
			}
		}

		private void insertMatch(String tag, int numMismatches)
		{
			// check for duplicates
			Match previous = results.get(tag);
			if (previous != null)
			{
				throw new RuntimeException(
						"Mismatch limit is too high.  " + numMismatches + " mismatches with barcode " +
						entry.getIndex() + " can result in barcode " + tag +
						", which conflicts with barcode " + previous.getEntry().getIndex() + " with " +
						previous.getMismatches() + " mismatches");
			}

			if (numMismatches >= matches.size())
			{
				// If necessary, create a new Match object for this Entry with this number of mismatches.
				// The Match object will be cached in the matches List.
				for (int m = matches.size(); m <= numMismatches; ++m)
					matches.add(new Match(entry, m));
			}

			results.put(tag, matches.get(numMismatches));
		}

		/**
		 * Recursive method to generate tags with substitution errors.
		 * The method implements a depth-first traversal of a tree, rooted at the original
		 * tag, where each branch is a different substitution applied to the root of the branch.
		 * Therefore, the level is equal to the number of substitutions applied to the tag.
		 *
		 * The state of the traversal is kept in this object's instance variables.
		 *
		 * @param startPos Position within alteredTag at which to start inserting errors.
		 * @param subsDone Number of substitutions already inserted, to be compared to maxSubstitutions.
		 * @param maxSubstitutions Limit to subsDone. This private function takes for granted that
		 * the caller has verified that maxSubstitutions is less than or equal to originalTag.length.
		 *
		 * @return Tags with substitutions are appended to this.resultList
		 */
		private void generate(int startPos, int subsDone, int maxSubstitutions)
		{
			subsDone += 1; // with this call to generate, we'll have done subsDone+1 substitutions to the tag
			// generate another substitution.  Start from startPos and onwards
			for (int pos = startPos; pos < originalTag.length; ++pos)
			{
				for (int sub = 0; sub < Alphabet.length; ++sub)
				{
					if (originalTag[pos] != Alphabet[sub]) // don't substitute with itself
					{
						alteredTag[pos] = Alphabet[sub];
						insertMatch(new String(alteredTag), subsDone);
						if (subsDone < maxSubstitutions)
							generate(pos+1, subsDone, maxSubstitutions);
						// restore original value at this position as we back out of this branch of the substitution tree
						alteredTag[pos] = originalTag[pos];
					}
				}
			}
		}
	}
}
