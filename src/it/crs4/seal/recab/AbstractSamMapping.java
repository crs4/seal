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

package it.crs4.seal.recab;

import it.crs4.seal.common.FormatException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.*;

public abstract class AbstractSamMapping implements ReadableSeqMapping
{
	protected enum TagDataType {
		Char,
		String,
		Int,
		Float,
		NumArray,
		Bytes;

		public static TagDataType fromSamType(char t)
		{
			switch (t)
			{
				case 'A':
					return TagDataType.Char;
				case 'i':
					return TagDataType.Int;
				case 'f':
					return TagDataType.Float;
				case 'Z':
					return TagDataType.String;
				case 'H':
					return TagDataType.Bytes;
				case 'B':
					return TagDataType.NumArray;
				default:
					throw new IllegalArgumentException("Unknown tag type " + t);
			}
		}
	};

	protected class TagCacheItem {
		private TagDataType type;
		private String value;

		public TagCacheItem(TagDataType type, String value) {
			this.type = type;
			this.value = value;
		}

		public String getValue() { return value; }
		public TagDataType getType() { return type; }
		public String toString() { return "(" + type + "," + value + ")"; }
	}

	////////////////////////////////////////////////
	// variables
	////////////////////////////////////////////////
	protected static final Pattern CigarElementPattern = Pattern.compile("(\\d+)([MIDNSHP])");
	protected ArrayList<AlignOp> alignment;

	protected HashMap<String, TagCacheItem> tagCache;

	public AbstractSamMapping() 
	{
		tagCache = new HashMap<String, TagCacheItem>();
	}

	////////////////////////////////////////////////
	// methods
	////////////////////////////////////////////////

	public List<AlignOp> getAlignment()
	{
		// scan the CIGAR string and cache the results
		if (alignment == null)
		{
			ArrayList<AlignOp> result = new ArrayList<AlignOp>(5);
			String cigar = getCigarStr();
			Matcher m = CigarElementPattern.matcher(cigar);

			int lastPositionMatched = 0;
			while (m.find())
			{
				result.add( new AlignOp(AlignOp.AlignOpType.fromSymbol(m.group(2)), Integer.parseInt(m.group(1))) );
				lastPositionMatched = m.end();
			}

			if (lastPositionMatched < cigar.length())
				throw new FormatException("Invalid CIGAR pattern " + getCigarStr());

			// cache result
			alignment = result;
		}
		return alignment;
	}

	/**
	 * Calculate the reference coordinate for each matched position in this mapping.
	 * Writes getLength() integers into dest.  Non-matched positions (AlignOp != Match)
	 * are set to -1, while the rest are set to the reference position to which
	 * the base at the corresponding position was matched.
	 *
	 * This method relies on the MD tag.
	 */
	public void calculateRefCoordinates(ArrayList<Integer> dest)
	{
		/*
		dest.clear();
		List<AlignOp> alignment = getAlignment();
		String md = getTag("MD");
		*/

	}

	protected void processMDSequence()
	{

	}

	//////////////////////// tag methods ////////////////////////
	/**
	 * Get the entire text related to tag.
	 * For instance, given a SAM record with tags
	 * ...XT:A:U	NM:i:0	SM:i:37	AM:i:0	X0:i:1...
	 * getTagText("NM") should return "NM:i:0"
	 * while
	 * getTagText("XX") should return null.
	 * @return null if the tag isn't found.  The tag's text otherwise.
	 */
	abstract protected String getTagText(String name);

	/**
	 * Retrieve a TagCacheItem for the named tag.
	 * Checks the cache and retrieves the item from the cache if it's there.  If 
	 * it's not, it calls getTagText to retrieve it, scans it creating a 
	 * TagCacheItem which it caches and returns to the caller.
	 * @return a TagCacheItem for the given tag name.  The returned object is the one in this object's cache.
	 * @exception NoSuchFieldException Tag not found in mapping record.
	 * @exception FormatException Invalid SAM tag syntax.
	 */
	protected TagCacheItem getTagItem(String name) throws NoSuchFieldException
	{
		TagCacheItem item = tagCache.get(name);
		if (item == null)
		{
			String text = getTagText(name);
			if (text == null)
				throw new NoSuchFieldException("no tag with name " + name);
			String[] fields = text.split(":", 3);
			if (fields.length < 3)
				throw new FormatException("Invalid SAM tag syntax " + text);
			if (fields[1].length() != 1)
				throw new FormatException("Invalid SAM tag type syntax: " + text);

			item = new TagCacheItem(TagDataType.fromSamType(fields[1].charAt(0)), fields[2]);
			tagCache.put(name, item);
		}
		return item;
	}

	public String getTag(String name) throws NoSuchFieldException
	{
		return getTagItem(name).getValue();
	}

	public boolean hasTag(String name)
	{
		try {
			getTagItem(name);
			return true;
		}
		catch (NoSuchFieldException e) {
			return false;
		}
	}

	public int getIntTag(String name) throws NoSuchFieldException
	{
		TagCacheItem item = getTagItem(name);
		if (item.getType() == TagDataType.Int)
			return Integer.parseInt(item.getValue());
		else
			throw new NumberFormatException("item " + item + " is not of integer type");
	}

	public double getDoubleTag(String name) throws NoSuchFieldException
	{
		TagCacheItem item = getTagItem(name);
		if (item.getType() == TagDataType.Float || item.getType() == TagDataType.Int)
			return Double.parseDouble(item.getValue());
		else
			throw new NumberFormatException("item " + item + " is not of double type");
	}

	//////////////////////// flag methods ////////////////////////

	public boolean isPaired() {
		return AlignFlags.Paired.is(getFlag());
	}

	public boolean isProperlyPaired() {
		return AlignFlags.ProperlyPaired.is(getFlag());
	}

	public boolean isMapped() {
		return AlignFlags.Unmapped.isNot(getFlag());
	}

	public boolean isUnmapped() {
		return AlignFlags.Unmapped.is(getFlag());
	}

	public boolean isMateMapped() {
		return AlignFlags.MateUnmapped.isNot(getFlag());
	}

	public boolean isMateUnmapped() {
		return AlignFlags.MateUnmapped.is(getFlag());
	}

	public boolean isOnReverse() {
		return AlignFlags.OnReverse.is(getFlag());
	}

	public boolean isMateOnReverse() {
		return AlignFlags.MateOnReverse.is(getFlag());
	}

	public boolean isRead1() {
		return AlignFlags.Read1.is(getFlag());
	}

	public boolean isRead2() {
		return AlignFlags.Read2.is(getFlag());
	}

	public boolean isSecondaryAlign() {
		return AlignFlags.SecondaryAlignment.is(getFlag());
	}

	public boolean isFailedQC() {
		return AlignFlags.FailedQC.is(getFlag());
	}

	public boolean isDuplicate() {
		return AlignFlags.Duplicate.is(getFlag());
	}
}
