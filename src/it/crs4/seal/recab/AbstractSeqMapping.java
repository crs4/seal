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

//import it.crs4.seal.recab.ReadableSeqMapping;

import java.util.ArrayList;
import java.util.regex.*;

public abstract class AbstractSeqMapping implements ReadableSeqMapping
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
			};
		}
	};

	protected class TagCacheItem {
		private TagCacheItem type;
		private String value;

		public TagCacheItem(TagDataType type, String value) {
			this.type = type;
			this.value = value;
		}

		public String getValue() { return value; }
		public TagDataType getType { return type; }
		public String toString() { return "(" + type + "," + value + ")"; }
	}

	////////////////////////////////////////////////
	// variables
	////////////////////////////////////////////////
	protected static final Pattern CigarElementPattern = Pattern.compile("(\\d+)(\\[MIDNSHP\\])");
	protected ArrayList<AlignOp> alignment;

	protected HashMap<String, TagCacheItem> tagCache;

	public AbstractSeqMapping() 
	{
		tagCache = new HashMap<String, TagCacheItem>();
	}

	////////////////////////////////////////////////
	// methods
	////////////////////////////////////////////////

	public ArrayList<AlignOp> getAlignment()
	{
		// scan the CIGAR string and cache the results
		if (alignment == null)
		{
			ArrayList<AlignOp> result = new ArrayList<AlignOp>(5);
			Matcher m = CigarElementPattern.matcher(getCigarStr());

			while (m.find())
				result.add( new AlignOp(AlignOp.AlignOpType.fromSymbol(m.group(2)), Integer.parseInt(m.group(1))) );

			if (!m.hitEnd())
				throw new FormatError("Invalid CIGAR pattern " + getCigarStr());
		}
		return alignment;
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

	protected TagCacheItem getTagItem(String name)
	{
		TagCacheItem item = tagCache.get(name);
		if (item == null)
		{
			String text = getTagText(name);
			if (text == null)
				throw new NoSuchFieldException("no tag with name " + name);
			String[] fields = text.split(":", 3);
			if (fields.length < 3)
				throw new FormatError("Invalid SAM tag syntax " + text);
			if (fields[1].length() != 1)
				throw new FormatError("Invalid SAM tag type syntax: " + text);

			item = new TagCacheItem(TagCacheItem.fromSamType(fields[1].charAt(0), fields[2]))
			tagCache.put(name, item);
		}
		return item;
	}

	public String getTag(String name)
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

	public int getIntTag(String name)
	{
		TagCacheItem item = getTagItem(name);
		if (item.getType() == TagDataType.Int)
			return Integer.parseInt(item.getValue());
		else
			throw new NumberFormatException("item " + item + " is not of integer type");
	}

	public double getDoubleTag(String name)
	{
		TagCacheItem item = getTagItem(name);
		if (item.getType() == TagDataType.Float)
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
