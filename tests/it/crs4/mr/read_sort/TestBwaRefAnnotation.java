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


package tests.it.crs4.seal.read_sort;

import java.io.StringReader;

import org.junit.*;
//import org.junit.runners.Suite;
import static org.junit.Assert.*;

import it.crs4.seal.read_sort.BwaRefAnnotation;

public class TestBwaRefAnnotation {
	private String annotationSample;
	private StringReader sampleReader;

	private BwaRefAnnotation emptyAnnotation;
	private BwaRefAnnotation loadedAnnotation;

	@Before
	public void setUp() throws java.io.IOException
	{
		emptyAnnotation = new BwaRefAnnotation();

		annotationSample = 
		  "3080436051 6 11\n" +
		  "0 chr1 (null)\n" +
		  "0 247249719 39\n" +
		  "0 chr2 (null)\n" +
		  "247249719 242951149 25\n" +
		  "0 chr3 (null)\n" +
		  "490200868 199501827 10\n" +
		  "0 chr4 (null)\n" +
		  "689702695 191273063 14\n" +
		  "0 chr5 (null)\n" +
		  "880975758 180857866 7\n" +
		  "0 mycontig (null)\n" +
		  "1061833624 170899992 11\n";
		sampleReader = new StringReader(annotationSample);
		loadedAnnotation = new BwaRefAnnotation();
		loadedAnnotation.load(sampleReader);
	}

	@Test(expected=BwaRefAnnotation.InvalidAnnotationFormatException.class)
	public void testLoadEmpty() throws java.io.IOException
	{
		emptyAnnotation.load( new StringReader("") );
	}

	@Test(expected=BwaRefAnnotation.InvalidAnnotationFormatException.class)
	public void testNotEnoughContigs() throws java.io.IOException
	{
		String ann = 
		  "3080436051 6 11\n" +
		  "0 chr1 (null)\n" +
		  "0 247249719 39\n" +
		  "0 chr2 (null)\n" +
		  "247249719 242951149 25\n";
		emptyAnnotation.load( new StringReader(ann) );
	}

	@Test(expected=BwaRefAnnotation.InvalidAnnotationFormatException.class)
	public void testTooManyContigs() throws java.io.IOException
	{
		String ann = 
		  "3080436051 1 11\n" +
		  "0 chr1 (null)\n" +
		  "0 247249719 39\n" +
		  "0 chr2 (null)\n" +
		  "247249719 242951149 25\n";
		emptyAnnotation.load( new StringReader(ann) );
	}

	@Test(expected=BwaRefAnnotation.InvalidAnnotationFormatException.class)
	public void testIncompleteContigRecord() throws java.io.IOException
	{
		String ann = 
		  "3080436051 1 11\n" +
		  "0 chr1 (null)\n";
		emptyAnnotation.load( new StringReader(ann) );
	}

	@Test(expected=BwaRefAnnotation.InvalidAnnotationFormatException.class)
	public void testZeroContigs() throws java.io.IOException
	{
		String ann = 
		  "3080436051 0 11\n" +
		  "0 chr1 (null)\n" +
		  "0 247249719 39\n";
		emptyAnnotation.load( new StringReader(ann) );
	}

	@Test
	public void testGetReferenceLength()
	{
		assertEquals(3080436051L, loadedAnnotation.getReferenceLength());
	}

	@Test
	public void testGetContigId()
	{
		assertEquals(1, loadedAnnotation.getContigId("chr1"));
		assertEquals(2, loadedAnnotation.getContigId("chr2"));
		assertEquals(6, loadedAnnotation.getContigId("mycontig"));
	}

	@Test(expected=BwaRefAnnotation.UnknownContigException.class)
	public void testGetContigIdUnknownContig()
	{
		loadedAnnotation.getContigId("zanzan");
	}

	@Test
	public void testGetAbsCoord()
	{
		assertEquals(1L, loadedAnnotation.getAbsCoord("chr1", 1));
		assertEquals(247249720L, loadedAnnotation.getAbsCoord("chr2", 1));
		assertEquals(247249720L + 49, loadedAnnotation.getAbsCoord("chr2", 50));
	}

	@Test
	public void testEmptyIterator()
	{
		java.util.Iterator<BwaRefAnnotation.Contig> it = emptyAnnotation.iterator();
		assertFalse(it.hasNext());

		boolean iterated = false;
		for (BwaRefAnnotation.Contig c: emptyAnnotation)
			iterated = true;
		assertFalse(iterated);
	}

	@Test
	public void testIterator()
	{
		int count = 0;
		for (BwaRefAnnotation.Contig c: loadedAnnotation)
			++count;
		assertEquals(6, count);

		java.util.Iterator<BwaRefAnnotation.Contig> it = loadedAnnotation.iterator();
		BwaRefAnnotation.Contig c;

		c = it.next();
		assertEquals("chr1", c.getName());
		assertEquals(0L, c.getStart());
		assertEquals(247249719L, c.getLength());
		assertEquals(1, c.getId());

		c = it.next();
		assertEquals("chr2", c.getName());
		assertEquals(247249719L, c.getStart());
		assertEquals(242951149L , c.getLength());
		assertEquals(2, c.getId());

		c = it.next(); // 3
		c = it.next(); // 4
		c = it.next(); // 5
		c = it.next(); // mycontig
		assertEquals("mycontig", c.getName());
		assertEquals(1061833624L, c.getStart());
		assertEquals(170899992L, c.getLength());
		assertEquals(6, c.getId());

		assertFalse(it.hasNext());
	}

	@Test(expected=java.lang.IllegalStateException.class)
	public void testItThrowsOnRemove()
	{
		java.util.Iterator<BwaRefAnnotation.Contig> it = loadedAnnotation.iterator();
		it.remove();
	}

	@Test(expected=java.util.NoSuchElementException.class)
	public void testIteratorThrowsAtTheEnd()
	{
		java.util.Iterator<BwaRefAnnotation.Contig> it = emptyAnnotation.iterator();
		it.next();
	}

	@Test
	public void testNameLineWithManySpaces() throws java.io.IOException
	{
		annotationSample = 
		  "3080436051 1 11\n" +
			"0 GL000229.1 dna:supercontig supercontig::GL000229.1:1:19913:1\n" + 
			"3095693981 4262 0";
		sampleReader = new StringReader(annotationSample);
		loadedAnnotation.load(sampleReader);
		BwaRefAnnotation.Contig c = loadedAnnotation.getContig("GL000229.1");
		assertEquals(3095693981L, c.start);
		assertEquals("GL000229.1", c.name);
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main("tests.it.crs4.seal.read_sort.TestBwaRefAnnotation");
	}
}
