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


package tests.it.crs4.seal.common;

import org.junit.*;
import static org.junit.Assert.*;

import it.crs4.seal.common.CutString;

public class TestCutString 
{
	private String record1 = "field11";
	private String record2 = "field21 field22";
	private String record3 = "field31 field32 field33";
	private String record4 = "field41 field42 field43 field44";
	private String record5 = "field51 field52 field53 field54";

	private CutString scanner;

	@Before
	public void setup()
	{
	}

	@Test(expected=IllegalArgumentException.class)
  public void testConstructorNoColums()
	{
		new CutString(" ");
	}

	@Test(expected=IllegalArgumentException.class)
  public void testConstructorEmptyDelim()
	{
		new CutString("", 1, 2);
	}

	@Test(expected=IllegalArgumentException.class)
  public void testConstructorDuplicateCols()
	{
		new CutString(" ", 1, 1);
	}

	@Test(expected=IllegalArgumentException.class)
  public void testConstructorOutOfOrderCols()
	{
		new CutString(" ", 2, 1, 3);
	}

	@Test(expected=CutString.FormatException.class)
	public void testsScanMissingFields() throws CutString.FormatException
	{
		scanner = new CutString(" ", 1, 2);
		scanner.loadRecord(record1);
	}

	@Test(expected=RuntimeException.class)
	public void testsGetFieldNotInit() throws CutString.FormatException
	{
		new CutString(" ", 1, 2).getField(0);
	}

	@Test(expected=ArrayIndexOutOfBoundsException.class)
	public void testsFieldOutOfBounds() throws CutString.FormatException
	{
		scanner = new CutString(" ", 0);
		scanner.loadRecord(record1);
		assertEquals("field11", scanner.getField(2));
	}

	@Test
	public void testsScanZero() throws CutString.FormatException
	{
		scanner = new CutString(" ", 0);
		scanner.loadRecord(record1);
		assertEquals("field11", scanner.getField(0));
	}

	@Test
	public void testsScanOne() throws CutString.FormatException
	{
		scanner = new CutString(" ", 0);
		scanner.loadRecord(record1);
		assertEquals("field11", scanner.getField(0));
		scanner.loadRecord(record2);
		assertEquals("field21", scanner.getField(0));
	}

	@Test
	public void testsScanTwo() throws CutString.FormatException
	{
		scanner = new CutString(" ", 1);
		scanner.loadRecord(record2);
		assertEquals("field22", scanner.getField(0));
	}

	@Test
	public void testsScanThree() throws CutString.FormatException
	{
		scanner = new CutString(" ", 0, 1);
		scanner.loadRecord(record2);
		assertEquals("field21", scanner.getField(0));
		assertEquals("field22", scanner.getField(1));
		scanner.loadRecord(record3);
		assertEquals("field31", scanner.getField(0));
		assertEquals("field32", scanner.getField(1));
	}

	@Test
	public void testsScanFour() throws CutString.FormatException
	{
		scanner = new CutString(" ", 1, 3);
		scanner.loadRecord(record4);
		assertEquals("field42", scanner.getField(0));
		assertEquals("field44", scanner.getField(1));
		scanner.loadRecord(record5);
		assertEquals("field52", scanner.getField(0));
		assertEquals("field54", scanner.getField(1));
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestCutString.class.getName());
	}
}
