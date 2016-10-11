// Copyright (C) 2011-2016 CRS4.
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

import it.crs4.seal.common.CutText;
import org.apache.hadoop.io.Text;

public class TestCutText
{
	private Text record1 = new Text("field11");
	private Text record2 = new Text("field21 field22");
	private Text record3 = new Text("field31 field32 field33");
	private Text record4 = new Text("field41 field42 field43 field44");
	private Text record5 = new Text("field51 field52 field53 field54");

	private CutText scanner;

	@Test(expected=IllegalArgumentException.class)
  public void testConstructorNoColums()
	{
		new CutText(" ");
	}

	@Test(expected=IllegalArgumentException.class)
  public void testConstructorEmptyDelim()
	{
		new CutText("", 1, 2);
	}

	@Test(expected=IllegalArgumentException.class)
  public void testConstructorDuplicateCols()
	{
		new CutText(" ", 1, 1);
	}

	@Test(expected=IllegalArgumentException.class)
  public void testConstructorOutOfOrderCols()
	{
		new CutText(" ", 2, 1, 3);
	}

	@Test(expected=CutText.FormatException.class)
	public void testsScanMissingFields() throws CutText.FormatException
	{
		scanner = new CutText(" ", 1, 2);
		scanner.loadRecord(record1);
	}

	/*
	 * Changed CutText not check for this case.
	@Test(expected=RuntimeException.class)
	public void testsGetFieldNotInit() throws CutText.FormatException
	{
		new CutText(" ", 1, 2).getField(0);
	}

	@Test(expected=RuntimeException.class)
	public void testsGetFieldPosNotInit() throws CutText.FormatException
	{
		new CutText(" ", 1, 2).getFieldPos(0);
	}
	*/

	@Test(expected=ArrayIndexOutOfBoundsException.class)
	public void testsFieldOutOfBounds() throws CutText.FormatException
	{
		scanner = new CutText(" ", 0);
		scanner.loadRecord(record1);
		assertEquals("field11", scanner.getField(2));
	}

	@Test(expected=ArrayIndexOutOfBoundsException.class)
	public void testsFieldPosOutOfBounds() throws CutText.FormatException
	{
		scanner = new CutText(" ", 0);
		scanner.loadRecord(record1);
		assertEquals("field11", scanner.getFieldPos(2));
	}

	@Test
	public void testsScanZero() throws CutText.FormatException
	{
		scanner = new CutText(" ", 0);
		scanner.loadRecord(record1);
		assertEquals("field11", scanner.getField(0));
		assertEquals(0, scanner.getFieldPos(0));
	}

	@Test
	public void testsScanOne() throws CutText.FormatException
	{
		scanner = new CutText(" ", 0);
		scanner.loadRecord(record1);
		assertEquals("field11", scanner.getField(0));
		assertEquals(0, scanner.getFieldPos(0));
		scanner.loadRecord(record2);
		assertEquals("field21", scanner.getField(0));
		assertEquals(0, scanner.getFieldPos(0));

		assertEquals(1, scanner.getNumFields());
	}

	@Test
	public void testsScanTwo() throws CutText.FormatException
	{
		scanner = new CutText(" ", 1);
		scanner.loadRecord(record2);
		assertEquals("field22", scanner.getField(0));
		assertEquals(8, scanner.getFieldPos(0));

		assertEquals(1, scanner.getNumFields());
	}

	@Test
	public void testsScanThree() throws CutText.FormatException
	{
		scanner = new CutText(" ", 0, 1);
		scanner.loadRecord(record2);
		assertEquals("field21", scanner.getField(0));
		assertEquals("field22", scanner.getField(1));
		assertEquals(8, scanner.getFieldPos(1));
		scanner.loadRecord(record3);
		assertEquals("field31", scanner.getField(0));
		assertEquals("field32", scanner.getField(1));
		assertEquals(8, scanner.getFieldPos(1));

		assertEquals(2, scanner.getNumFields());
	}

	@Test
	public void testsScanFour() throws CutText.FormatException
	{
		scanner = new CutText(" ", 1, 3);
		scanner.loadRecord(record4);
		assertEquals("field42", scanner.getField(0));
		assertEquals("field44", scanner.getField(1));
		assertEquals(24, scanner.getFieldPos(1));
		scanner.loadRecord(record5);
		assertEquals("field52", scanner.getField(0));
		assertEquals("field54", scanner.getField(1));
		assertEquals(24, scanner.getFieldPos(1));

		assertEquals(2, scanner.getNumFields());
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestCutText.class.getName());
	}
}
