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

import it.crs4.seal.common.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TestUtils
{
	@Test
	public void testSanitizeFilename()
	{
		String name;

		name = "my_name";
		assertEquals(name, Utils.sanitizeFilename(name));

		name = "my.name";
		assertEquals(name, Utils.sanitizeFilename(name));

		name = "my-name";
		assertEquals(name, Utils.sanitizeFilename(name));

		name = "my/name";
		assertEquals("my_name", Utils.sanitizeFilename(name));

		name = "my name";
		assertEquals("my_name", Utils.sanitizeFilename(name));

		name = "my!@#$%^&*()\\name";
		assertEquals("my___________name", Utils.sanitizeFilename(name));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testSanitizeFilenameEmpty()
	{
		Utils.sanitizeFilename("");
	}

	@Test
	public void testGetTaskAttemptContext() {
		TaskAttemptContext tac = Utils.getTaskAttemptContext(new Configuration());
		assertNotNull(tac);
	}


	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main(TestUtils.class.getName());
	}
}
