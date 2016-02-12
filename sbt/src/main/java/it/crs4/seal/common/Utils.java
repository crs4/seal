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

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class Utils
{
	/**
	 * Offset by which Sanger-style ASCII-encoded quality scores are shifted.
	 */
	public static final int SANGER_OFFSET = 33;

	/**
	 * Maximum encodable quality score for Sanger Phred+33 encoded base qualities.
	 */
	public static final int SANGER_MAX = 62;

	/**
	 * Offset by which Illumina-style ASCII-encoded quality scores are shifted.
	 */
	public static final int ILLUMINA_OFFSET = 64;

	/**
	 * Maximum encodable quality score for Illumina Phred+64 encoded base qualities.
	 */
	public static final int ILLUMINA_MAX = 62;


	private Utils() {} // no instantiation

	/**
	 * Given a number of parameters, return the first that is not null.
	 * If all parameters are null then the result is null.
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Object> T firstNonNull(T... objects)
	{
		for (T o: objects)
		{
			if (o != null)
				return o;
		}
		return null;
	}

	/**
	 * Warn the user that he's using a deprecated property.
	 * If newProperty is provided, the message will suggest to the user to substitute uses
	 * of the deprecatedProperty with the new one.
	 */
	public static void deprecationWarning(Log log, String deprecatedProperty, String newProperty)
	{
		log.warn("Your configuration is using the deprecated property " + deprecatedProperty);
		if (newProperty == null)
			log.warn("You should update your configuration to avoid using it.  See the documentation for details.");
		else
			log.warn("You should update your configuration to replace it with " + newProperty + ". See the documentation for details.");
	}

	/**
	 * Check whether a deprecated property is used, and if so emit a warning.
	 * @param deprecatedProperty The name of the deprecated property.
	 * @param newProperty The name of the property that replaces the deprecated one, if any,
	 *        to write a suggestion to the user.
	 */
	public static void checkDeprecatedProp(Configuration conf, Log log, String deprecatedProperty, String newProperty)
	{
		if (conf.get(deprecatedProperty) != null)
			deprecationWarning(log, deprecatedProperty, newProperty);
	}

	/**
	 * Substitute any characters to be avoided in a file name with '_'.
	 */
	public static String sanitizeFilename(String name)
	{
		if (name.isEmpty())
			throw new IllegalArgumentException("Empty file name!");
		// replace all non-word characters (a word character is: [a-zA-Z_0-9]) except '.' and '-'
		return name.replaceAll("[\\W&&[^.-]]", "_");
	}

	/**
	 * Hide the version-agnostic way we instantiate a TaskAttemptContext.
	 *
	 * The shift from Hadoop 1 to Hadoop 2 has changed TaskAttemptContext from
	 * a class to an interface.  This creates an issue for us as we'd like to
	 * support both versions.  To support both versions, we instantiate a
	 * TaskAttemptContext through reflection and hide the mess in this method.
	 */
	public static TaskAttemptContext getTaskAttemptContext(Configuration conf) {
		final String[] knownImplementations  = new String[] {
			"org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl",
			"org.apache.hadoop.mapreduce.TaskAttemptContext"
		};

		for (String name: knownImplementations) {
			TaskAttemptContext tac = createTACImplementation(name, conf);
			if (tac != null)
				return tac;
			// else, try again
		}
		// If we're here, it means we couldn't instantiate a TaskAttemptContext.
		// Only only option now is to throw an exception
		throw new RuntimeException("It seems this software isn't compatible with your\n" +
				" version of Hadoop.  Please file a bug report.  Message:\n" +
				"Couldn't instantiate a TaskAttemptContext");
	}

	private static TaskAttemptContext createTACImplementation(String fullName, Configuration conf) {
		try {
			return (TaskAttemptContext) Class.forName(fullName).
				getConstructor(Configuration.class, TaskAttemptID.class).
				newInstance(conf, new TaskAttemptID());
		} catch (ClassNotFoundException e) {
		} catch (NoSuchMethodException e) {
		} catch (Exception e) {
			System.err.println("WARNING! Couldn't instantiate " + fullName + " though we found it. Exception: " + e);
		}
		return null;
	}


}
