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

package it.crs4.seal.demux;

import it.crs4.seal.demux.SampleSheet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.InputStreamReader;
import java.io.IOException;

public class DemuxUtils
{
	/**
	 * Class cannot be instantiated.
	 */
	private DemuxUtils() {}

	public static SampleSheet loadSampleSheet(Path qualifiedPath, Configuration conf) throws IOException, SampleSheet.FormatException
	{
		// load the sample sheet
		FileSystem fs = qualifiedPath.getFileSystem(conf);
		FSDataInputStream dstream = fs.open(qualifiedPath);
		InputStreamReader istream = new InputStreamReader(dstream);

		SampleSheet sheet = new SampleSheet();
		sheet.loadTable(istream);

		istream.close();
		dstream.close();

		return sheet;
	}
}
