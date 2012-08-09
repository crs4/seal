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

import it.crs4.seal.common.IMRContext;
import it.crs4.seal.common.SequenceId;

import fi.tkk.ics.hadoop.bam.SequencedFragment;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

public class DemuxReducer
{
	private static final Log LOG = LogFactory.getLog(DemuxReducer.class);
	private BarcodeLookup barcodeLookup;
	private Text outputKey = new Text();

	public void setup(String localSampleSheetPath, Configuration conf) throws IOException
	{
		// load the sample sheet
		Path path = new Path(localSampleSheetPath).makeQualified(FileSystem.getLocal(conf));
		SampleSheet sampleSheet;
		try {
			sampleSheet = DemuxUtils.loadSampleSheet(path, conf);
		}
		catch (SampleSheet.FormatException e) {
			throw new RuntimeException("Error loading sample sheet.  Message: " + e.getMessage());
		}
		barcodeLookup = new BarcodeLookup(sampleSheet, conf.getInt(Demux.CONF_MAX_MISMATCHES, Demux.DEFAULT_MAX_MISMATCHES));
	}

	public void reduce(SequenceId key, Iterable<SequencedFragment> sequences, IMRContext<Text,SequencedFragment> context) throws IOException, InterruptedException
	{
		//////////////////////////////////////////
		// fragments should all have non-null Read and Lane, as verified by the Mapper.
		//////////////////////////////////////////
		Iterator<SequencedFragment> seqs_it = sequences.iterator();
		SequencedFragment fragment;

		fragment = seqs_it.next(); // this should be read 2
		if ( fragment.getRead() != 2)
			throw new RuntimeException("Missing read 2 in multiplexed input for location " + key.getLocation() + ".  Record: " + fragment);

		int lane = fragment.getLane();
		String indexSeq = fragment.getSequence().toString();
		if (indexSeq.length() != 7)
			throw new RuntimeException("Unexpected bar code sequence length " + indexSeq.length() + " (expected 7)");
		indexSeq = indexSeq.substring(0,6); // trim the last base -- it should be a spacer

		String sampleId;
		BarcodeLookup.Match m = barcodeLookup.getSampleId(lane, indexSeq);
		if (m == null)
			sampleId = "unknown";
		else
		{
			sampleId = m.getEntry().getSampleId();
			context.increment("Barcode base mismatches", String.valueOf(m.getMismatches()), 1);
		}

		outputKey.set(sampleId);

		fragment = seqs_it.next(); // should be read 1
		if (fragment.getRead() != 1)
			throw new RuntimeException("Missing read 1 in multiplexed input for location " + key.getLocation() + ".  Record: " + fragment);

		// write out the first read
		fragment.setIndexSequence(indexSeq);
		context.write(outputKey, fragment);
		context.increment("Sample reads", sampleId, 1);

		// all following reads need to be adjusted, decreasing their read number by 1
		while (seqs_it.hasNext())
		{
			fragment = seqs_it.next();
			if (fragment.getRead() < 3)
				throw new RuntimeException("Expecting read numbers greater than 2 but found " + fragment.getRead() + ".  Record: " + fragment);
			fragment.setRead( fragment.getRead() - 1);
			fragment.setIndexSequence(indexSeq);
			context.write(outputKey, fragment);
			context.increment("Sample reads", sampleId, 1);
		}
	}
}
