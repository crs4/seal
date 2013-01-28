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
	private boolean expectIndexRead = true;

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

		expectIndexRead = !conf.getBoolean(Demux.CONF_NO_INDEX_READS, false);
	}

	public void reduce(SequenceId key, Iterable<SequencedFragment> sequences, IMRContext<Text,SequencedFragment> context) throws IOException, InterruptedException
	{
		//////////////////////////////////////////
		// Fragments should all have non-null Read and Lane, as verified by the Mapper.
		// They should be ordered read 2, read 1, read 3 and over
		//////////////////////////////////////////
		Iterator<SequencedFragment> seqs_it = sequences.iterator();
		SequencedFragment fragment;
		String flowcellId = "";
		String indexSeq = ""; // default index is blank
		String sampleId;

		fragment = seqs_it.next();

		if (expectIndexRead)
		{
			// Fetch the first fragment from the list -- it should be the index sequence
			if (fragment.getRead() != 2)
				throw new RuntimeException("Missing read 2 in multiplexed input for location " + key.getLocation() + ".  Record: " + fragment);

			indexSeq = fragment.getSequence().toString();
			if (indexSeq.length() != 7)
				throw new RuntimeException("Unexpected bar code sequence length " + indexSeq.length() + " (expected 7)");
			indexSeq = indexSeq.substring(0,6); // trim the last base -- it should be a spacer

			// We've consumed this index read.  Advance to the next one.
			fragment = seqs_it.next();
		}

		// From here on, they should be all data reads.

		int lane = fragment.getLane();
		BarcodeLookup.Match m = barcodeLookup.getSampleId(lane, indexSeq);
		if (m == null)
			sampleId = "unknown";
		else
		{
			sampleId = m.getEntry().getSampleId();
			flowcellId = m.getEntry().getFlowcellId();
			context.increment("Barcode base mismatches", String.valueOf(m.getMismatches()), 1);
		}

		outputKey.set(sampleId); // same output key for all reads

		boolean done = false;
		do {
			fragment.setIndexSequence(indexSeq);

			// When we read qseq, the flowcell id isn't set (the file format doesn't include that data.
			// Since we have the chance here, we'l extract the flowcell id from the sample sheet
			// and set it on the outgoing SequencedFragment.
			if (fragment.getFlowcellId() == null)
				fragment.setFlowcellId(flowcellId);

			if (expectIndexRead && fragment.getRead() > 2)
				fragment.setRead( fragment.getRead() - 1);

			context.write(outputKey, fragment);
			context.increment("Sample reads", sampleId, 1);

			if (seqs_it.hasNext())
				fragment = seqs_it.next();
			else
				done = true;
		} while (!done);
	}
}
