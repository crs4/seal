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
import it.crs4.seal.common.Utils;

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
	private static final byte[] SLASH_X = "/X".getBytes();

	private BarcodeLookup barcodeLookup;
	private Text outputKey = new Text();
	private boolean expectIndexRead = true;
	private boolean separatesReads = false;

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
		separatesReads = conf.getBoolean(Demux.CONF_SEPARATE_READS, false);
	}

	public void reduce(SequenceId key, Iterable<SequencedFragment> sequences, IMRContext<Text,SequencedFragment> context) throws IOException, InterruptedException
	{
		// XXX: this function is growing too much.  Consider refactoring.
		//////////////////////////////////////////
		// Fragments should all have non-null Read and Lane, as verified by the Mapper.
		// They should be ordered read 2, read 1, read 3 and over
		//////////////////////////////////////////
		Iterator<SequencedFragment> seqs_it = sequences.iterator();
		SequencedFragment fragment;
		String flowcellId = "";
		String indexSeq = ""; // default index is blank
		String sampleId;
		String project;

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
		{
			sampleId = "unknown";
			project = ".";
		}
		else
		{
			sampleId = m.getEntry().getSampleId();
			flowcellId = m.getEntry().getFlowcellId();
			project = m.getEntry().getProject();
			if (project == null)
				project = Demux.DEFAULT_PROJECT;
			context.increment("Barcode base mismatches", String.valueOf(m.getMismatches()), 1);
		}

		// Project/sample results in that directory structure. The key is the same for all reads in iterator
		// TODO:  profile!  We're sanitizing and rebuilding the file name for
		// each set of reads.  It may be a significant waste of CPU that could be fixed by a caching mechanism.
		String keyString = Utils.sanitizeFilename(project) + '/' + Utils.sanitizeFilename(sampleId);
		outputKey.set(keyString);
		if (separatesReads) {
			// append a slash and an 'X' (the latter to make a space for the read number)
			outputKey.append(SLASH_X, 0, SLASH_X.length);
		}

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

			if (separatesReads)
			{
				// Overwrite the last character of the key with the read number.
				// This technique only supports single digit read numbers
				outputKey.getBytes()[outputKey.getLength() - 1] = (byte)(fragment.getRead().byteValue() + '0');
			}

			context.write(outputKey, fragment);
			context.increment("Sample reads", keyString, 1);

			if (seqs_it.hasNext())
				fragment = seqs_it.next();
			else
				done = true;
		} while (!done);

		if (fragment.getRead() > 2)
		{ // although the code above is generic and will handle any number of reads,
			// in our current use cases any more than 2 data reads (non-index) indicate
			// a problem with the data.
			// XXX: if someone removes this check, verify the "separatesReads" section above.
			throw new RuntimeException("Unexpected output read number " + fragment.getRead() +
					" at location " + key.getLocation() +
					" (note that if read number may have been decremented by 1 if an index sequence was present).");
		}
	}
}
