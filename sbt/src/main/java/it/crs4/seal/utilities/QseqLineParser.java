
package it.crs4.seal.utilities;

import org.apache.hadoop.io.Text;

import org.seqdoop.hadoop_bam.SequencedFragment;

public class QseqLineParser
{
	public static void parseQseqLine(String line, SequencedFragment dest)
	{
		String[] fields = line.split("\t");

		if (fields.length != 11) {
			throw new RuntimeException("Unexpected number of fields in qseq line.  Found " + 
					fields.length + " (expected 11).\nLine:  " + line);
		}

		dest.setInstrument(                    fields[0]);
		dest.setRunNumber(    Integer.parseInt(fields[1]));
		dest.setLane(         Integer.parseInt(fields[2]));
		dest.setTile(         Integer.parseInt(fields[3]));
		dest.setXpos(         Integer.parseInt(fields[4]));
		dest.setYpos(         Integer.parseInt(fields[5]));
		dest.setIndexSequence(      "0".equals(fields[6]) ? null : fields[6]);
		dest.setRead(         Integer.parseInt(fields[7]));
		dest.setSequence(             new Text(fields[8]));
		dest.setQuality(              new Text(fields[9]));
		dest.setFilterPassed(       "1".equals(fields[10]));
	}
}
