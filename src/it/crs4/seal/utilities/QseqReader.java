
package it.crs4.seal.utilities;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.seqdoop.hadoop_bam.SequencedFragment;

public interface QseqReader extends Closeable
{
	public boolean done();
	public List<SequencedFragment> getCurrent();
	public void advance() throws IOException;
}
