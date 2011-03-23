
package it.crs4.mr.read_sort;

import java.util.Map;
import java.util.HashMap;
import java.io.LineNumberReader;
import java.io.IOException;
import java.io.Reader;

public class BwaRefAnnotation
{
	public static class UnknownContigException extends java.lang.RuntimeException {
		public UnknownContigException(String msg) {
			super(msg);
		}
	}

	public static class InvalidAnnotationFormatException extends java.lang.RuntimeException { 
		public InvalidAnnotationFormatException(String msg) {
			super(msg);
		}
	}

	private static class Contig
	{
		public int id;
		public long start, length;
		public String name;

		public Contig(String name, int id, long start, long length)
		{
			this.id = id;
			this.start = start;
			this.length = length;
			this.name = name;
		}
	};

	private enum AnnScannerState {
		NameLine,
		CoordLine
	}

	private Map<String, Contig> contigMap;
	private long referenceLength;

	public BwaRefAnnotation()
	{
		contigMap = new HashMap<String, Contig>(30); // initial capacity of 30
		referenceLength = -1;
	}

	public BwaRefAnnotation(Reader in) throws IOException
	{
		contigMap = new HashMap<String, Contig>(30); // initial capacity of 30
		referenceLength = -1;
		this.load(in);
	}

	public void load(Reader in) throws IOException, InvalidAnnotationFormatException
	{
		LineNumberReader input = new LineNumberReader(in);

		String line = null;
		line = input.readLine();
		if (line == null)
			throw new InvalidAnnotationFormatException("Empty annotations file");

		try
		{
			long[] row = scanPosLine(line);

			referenceLength = row[0];
			if (referenceLength <= 0)
				throw new InvalidAnnotationFormatException("Invalid reference length " + referenceLength);
			int nContigs = (int)row[1]; // cast to avoid warning about loss of precision
			if (nContigs <= 0)
				throw new InvalidAnnotationFormatException("Invalid number of contigs " + nContigs);

			AnnScannerState state = AnnScannerState.NameLine;
			int contigNumber = 0;
			String lastContigName = null;

			line = input.readLine();
			while (line != null) 
			{
				if (line.equals("")) // skip blank lines
					continue;

				if (state == AnnScannerState.NameLine)
				{
					String[] fields = scanNameLine(line);
					contigNumber += 1;
					if (contigNumber > nContigs)
						throw new InvalidAnnotationFormatException("There are more contigs than expected (first line says we should have " + nContigs + ")");
					lastContigName = fields[1];
					state = AnnScannerState.CoordLine;
				}
				else // state is CoordLine
				{
					long[] fields = scanPosLine(line);
					contigMap.put(lastContigName, new Contig(lastContigName, contigNumber, fields[0], fields[1]));
					state = AnnScannerState.NameLine;
				}
				line = input.readLine();
			}
			if (state != AnnScannerState.NameLine)
				throw new InvalidAnnotationFormatException("last entry is incomplete (found the name line but not the coordinates)");
			if (contigNumber < nContigs)
				throw new InvalidAnnotationFormatException("Not enough contig records.  Header said we should have " + nContigs + ", but we only found " + contigNumber);
		} 
		catch (NumberFormatException e) {
			throw new InvalidAnnotationFormatException("Line " + input.getLineNumber() + ": invalid number (" + e.getMessage() + "). Original line: " + line);
		}
		catch (InvalidAnnotationFormatException e) {
			// add line number to message
			throw new InvalidAnnotationFormatException("Line " + input.getLineNumber() + ": " + e.getMessage());
		}
	}

	private long[] scanPosLine(String line) throws NumberFormatException 
	{
		String[] fields = line.split("\\s+");
		if (fields.length != 3)
			throw new InvalidAnnotationFormatException("Wrong number of fields (" + fields.length + ").  Expected 3");

		long[] retval = new long[3];
		for (int i = 0; i <= 2; ++i)
		{
			retval[i] = Long.parseLong(fields[i]);
			if (retval[i] < 0)
				throw new NumberFormatException();
		}
		return retval;
	}

	private String[] scanNameLine(String line)
	{
		String[] fields = line.split("\\s+");
		if (fields.length != 3)
			throw new InvalidAnnotationFormatException("Wrong number of fields (" + fields.length + ").  Expected 3");
		return fields;
	}

	public long getReferenceLength()
	{
		return referenceLength;
	}

	public int getContigId(String name)
	{
		return getContig(name).id;
	}

	public long getAbsCoord(String contig_name, long localCoord)
	{
		Contig contig = getContig(contig_name);
		return contig.start + localCoord;
	}

	private Contig getContig(String name)
	{
		Contig c = contigMap.get(name);
		if (c != null)
			return c;
		else
			throw new UnknownContigException("Unknown contig name '" + name + "'");
	}
}
