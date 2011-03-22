
package tests.it.crs4.mr.read_sort;

import java.io.StringReader;

import org.junit.*;
//import org.junit.runners.Suite;
import static org.junit.Assert.*;

import it.crs4.mr.read_sort.BwaRefAnnotation;

public class TestBwaRefAnnotation {
	private String annotationSample;
	private StringReader sampleReader;

	private BwaRefAnnotation emptyAnnotation;
	private BwaRefAnnotation loadedAnnotation;

	@Before
	public void setUp() throws java.io.IOException
	{
		emptyAnnotation = new BwaRefAnnotation();

		annotationSample = 
		  "3080436051 6 11\n" +
		  "0 chr1 (null)\n" +
		  "0 247249719 39\n" +
		  "0 chr2 (null)\n" +
		  "247249719 242951149 25\n" +
		  "0 chr3 (null)\n" +
		  "490200868 199501827 10\n" +
		  "0 chr4 (null)\n" +
		  "689702695 191273063 14\n" +
		  "0 chr5 (null)\n" +
		  "880975758 180857866 7\n" +
		  "0 mycontig (null)\n" +
		  "1061833624 170899992 11\n";
		sampleReader = new StringReader(annotationSample);
		loadedAnnotation = new BwaRefAnnotation();
		loadedAnnotation.load(sampleReader);
	}

	@Test(expected=BwaRefAnnotation.InvalidAnnotationFormatException.class)
	public void testLoadEmpty() throws java.io.IOException
	{
		emptyAnnotation.load( new StringReader("") );
	}

	@Test(expected=BwaRefAnnotation.InvalidAnnotationFormatException.class)
	public void testNotEnoughContigs() throws java.io.IOException
	{
		String ann = 
		  "3080436051 6 11\n" +
		  "0 chr1 (null)\n" +
		  "0 247249719 39\n" +
		  "0 chr2 (null)\n" +
		  "247249719 242951149 25\n";
		emptyAnnotation.load( new StringReader(ann) );
	}

	@Test(expected=BwaRefAnnotation.InvalidAnnotationFormatException.class)
	public void testTooManyContigs() throws java.io.IOException
	{
		String ann = 
		  "3080436051 1 11\n" +
		  "0 chr1 (null)\n" +
		  "0 247249719 39\n" +
		  "0 chr2 (null)\n" +
		  "247249719 242951149 25\n";
		emptyAnnotation.load( new StringReader(ann) );
	}

	@Test(expected=BwaRefAnnotation.InvalidAnnotationFormatException.class)
	public void testIncompleteContigRecord() throws java.io.IOException
	{
		String ann = 
		  "3080436051 1 11\n" +
		  "0 chr1 (null)\n";
		emptyAnnotation.load( new StringReader(ann) );
	}

	@Test(expected=BwaRefAnnotation.InvalidAnnotationFormatException.class)
	public void testZeroContigs() throws java.io.IOException
	{
		String ann = 
		  "3080436051 0 11\n" +
		  "0 chr1 (null)\n" +
		  "0 247249719 39\n";
		emptyAnnotation.load( new StringReader(ann) );
	}

	@Test
	public void testGetReferenceLength()
	{
		assertEquals(3080436051L, loadedAnnotation.getReferenceLength());
	}

	@Test
	public void testGetContigId()
	{
		assertEquals(1, loadedAnnotation.getContigId("chr1"));
		assertEquals(2, loadedAnnotation.getContigId("chr2"));
		assertEquals(6, loadedAnnotation.getContigId("mycontig"));
	}

	@Test(expected=BwaRefAnnotation.UnknownContigException.class)
	public void testGetContigIdUnknownContig()
	{
		loadedAnnotation.getContigId("zanzan");
	}

	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main("tests.it.crs4.mr.read_sort.TestBwaRefAnnotation");
	}
}
