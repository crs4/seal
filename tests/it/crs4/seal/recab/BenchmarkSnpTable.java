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


package tests.it.crs4.seal.recab;

import org.junit.*;
import static org.junit.Assert.*;

import it.crs4.seal.recab.SnpTable;
import it.crs4.seal.recab.ArraySnpTable;
import it.crs4.seal.recab.ArrayListSnpTable;
import it.crs4.seal.recab.HashSetSnpTable;
import it.crs4.seal.recab.SnpDef;
import it.crs4.seal.recab.SnpReader;
import it.crs4.seal.recab.VcfSnpReader;
import it.crs4.seal.common.FormatException;

import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BenchmarkSnpTable
{
	public static class Run
	{
		private long startLoad = 0;
		private long endLoad = 0;
		private long startQuery = 0;
		private long endQuery = 0;
		private int nQueries = 0;
		private int tableSize = 0;
		private long memUsage = 0;

		public void setRun( long sLoad, long eLoad, long sQuery, long eQuery, int nQueries, int tableSize) 
		{
			startLoad = sLoad;
			endLoad = eLoad;
			startQuery = sQuery;
			endQuery = eQuery;
			this.nQueries = nQueries;
			this.tableSize = tableSize;
		}

		public long getLoadTime() { return endLoad - startLoad; }
		public long getQueryTime() { return endQuery - startQuery; }
		public long getNQueries() { return nQueries; }
		public long getTableSize() { return tableSize; }

		public void report(String klas, PrintStream out)
		{
			out.println("Benchmark for " + klas);
			out.println("\ttable size: " + getTableSize());
			out.println("\tn queries: " + getNQueries());
			out.println("\tload time: " + String.format("%.3f", (getLoadTime() / 1e9)) );
			out.println("\tquery time: " + String.format("%.3f", (getQueryTime() / 1e9)) );
			out.println("\tloads / sec: " + String.format("%.3f", getTableSize() / (getLoadTime() / 1e9) ) );
			out.println("\tqueries / sec: " + String.format("%.3f", getNQueries() / (getQueryTime() / 1e9) ) );
			out.println("\tmemory usage: " + String.format("%.3f MB", memUsage / ((float)1024*1024)) );
		}

		public void snapMemoryUsage()
		{
			System.gc();
			System.gc();
			Runtime r = Runtime.getRuntime();
			memUsage = r.totalMemory() - r.freeMemory();
		}
	}

	public static class Benchmark
	{
		private SnpTable table;
		private SnpReader reader;
		private int nQueries;

		private static class QGenerator
		{
			private ArrayList<String> contigs;
			private int maxpos;
			private Random rnd;

			public QGenerator(Set<String> contigs, int maxpos)
			{
				this.maxpos = maxpos+1;

				this.contigs = new ArrayList<String>(contigs.size());
				for (String c: contigs)
					this.contigs.add(c);

				rnd = new Random();
			}

			public void next(SnpDef snp)
			{
				snp.setContigName( contigs.get( rnd.nextInt(contigs.size()) ));
				snp.setPosition( rnd.nextInt(maxpos) );
			}
		}

		public Benchmark(SnpTable table, SnpReader reader)
		{
			this.table = table;
			this.reader = reader;
			nQueries = 100000000;
		}

		public void setNQueries(int n)
		{
			nQueries = n;
		}

		public Run run() throws Exception
		{
			System.gc();

			long loadStart = System.nanoTime();
			table.load(reader);
			long loadEnd = System.nanoTime();

			QGenerator generator = new QGenerator(table.getContigs(), 250000000);
			SnpDef snp = new SnpDef();

			long queryStart = System.nanoTime();
			for (int i = nQueries; i > 0; --i)
			{
				generator.next(snp);
				table.isSnpLocation(snp.getContigName(), snp.getPosition());
			}
			long queryEnd = System.nanoTime();

			Run run = new Run();
			run.setRun(loadStart, loadEnd, queryStart, queryEnd, nQueries, table.size());
			return run;
		}
	}

	public static void main(String[] args) throws Exception
	{
		if (args.length != 1)
		{
			System.err.println("Usage: BenchmarkSnpTable <vcf file>");
			System.exit(1);
		}

		File vcf = new File(args[0]);
		VcfSnpReader vcfReader = new VcfSnpReader(new FileReader(vcf));

		Class[] tableClasses = new Class[] { HashSetSnpTable.class, ArrayListSnpTable.class, ArraySnpTable.class };

		for (Class klas: tableClasses)
		{
			System.out.println("Trying " + klas);
			try
			{
				vcfReader = new VcfSnpReader(new FileReader(vcf));
				SnpTable table = (SnpTable)klas.newInstance();
				Benchmark bench = new Benchmark(table, vcfReader);
				System.out.println("Running bechmark");
				Run r = bench.run();
				System.out.println("finished\n\n");
				r.report(klas.toString(), System.out);
			}
			catch (OutOfMemoryError e) {
				System.err.println("ran out of memory with class " + klas);
			}
			catch (Exception e) {
				System.err.println("Error with class " + klas);
			}
		}
	}
}
