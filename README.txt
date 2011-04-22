==================
Seal
==================


Seal is a toolkit of distributed applications for generating, manipulating, and
analyzing short read alignments.  Seal applications generally run on the Hadoop
framework and are made to scale well in the amount of computing nodes available
and the amount of the data to process.  This fact makes Seal particularly well
suited for processing large data sets.
Seal is part of the Biodoop suite.


Seal currently includes three main applications:  PairReadsQSeq, Seqal, and
ReadSort.

PairReadsQSeq
	A preprocessor to convert Illumina ``qseq`` files into ``prq`` file format; 
	prq files are simply 5 tab-separated fields per line:
	id, read 1, base qualities 1, read 2, base qualities 2.
	PairReadsQSeq also filters reads that don't have a minimum number of known
	bases, and reads that failed machine quality checks.  If you already have 
	data in ``prq`` format you may choose to skip running PairReadsQSeq and 
	jump directly to Seqal.

Seqal
  TODO

ReadSort
	A Hadoop utility to sort read alignments.


Please see the full Seal documentation at  docs/_build/html/index.html.

Authors
++++++++++++

Seal was written by:
 * Luca Pireddu <luca.pireddu@crs4.it>
 * Simone Leo <simone.leo@crs4.it>
 * Gianluigi Zanetti <gianluigi.zanetti@crs4.it>


License
++++++++

Seal and its components are released under the `GPLv3 license <http://www.gnu.org/licenses/gpl.html>`_.


Copyright
++++++++++

Copyright CRS4, 2011.
