
====================
PairReadsQSeq
====================


PairReadsQSeq is a preprocessor to convert Illumina ``qseq`` files into
``prq`` file format; prq files are simply 5 tab-separated fields per line:
id, read 1, base qualities 1, read 2, base qualities 2.
PairReadsQSeq also filters reads that don't have a minimum number of known
bases, and reads that failed machine quality checks.  If you already have 
data in ``prq`` format you may choose to skip running PairReadsQSeq and 
jump directly to Seqal.


Please see the full documentation at  docs/_build/html/index.html.

Authors
++++++++++++

PairReadsQSeq was been written by:
  * Luca Pireddu <luca.pireddu@crs4.it>


License
++++++++

PairReadsQSeq and its components are released under the `GPLv3 license <http://www.gnu.org/licenses/gpl.html>`_.


Copyright
++++++++++

Copyright CRS4, 2011.
