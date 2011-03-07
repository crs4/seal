.. You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PairReadsQSeq |release| Documentation
=====================================

PairReadsQSeq is a Hadoop utility to convert  Illumina `qseq`_ files into
`prq`_ file format; prq files are simply 5 tab-separated fields per line:
id, read 1, base qualities 1, read 2, base qualities 2.
If you already have data in prq format you may
choose to skip running PairReadsQSeq and jump directly to Seqal.

PairReadsQSeq also filters read pairs where both reads don't have a minimum 
number of known bases (30 by default).

Contents:

.. toctree::
   :maxdepth: 2

   installation
   usage
   file_formats


Indices and tables
==================

* :ref:`genindex`
* :ref:`search`

.. _qseq: file_formats.html#qseq-file-format-input
.. _prq: file_formats.html#prq-file-format-output
