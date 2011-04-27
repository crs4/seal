.. _prq_index:

PairReadsQSeq Documentation
=====================================

PairReadsQSeq is a Hadoop utility to convert  Illumina `qseq`_ files into
`prq`_ file format; prq files are simply 5 tab-separated fields per line:
id, read 1, base qualities 1, read 2, base qualities 2.
Also, prq indicates unknown bases with an 'N', as opposed to the '.' used in
QSeq files.

If you already have data in prq format you may
choose to skip running PairReadsQSeq and jump directly to Seqal.

PairReadsQSeq also *filters read pairs* where both reads don't have a minimum 
number of known bases (30 by default).

In addition, PairReadsQSeq by default *filters read pairs* if both its reads failed the machine quality
checks (last column of the Qseq file format).

Usage
+++++

To run PairReadsQSeq, use the ``bin/run_prq.sh`` script in the Seal
distribution.  For example,

  ./bin/run_prq.sh /user/me/qseq_input /user/me/prq_output

The ``run_prq.sh`` command takes two mandatory arguments:

#. Input path, containing individual reads in the qseq_ format;
#. Output path, where paired reads will be written in ``prq`` format.
#. (Optional) Minimum number of known bases (default is 30).

The input and output paths must be on an HDFS volume. If you like, you can use 
paths relative to the current user's HDFS home directory, i.e., ``/user/<USERNAME>``.


Read Filtering
++++++++++++++++

PairReadsQSeq filters read pairs that fail to meet certain quality criteria.

* not enough known bases;
* failure to meet the sequencing machine's quality checks.

Min number of known bases
---------------------------

Reads output from the sequencing machine often contain bases that could not be
read.  Reads with too few known bases are undesirable, so PairReadsQSeq can
filter them.  By default, if neither read in a pair has at least 30 known bases
the pair is dropped.  You can override this setting by providing a 3rd argument
to ``run_prq.sh``.  For instance, to require 15 known bases::

  bin/run_prq.sh /user/me/qseq_data /user/me/prq_data 15

To disable this feature specify a minimum known base threshold of 0.

Failed quality checks
------------------------

As previously mentioned, PairReadsQSeq by default filters read pairs if both 
the pair's reads failed the machine quality checks.  Reads that don't meet 
machine-based quality checks are identified in qseq_ files by the value in the 
last column (0: failed check; 1: passed check).  To disable this behaviour 
edit the file ``bin/run_prq.sh`` and change
::
  bl.prq.drop-failed-filter=true

to 

::

  bl.prq.drop-failed-filter=false

Counters
+++++++++++

:NotEnoughBases: 
  number of reads that have fewer known bases than the minimum requirement.

:FailedFilter:
  number of reads that failed machine quality checks.

:Dropped:
  number of reads dropped from the dataset for either of the reasons above.


.. _qseq: file_formats.html#qseq-file-format-input
.. _prq: file_formats.html#prq-file-format-output
