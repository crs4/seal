Usage
=====

To run PairReadsQSeq, use the ``run_prq.sh`` script in the ``bin``
subdirectory of the distribution root.  For example,

  ./bin/run_prq.sh /user/me/qseq_input /user/me/prq_output

The ``run_prq.sh`` command takes two mandatory arguments:

#. Input path, containing individual reads in the qseq_ format;
#. Output path, where paired reads will be written in ``prq`` format.
#. (Optional) Minimum number of known bases (default is 30).

The input and output paths must be on an HDFS volume. If you like, you can use 
paths relative to the current user's HDFS home directory, i.e., ``/user/<USERNAME>``.

Read Filtering
================

PairReadsQSeq filters read pairs that fail to meet certain quality criteria.

* not enough known bases;
* failure to meet the sequencing machine's quality checks.

Min number of known bases
+++++++++++++++++++++++++++

Reads output from the sequencing machine often contain bases that could not be
read.  Reads with too few known bases are undesirable, so PairReadsQSeq can
filter them.  By default, if neither read in a pair has at least 30 known bases
the pair is dropped.  You can override this setting by providing a 3rd argument
to ``run_prq.sh``:

  e.g. bin/run_prq.sh /user/me/qseq_data /user/me/prq_data 15

To disable this feature specify a minimum known base threshold of 0.

Failed quality checks
++++++++++++++++++++++++

Reads that don't meet machine-based quality checks are identified in qseq_ files
by the value in the last column (0: failed check; 1: passed check).
PairReadsQSeq normally eliminates read pairs if both reads failed quality
checks.  To disable this behaviour edit ``bin/run_prq.sh`` and change

  bl.prq.drop-failed-filter=true

to 

  bl.prq.drop-failed-filter=false

Counters
===========

:NotEnoughBases: 
  number of reads that have fewer known bases than the minimum requirement.

:FailedFilter:
  number of reads that failed machine quality checks.

:Dropped:
  number of reads dropped from the dataset for either of the reasons above.


.. _qseq: file_formats.html#qseq-file-format-input
