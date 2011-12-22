.. _prq_index:

PairReadsQSeq 
==============

PairReadsQSeq (PRQ) is a Hadoop utility to convert Illumina `qseq`_ or Fastq files into
`prq`_ file format; prq files are simply 5 tab-separated fields per line:
id, read 1, base qualities 1, read 2, base qualities 2.

PairReadsQSeq standardizes base quality scores to Sanger-style Phred+33 encoding.
In addition, it converts unknown bases 'N' (as opposed to the '.' used in
QSeq files).

If you already have data in prq format you may
choose to skip running PairReadsQSeq and jump directly to Seqal.

PairReadsQSeq by default *filters read pairs* where both reads don't have a minimum 
number of known bases (30 by default).

In addition, PairReadsQSeq by default *filters read pairs* if both its reads failed the machine quality
checks (last column of the Qseq file format).

Usage
+++++

To run PairReadsQSeq, launch ``bin/prq``.  For example,

::

  ./bin/prq /user/me/qseq_input /user/me/prq_output


``prq`` follows the normal Seal usage convention.  See the section
:ref:`program_usage` for details.


Configurable Properties
++++++++++++++++++++++++++

================================ ===========================================================
**Name**                           **Meaning**
-------------------------------- -----------------------------------------------------------
bl.qseq.base-quality-encoding     "illumina" or "sanger"
bl.prq.input-format               "qseq" or "fastq".
bl.prq.min-bases-per-read         See `Read Filtering`_
bl.prq.drop-failed-filter         See `Read Filtering`_
bl.prq.warning-only-if-unpaired   PRQ normally stops with an error if it finds an unpaired 
                                  read.  If this property is set to true it will instead 
                                  emit a warning and keep going.
================================ ===========================================================

In addition, all the general Seal and Hadoop configuration properties apply.

.. note:: **Config File Section Title**: Prq


Input format
+++++++++++++++

By default PairReadsQSeq expects input in Qseq format.  You can specify Fastq
by setting `-D bl.prq.input-format=fastq`::

  ./bin/prq -D bl.prq.input-format=fastq fastq prq

Quality encoding
-------------------

PairReadsQSeq expects the base quality scores in qseq files to be encoded in
Illumina Phred+64 format.  They will be converted to Sanger Phred+33 format in
the output prq file.  If the scores are already converted, tell PRQ with 
`bl.qseq.base-quality-encoding=sanger`::


  ./bin/prq -D bl.prq.base-quality-encoding=sanger qseq prq


Read Filtering
++++++++++++++++

PairReadsQSeq can filter read pairs that fail to meet certain quality criteria.

* not enough known bases;
* failure to meet the sequencing machine's quality checks.

Min number of known bases
---------------------------

Property name:  ``bl.prq.min-bases-per-read``

Reads output from the sequencing machine often contain bases that could not be
read.  Reads with too few known bases are undesirable, so PairReadsQSeq can
filter them.  By default, if neither read in a pair has at least 30 known bases
the pair is dropped.  You can override this setting by setting the
``bl.prq.min-bases-per-read`` property to your desired value.  For instance, to 
require 15 known bases::

  bin/prq -D bl.prq.min-bases-per-read=15 /user/me/qseq_data /user/me/prq_data

**To disable this feature** specify a minimum known base threshold of 0.


Failed quality checks
------------------------

Property name:  ``bl.prq.drop-failed-filter``

As previously mentioned, PairReadsQSeq by default filters read pairs if both 
the pair's reads failed the machine quality checks.  Reads that don't meet 
machine-based quality checks are identified in qseq_ files by the value in the 
last column (0: failed check; 1: passed check).  To disable this behaviour 
set the property ``bl.prq.drop-failed-filter`` to false.


Counters
+++++++++++

PRQ provides a number of counters that report on the number of reads filtered.

:NotEnoughBases: 
  number of reads that have fewer known bases than the minimum requirement.

:FailedFilter:
  number of reads that failed machine quality checks.

:Unpaired:
  number of unpaired reads found in the data (only if ``bl.prq.warning-only-if-unpaired`` is enabled).

:Dropped:
  number of reads dropped from the dataset for any of the reasons above.

  


.. _qseq: file_formats.html#qseq-file-format-input
.. _prq: file_formats.html#prq-file-format-output
