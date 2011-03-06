.. You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PairReadsQSeq |release| Documentation
=====================================

PairReadsQSeq is a Hadoop utility to convert  Illumina ``qseq`` files into
``prq`` file format; prq files are simply 5 tab-separated fields per line:
id, read 1, base qualities 1, read 2, base qualities 2.
If you already have data in ``prq`` format you may
choose to skip running PairReadsQSeq and jump directly to Seqal.

PairReadsQSeq also filters read pairs where both reads don't have a minimum 
number of known bases (30 by default).

Usage
-----

To run PairReadsQSeq, use the ``run_prq.sh`` script in the ``bin``
subdirectory of the distribution root.  For example,

  ./bin/run_prq.sh /user/me/qseq_input /user/me/prq_output

The ``run_prq.sh`` command takes two mandatory arguments:

#. input path, containing individual reads in the QSeq format;
#. output path, where paired reads will be written in ``prq`` format.

The data must be on an HDFS volume. If you like you can used paths relative 
to the current user's HDFS home directory, i.e., ``/user/<USERNAME>``.

Optionally you can provide a minimum number of known bases (default is 30) to
require of either read in a pair.  If neither read reaches the required number
then the pair will be dropped.  Dropped pairs are counted by the
*NotEnoughBases* counter.

Installation
------------

A pre-built jar is included in the distribution root.  If you want to
rebuild it from source, run `ant <http://ant.apache.org>`_ in the prq root
directory.

Note that ``run_prq.sh`` expects ``PairReadsQSeq.jar`` to be in its
parent directory.

Hadoop
-------

It is recommended that you set the HADOOP_HOME environment variable to
point to your Hadoop installation; as an alternative, ensure that the 
``hadoop`` executable is in your ``PATH``.
In addition, if you use a non-standard Hadoop configuration directory,
the ``HADOOP_CONF_DIR`` environment variable has to be set to point to
that directory.

For instance, if Hadoop is installed in ``/opt/hadoop`` and its
configuration directory is in ``/etc/hadoopconf``::

 export HADOOP_HOME="/opt/hadoop"
 export HADOOP_CONF_DIR="/etc/hadoopconf"

