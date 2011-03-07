Usage
=====

To run PairReadsQSeq, use the ``run_prq.sh`` script in the ``bin``
subdirectory of the distribution root.  For example,

  ./bin/run_prq.sh /user/me/qseq_input /user/me/prq_output

The ``run_prq.sh`` command takes two mandatory arguments:

#. Input path, containing individual reads in the QSeq format;
#. Output path, where paired reads will be written in ``prq`` format.
#. (Optional) Minimum number of known bases (default is 30).

The input and output paths must be on an HDFS volume. If you like, you can use 
paths relative to the current user's HDFS home directory, i.e., ``/user/<USERNAME>``.

The third optional argument is the minimum number of known bases (default is 30)
required of either read in a pair.  If neither read reaches the required number
then the pair will be dropped.  Dropped pairs are counted by the
*NotEnoughBases* counter.

