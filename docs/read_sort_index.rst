.. _read_sort_index:

ReadSort
==========

ReadSort is a Hadoop utility to sort read alignments.  Currently ReadSort only
supports sorting by mapping coordinate, and only supports reading and writing
SAM.  

**Currently ReadSort is tuned to work well sorting mappings uniformly distributed over
the reference.**

ReadSort works by breaking up the output into several several sorted parts, one
per reduce task (like all Hadoop applications).  Each output file is itself sorted.  In addition, all
mappings in part *n* come before part *n+1*.  Therefore, to create a single file
with all the ordered data one merely has to concatenate all the output files in
order.

Sorting using ReadSort requires access to the reference annotations file of the
reference used to create the mappings (because ReadSort has to read the relative
positions of the contigs).


Usage
++++++++


Creating a single sorted SAM requires two steps:  sorting and merging.

#. ``./bin/read_sort -ann file:///references/human_g1k.ann hdfs_input_sam hdfs_sorted``
#. ``./bin/merge_sorted_alignments -ann file:///references/human_g1k.ann hdfs_sorted > local_sorted.sam``

Alternatively, you can place the sorted output directly on HDFS::

  ./bin/merge_sorted_alignments -ann file:///references/human_g1k.ann hdfs_sorted whole_sorted.sam

You could also convert it to bam, on-the-fly::

  ./bin/merge_sorted_alignments -ann file:///references/human_g1k.ann hdfs_sorted | samtools view -bST  /references/human_g1k.fai /dev/stdin -o whole_sorted.bam


Remember that the annotation file path *must be accessible by all Hadoop cluster
nodes*. It will be accessed by the mappers and partitioners. You may place the 
file on a shared volume or HDFS.  Also, unqualified paths (without ``file://``) 
are **assumed to be on HDFS**.


Distributed reference
------------------------

Option:  ``--distributed-reference``, ``-distref``

ReadSort also supports using a distributed reference archive, much like 
:ref:`Seqal <seqal_index>`.  In fact, this approach may prove advantageous if
you call ReadSort right after aligning with Seqal, and you don't keep references
on a shared volume.

To use this feature, specify the name of the reference archive to be distributed
with ``-distref``, then use ``-ann`` to specify the name of the annotations file
*relative to the archive root*.

Note that you will still need a locally accessible annotations file for the merging 
step.



Number of reduce tasks
-------------------------

Option:  ``--reducers``

ReadSort by default uses 3 * reduce tasks per node.  You can override the 
default number of reduce tasks with the ``--reducers`` option.
