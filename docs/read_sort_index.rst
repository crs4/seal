.. _read_sort_index:

ReadSort
==========

ReadSort is a Hadoop utility to sort read alignments.  Currently ReadSort only
supports sorting by mapped coordinate, and only supports reading and writing
SAM.

**Currently ReadSort is tuned to work well sorting mappings uniformly distributed over
the reference.**

ReadSort works by breaking up the output into several several sorted parts, one
per reduce task (like all Hadoop applications).  Each output file is itself
sorted.  In addition, all mappings in part *n* come before part *n+1*.
Therefore, to create a single file with all the ordered data one merely has to
concatenate all the output files in order.

Sorting using ReadSort requires access to the annotations file of the
reference used to create the mappings (because ReadSort has to read the relative
positions of the contigs).


Usage
++++++++


Creating a single sorted SAM requires two steps:  sorting and merging.

#. ``seal read_sort -ann file:///references/human_g1k.ann hdfs_input_sam hdfs_sorted``
#. ``seal merge_alignments --sort-order coordinate -ann file:///references/human_g1k.ann hdfs_sorted > local_sorted.sam``

Alternatively, you can place the sorted output directly on HDFS::

  seal merge_alignments --sort-order coordinate -ann file:///references/human_g1k.ann hdfs_sorted whole_sorted.sam

You could also convert it to bam, on-the-fly::

  seal merge_alignments --sort-order coordinate -ann file:///references/human_g1k.ann hdfs_sorted | samtools view -bST  /references/human_g1k.fai /dev/stdin -o whole_sorted.bam

You can also add the read group header to the SAM.  Run ``./bin/merge_alignments
--help`` to see the options.

Remember that the annotation file path *must be accessible by all Hadoop cluster
nodes*. It will be accessed by the mappers and partitioners. You may place the
file on a shared volume or HDFS.  Also, unqualified paths (without ``file://``)
are **assumed to be on the Hadoop cluster's default file system (usually HDFS)**.

``seal read_sort`` follows the normal Seal usage convention.  See the section
:ref:`program_usage` for details.



Distributed reference
------------------------

Option:  ``--distributed-reference``, ``-distref``

ReadSort also supports using a distributed reference archive, much like
:ref:`Seqal <seqal_index>`.  In fact, this approach may prove advantageous if
you call ReadSort right after aligning with Seqal since Hadoop may be able to
reuse the archive it distributed in the previous step.

To use this feature, specify the name of the reference archive to be distributed
with ``-distref``, then use ``-ann`` to specify the name of the annotations file
*relative to the archive root*.

Note that you will still need a locally accessible annotations file for the merging
step.


Configurable Properties
++++++++++++++++++++++++++

ReadSort does not have any program-specific configurable properties at the
moment.  You can still use its section to configure Hadoop property values
specific to ReadSort.

.. note:: **Config File Section Title**: ReadSort
