ReadSort |release| Documentation
====================================

ReadSort is a Hadoop utility to sort read alignments.  Currently ReadSort only
supports sorting by mapping coordinate, and only supports reading and writing
SAM.  

On a Hadoop cluster with 30 nodes ReadSort can sort and download to the local file
system 24 GB of reads (about 73M) in about 6 minutes.


Creating a sorted SAM requires two steps:

#. ``./bin/read_sort -ann file:///references/human_g1k.ann hdfs_input_sam hdfs_sorted``
#. ``./bin/merge_sorted_alignments -ann file:///references/human_g1k.ann hdfs_sorted > local_sorted.sam``



Contents:
---------

.. toctree::
   :maxdepth: 2

   installation
   usage
