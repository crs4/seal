
Usage
=======

Constructing a sorted SAM requires two steps:

#. ``./bin/read_sort -ann file:///reference/human_g1k_v37.ann hdfs_sam hdfs_sorted``
#. ``./bin/merge_sorted_alignments -ann file:///reference/human_g1k_v37.ann hdfs_sorted > local_sorted.sam``


ReadSort requires the annotations file that describes the order of the contigs
in the reference.  Only BWA format is supported at the moment.

Options
+++++++++++++++++++++

``--annotations``
  The reference annotations file to use.  Unqualified paths are assumed to be on
  HDFS, so if you have a file that is accessible on a mounted filesystem, you
  need to specify the path by prefixing `file://` (e.g. ``file:///absolute/path/to/file.ann``).

``--distributed-reference``
  If you just generated the SAM with the *Seqal* aligner, you may still have the
  distributed BWA reference lying around.  You can tell ReadSort to use its
  annotations file.  Specify the HDFS path to the reference archive with
  ``--distributed-reference`` and specify the name of the annotations file
  within it using ``--annotations``.  This is only valid of the
  ``bin/read_sort`` step.  You will still need a locally accessible annotations
  file for the merging step.

``--reducers``
  Override the number of the reduce tasks used by ReadSort.

Settings
+++++++++

Within ``bin/read_sort`` there are a number of Hadoop property settings relating
to memory usage that you may need to tweak for your cluster.  Please see the
Hadoop documentation for details on their meaning.


