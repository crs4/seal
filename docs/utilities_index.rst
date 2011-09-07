.. _utilities_index:

Seal Utilities
================

We've included in the ``bin`` directory of the Seal distribution some of the 
utilities and scripts we use 
internally.  These may not be all maintained (it depends on whether we're using
them at the moment).  Use at your own risk!!

Note that some of these utilities don't follow the :ref:`Seal usage convention
<program_usage>`.


merge_alignments
+++++++++++++++++++

A utility merge SAM mapping records into a single, well-formatted SAM file.  You can
also use it to merge and copy to a local file system.


Usage::

  bin/merge_alignments [options] --annotations=file://reference.ann input_dir output_file.sam

Options::

 --sort-order <sort order> A valid SAM sort order.  Default:  coordinate.
 --rg-cn <center>          Read group center
 --rg-dn <date>            Read group date
 --rg-id <ID>              Read group id
 --rg-lb <library>         Read group library
 --rg-pl <platform>        Read group platform
 --rg-pu <pu>              Read group platform unit
 --rg-sm <sample>          Read group sample



distcp_files
+++++++++++++++

A utility useful for uploading files to HDFS.

Under the hood, it uses ``hadoop distcp``, but it avoids you having to prepend
``file://`` to local files.

Usage::

  bin/distcp_files filenames*  hdfs_dest_directory


align_script
+++++++++++++

A script to perform read alignment using libbwa (the same one used by Seqal).

Usage::

  bin/align_script [OPTIONS] --reference=REFERENCE PRQFILE PRQFILE

You can also pipe it input in prq format via stdin.


find_fq_format
+++++++++++++++

Reads a fastq file and tries to determine whether the base qualities are encoded
in Sanger or Illumina format.


prq_local
++++++++++++

Reformat a pair of qseq files (one for reads 1, one for reads 2) into a prq file.
Read mates have to appear on the same line of their respective files.

Usage::

  bin/prq_local reads1.qseq reads2.qseq output.prq

prq_to_fastq
++++++++++++++

Split a prq file into two fastq files.

Usage::

  bin/prq_to_fastq input.prq reads1.fastq reads2.fastq


realign_snp
++++++++++++++

Realign SNPs.  See the documentation in the script file for details.

