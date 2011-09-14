.. _merge_alignments_index:

MergeAlignments
====================

MergeAlignments is a utility to merge SAM mapping records into a single, 
well-formatted SAM file, which could be on the local file system.  It takes 
all the inputs specified on the command line
and sends them to the chosen output destination, after emitting the SAM header
based on the specified reference.

MergeAlignments interprets path wildcards (e.g. ``/user/me/part-*``) and sorts
by name the file names they produce.  Specifying a directory ``dir`` as input is
equivalent to specifying ``dir/*``.

The main motivation behind MergeAlignments is to provide a tool to merge
the ordered SAM mappings produced by :ref:`ReadSort<read_sort_index>` into a
well-formed SAM on the local file system for further processing by traditional 
tools.

MergeAlignments is not a MapReduce program!
++++++++++++++++++++++++++++++++++++++++++++++

Although MergeAlignments can access HDFS, it runs locally on the machine where
it is started.  Keep that in mind.  Also, it doesn't follow the 
:ref:`Seal usage convention <program_usage>` so at the moment it does not look
at the :ref:`seal_config` nor does it parse the standard command line options
(in particular ``-D`` to set configuration properties).  We hope to remedy this
limitation soon.


Usage
+++++++++

::

  bin/merge_alignments [options] input_dir+ output_file.sam

or, to write to standard output::

  bin/merge_alignments [options] input_dir+ 

Options
+++++++++++

::

 --reference <REF_PATH>        root path to the reference used to
                               create the SAM data
 --annotations <ref.ann>       annotation file (.ann) of the BWA
                               reference used to create the SAM data
                               (not required if you specify ref)
 --sort-order <sort order>     A valid SAM sort order.  Default:  coordinate.
 --rg-cn <center>              Read group center
 --rg-dt <date>                Read group date
 --rg-id <ID>                  Read group id
 --rg-lb <library>             Read group library
 --rg-pl <platform>            Read group platform
 --rg-pu <pu>                  Read group platform unit
 --rg-sm <sample>              Read group sample
 --sq-assembly <ASSEMBLY_ID>   Genome assembly identifier (@SQ AS:xxxx tag)
 --md5                         generated MD5 checksums for reference contigs

.. attention:: ``--sort-order`` for now by default takes the value 'coordinate' for
  backwards compatibility.  We recommend you specify your desired value 
  explicitly since this default may change in the future.

MD5
.......

If desired, MergeAlignments can insert the MD5 checksum of the reference contigs
into the SAM header, as permitted by the SAM format specification.  To enable
this feature specify the ``--md5`` command line option and provide the path
to the full reference fasta and annotations (the path of the annotations will be
assumed to be <ref>.ann if only the reference is provided).  Note that the
checksums are calculated on-the-fly so they will add a couple of minutes to the
processing time.

Sort order
............

The sort order of the data is not verified by MergeAlignments.  At the moment MergeAlignments by default assumes that the data is sorted by coordinate, which provides compatibility to previous versions of Seal.  We recommend that you explicitly specify the sort order of your data lest the default change to 'unordered'.


Examples
+++++++++

Simple invocation after ReadSort::

  ./bin/merge_alignments --sort-order coordinate --annotations "file://${RefPath}.ann" sort_output_dir file:///tmp/local_file.sam


Write a proper SAM from unsorted Seqal output back to HDFS::

  ./bin/merge_alignments --sort-order unsorted --annotations "file://${RefPath}.ann" seqal_output_dir merged_file.sam


Add RG tag and assembly id::

  ./bin/merge_alignments --sort-order coordinate --annotations "file://${RefPath}.ann"  --sq-assembly NCBIv37 \
    --rg-id "${Id}" --rg-sm "${SampleName}" --rg-cn "${Centre}" --rg-dt "${Date}" \
    "${ReadSortOutputDir}" file:///tmp/local_file.sam

Pipe into samtools to generate a BAM on-the-fly::

  ./bin/merge_alignments --sort-order coordinate --annotations "file://${RefPath}.ann" | \
    samtools view -bST  "${RefPath}.fai" /dev/stdin -o "${MergeOutputFile}"

Add MD5 checksums, write to a local SAM::

  ./bin/merge_alignments --sort-order coordinate --md5 --reference "file://${RefPath}" \
    "${ReadSortOutputDir}" > local_file.sam

