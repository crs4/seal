.. _news:

News
===================================

New in this release
-----------------------

RecabTable program
+++++++++++++++++++++++

A new program has been added to the Seal suite:  :ref:`RecabTable <recab_table_index>`.  RecabTable computes a result equivalent to the 
`GATK CountCovariatesWalker <http://www.broadinstitute.org/gsa/gatkdocs/release/org_broadinstitute_sting_gatk_walkers_recalibration_CountCovariatesWalker.html>`_,
but does so in a scalable way by taking advantage of your Hadoop cluster.

See the :ref:`RecabTable <recab_table_index>` page for all the details.


PairReadsQSeq can now also read fastq
++++++++++++++++++++++++++++++++++++++++

In particular, :ref:`PairReadsQSeq <prq_index>` can read the meta-infomation in the fastq files 
produced by the new version of CASAVA, and it should also be able to cope with
generic fastq files as long as the trailing "/1" or "/2" is present in the read
id to indicate the read number.



prq files now always use `sanger` quality encoding
++++++++++++++++++++++++++++++++++++++++++++++++++++++

The :ref:`prq file format <file_formats_prq>` now has been defined as using
the Sanger Phred+33 quality encoding.  Therefore, :ref:`PairReadsQSeq <prq_index>` now produces Sanger qualities and Seqal by default expects Sanger qualities.



Seqal default quality encoding is now `sanger`
++++++++++++++++++++++++++++++++++++++++++++++++

We've changed the default base quality encoding expected by :ref:`Seqal
<seqal_index>` from Illumina Phred+64 to Sanger Phred+33.  The reason for the
change is that :ref:`PairReadsQSeq <prq_index>` now generates prq files using
the Sanger encoding, and Illumina itself is moving to Fastq files using the
Sanger encoding.

You can get the old behaviour by setting
`-D bl.seqal.fastq-subformat=fastq-illumina` when you call ``seqal``.


TsvSort utility
+++++++++++++++++++

More than a simple utility, TsvSort is a Hadoop program for sorting text files
based on the Terasort algorithm. It is a scalable, fast, distributed sorting
application.  It allows a use pattern similar to the Unix ``sort`` utility,
allowing you to specify a field delimiter and which fields to use as keys.

See the :ref:`TsvSort <tsv_sort_index>` page for details.



Bug fixes and usability
++++++++++++++++++++++++++++++

A few bug fixes and usability improvements are also introduced by this release.

* when an error in the input file format is encountered, the tools now try to tell
  you exactly in which file and line the problem occurred.

* Seqal logging and error reporting has been fixed.  In particular, when a usage
  error occurred with Seqal the program blurted a rather unhelpful message such
  as ``Error running seqal``.  We had a problem that was causing the actual
  error message to be lost.  That should be fixed now.





New in 0.2.3
---------------

Improved MergeAlignments
+++++++++++++++++++++++++++

The MergeAlignments utility provided to merge multi-part output from Seal tools
now has a couple of additional features:

* Reference checksums
* additional SAM header tags

See the :ref:`merge_alignments_index` documentation for details.


New in 0.2.2
------------------

Seqal now integrates BWA 0.5.9
++++++++++++++++++++++++++++++++++++

We updated the Seqal distributed alignment tool to include the alignment code
from BWA 0.5.9.

New configuration system
+++++++++++++++++++++++++++

You can now store your usual Seal run configuration in a separate config file 
(by default, ``$HOME/.sealrc``).  All programs in the Seal suite will now read that
file if it exists.  You can also specify your own configuration file name,
allowing you to easily have a number of preset run configurations.  In 
addition, you can now specify all options directly on the command line
(overriding default and file settings).

For more details, see the section :ref:`seal_config`.



Changes names of executables
+++++++++++++++++++++++++++++

============================  ======================
**Old name**                   **New name** 
----------------------------  ----------------------
bin/run_prq.sh                 bin/prq
bin/run_seqal.sh               bin/seqal
bin/merge_sorted_alignments    bin/merge_alignments
============================  ======================


Multiple inputs
+++++++++++++++++++

All Seal Hadoop commands except Seqal now accept multiple input paths.  The
generic command line is::

  tool [ options ] <input 1> <input 2>...<input N> <output>

Seqal unfortunately can only take a single input path for now.  This is due to a limitation in the
Hadoop pipes command line interface.



Changes in command line tool usage
++++++++++++++++++++++++++++++++++++

We have made the command line interface of the Seal tools more consistent.  This
change mainly affects PairReadsQSeq and Seqal.  We describe this new command line interface
in the section on :ref:`program_usage` section.

Prq
........

In addition to changing the name of the command from ``run_prq.sh`` to ``prq``,
we have also changed the arguments ``prq`` accepts.

Old::

  ./bin/run_prq.sh input output 54

where 54 was an optional argument to override the minimum number of required
bases for a read to avoid filtering.

New::

  ./bin/prq -D bl.prq.min-bases-per-read=54 input output

Now the parameter is a configuration property that can 
be specified on the command line or the new `Seal configuration file <seal_config>`.  
PairReadsQSeq configuration properties are documented in the section :ref:`prq_index`


Seqal
.........

In addition to changing the name of the command from ``run_seqal.sh`` to ``seqal``,
we have also changed the arguments ``seqal`` accepts.

Old::

  ./bin/run_seqal.sh input output reference 15

where ``15`` was an optional argument to control read trimming.

New::

  ./bin/seqal -D bl.seqal.trim.qual=15 input output

or::

  ./bin/seqal --trimq 15 input output

Now the trim quality parameter is the configuration property ``bl.seqal.trim.qual`` that can 
be specified on the command line or the new :ref:`Seal configuration file <seal_config>`.  
In addition, Seqal provides a shortcut ``--trimq`` argument.
Seqal configuration properties are documented in the section :ref:`seqal_options`.



Changes to default values
+++++++++++++++++++++++++++++

Note the changes to the default values of these Seqal options.  They may affect
your workflow.

====================================  ===============  ================
**Parameter**                          **Old value**    **New value** 
------------------------------------  ---------------  ----------------
bl.seqal.min_hit_quality                     1             0
bl.seqal.remove_unmapped                   True          False
====================================  ===============  ================


Let PRQ discard unpaired reads
+++++++++++++++++++++++++++++++

PRQ used to stop with a (rather cryptic) error if it encountered an unpaired
read in the input data.  By default it still does that, although we think we've
somewhat improved the error message.  However, if you prefer you can tell it to
discard the unpaired reads with a warning::

  ./bin/prq -D bl.prq.warning-only-if-unpaired=true input output



.. _ProgramUsage: :ref:program_usage
