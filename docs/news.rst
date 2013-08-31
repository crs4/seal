.. _news:

News
===================================

New in 0.4.0
---------------------------------

Hadoop-BAM
++++++++++++

This version of Seal depends on the
`Hadoop-BAM <http://sourceforge.net/projects/hadoop-bam/>`_ library for much of
its I/O functionality (specifically Fastq, Qseq, and BAM input and output).
This change entails a few things of which you need to be aware.

Changes in property names
............................

The properties that configured fastq and qseq input or output have changed name.
Here is a full list:

======================================== ===========================================================
**Old property**                         **Replacement**
---------------------------------------- -----------------------------------------------------------
seal.fastq-input.base-quality-encoding    seal.input.base-quality-encoding
seal.qseq-input.base-quality-encoding     seal.input.base-quality-encoding
seal.qseq-output.base-quality-encoding    not available
======================================== ===========================================================

Note that the **old property names are no longer supported** and Seal **will not
warn you if you try to use them**.


Hadoop-BAM jars
.....................

You will need to make the Hadoop-BAM jars available to the Seal apps running on
your cluster.  See `this page <http://www.cloudera.com/blog/2011/01/how-to-include-third-party-libraries-in-your-map-reduce-job/>`_
for useful instructions.  If you like, you can use the ``-libjars`` option with Seal
applications.



Repackaging
+++++++++++++

This version repackages Seal in a more conventional way and partly automates
installation with PyPi.  As a result, the names of all the Seal commands have
changed.  We now only have a single command, ``seal``, which in turn has many
subcommands.

============================  ========================
**Old name**                   **New name**
----------------------------  ------------------------
bwa_index_to_mmap              seal bwa_index_to_mmap
demux                          seal demux
distcp_files                   seal distcp_files
merge_alignments               seal merge_alignments
prq                            seal prq
read_sort                      seal read_sort
recab_table                    seal recab_table
recab_table_fetch              seal recab_table_fetch
seqal                          seal seqal
tsvsort                        seal tsvsort
version                        seal version
============================  ========================

Also, the names of the Python packages in Seal have changed (the root was ``bl``
but is now ``seal``).  This change will not affect you unless you were using Seal
modules from your own scripts or if you want remove seal---you'll now have to
remove the ``seal`` directory instead of the ``bl`` directory.


Easier installation
++++++++++++++++++++++

We've made installing Seal easier.

Once you install all the dependencies and Python pip with your package manager (see
the :ref:`installation <installation>` page), you can now install Pydoop and
Seal with a simple command::

  pip install seal


Running Seal tools
++++++++++++++++++++

The way to run the Seal tools if you don't install them to the system (e.g., you
build Seal but don't install it) has changed slightly.  In that case, you now 
*have to* set PYTHONPATH to include the Seal build directory.  This setting is
not necessary if you install Seal to one of the standard system locations.


New in 0.3.0
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


Changes in property names
+++++++++++++++++++++++++++++++

A number of property names have been changed.  We have migrated from the old
"bl."-style naming, which we had for historic reasons, and have moved to a new
"seal." naming scheme.

Deprecated property names should still work with this version, but will be
removed from new ones.  You are urged to updated your configuration files and/or
scripts, especially since *Seal ignores properties it does not recognize*, so
misnamed properties do not normally result in an error message.

Here is a list of the deprecated property names.

======================================== ===========================================================
**Deprecated property**                   **Replacement**
---------------------------------------- -----------------------------------------------------------
bl.prq.min-bases-per-read                 seal.prq.min-bases-per-read
bl.prq.drop-failed-filter                 seal.prq.drop-failed-filter
bl.prq.warning-only-if-unpaired           seal.prq.warning-only-if-unpaired
bl.seqal.log.level                        seal.seqal.log.level
bl.seqal.alignment.max.isize              seal.seqal.alignment.max.isize
bl.seqal.pairing.batch.size               seal.seqal.pairing.batch.size
bl.seqal.fastq-subformat                  seal.seqal.fastq-subformat
bl.seqal.min_hit_quality                  seal.seqal.min_hit_quality
bl.seqal.remove_unmapped                  seal.seqal.remove_unmapped
bl.seqal.discard_duplicates               seal.seqal.discard_duplicates
bl.seqal.nthreads                         seal.seqal.nthreads
bl.seqal.trim.qual                        seal.seqal.trim.qual
bl.seqal.log.level                        seal.seqal.log.level
bl.seqal.discard_duplicates               seal.seqal.discard_duplicates
======================================== ===========================================================



prq files now always use `sanger` quality encoding
++++++++++++++++++++++++++++++++++++++++++++++++++++++

The :ref:`prq file format <file_formats_prq>` now has been defined as using
the Sanger Phred+33 quality encoding.  Therefore, :ref:`PairReadsQSeq <prq_index>`
now produces Sanger qualities and Seqal by default expects Sanger qualities.



Seqal default quality encoding is now `sanger`
++++++++++++++++++++++++++++++++++++++++++++++++

Since, as just mentioned, :ref:`prq files <file_formats_prq>` now contain base
quelities in Sanger Phred+33 encoding,  we've changed the default base quality
encoding expected by :ref:`Seqal <seqal_index>` from Illumina Phred+64 to
Sanger Phred+33.

You can get the old behaviour by setting
`-D seal.seqal.fastq-subformat=fastq-illumina` when you call ``seqal``.


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

  ./bin/seqal -D seal.seqal.trim.qual=15 input output

or::

  ./bin/seqal --trimq 15 input output

Now the trim quality parameter is the configuration property ``seal.seqal.trim.qual`` that can
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
seal.seqal.min_hit_quality                     1             0
seal.seqal.remove_unmapped                   True          False
====================================  ===============  ================


Let PRQ discard unpaired reads
+++++++++++++++++++++++++++++++

PRQ used to stop with a (rather cryptic) error if it encountered an unpaired
read in the input data.  By default it still does that, although we think we've
somewhat improved the error message.  However, if you prefer you can tell it to
discard the unpaired reads with a warning::

  ./bin/prq -D bl.prq.warning-only-if-unpaired=true input output



.. _ProgramUsage: :ref:program_usage
