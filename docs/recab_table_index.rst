.. _recab_table_index:

RecabTable
===================

RecabTable is a Hadoop program to calculate a table of base qualities for all values of 
a given set of factors.  It computes a result equivalent to the `GATK CountCovariatesWalker 
<http://www.broadinstitute.org/gsa/gatkdocs/release/org_broadinstitute_sting_gatk_walkers_recalibration_CountCovariatesWalker.html>`_ 
in the Seal framework.

What it does
+++++++++++++++

RecabTable looks at all the mappings in the input data set.  It discards
mappings that are:

- unmapped
- have a mapq of 0
- are marked as duplicates, having failed QC, or as secondary alignments.

For the remaining reads, RecabTable looks at all the bases that have been matched to a
reference coordinate (CIGAR 'M' operator).  For these bases, it skips the ones
that are aligned to a known variation site, that have a base quality score
of 0 or that are not determined (N values).  For the rest, it computes the
values of the selected covariates and, for each combination of values,
notes whether the base is a match or a mismatch to the reference.

RecabTable sums together all the identical combinations of covariate values 
observed, giving a total number of times each combination was observed in the
input data set and how many of those times the base was a match or mismatch to
the reference.


Covariates
................

The following covariates are used by RecabTable:

- Read group
- Base quality score
- Sequencing cycle
- 



Usage
+++++++

To run RecabTable, launch ``bin/recab-table``.  You will need to provide
input and output paths, and a database of known variation sites in ROD or VCF
format.  For example,

::
  ./bin/recab-table --vcf-file dbsnp.vcf sam_directory recab_table_output

Input files must be in SAM format without a header (like the ones produced by
Seqal and ReadSort).  Output files are all CSV, without a header.

To assemble a single CSV that can be used by GATK TableRecalibration use::

  hadoop dfs -cat recab_table_output/part-* > recab_table.csv

or::

  hadoop dfs -getmerge recab_table_output/part-* recab_table.csv


``recab-table`` follows the normal Seal usage convention.  See the section
:ref:`program_usage` for details.




Configurable Properties
++++++++++++++++++++++++++

========================================== ==========================================================
**Name**                                    **Meaning**                                             
------------------------------------------ ----------------------------------------------------------
seal.recab.rg-covariate.default-rg          Read group to assign to mappings without an RG tag.      
seal.recab.smoothing                        Smoothing parameter for empirical quality calculation    
                                            (default: 0).                                            
seal.recab.max-qscore                       Upper limit for the empirical quality scores             
                                            (default: 40).                                           
seal.recab.skip-known-variant-sites         Don't use known variants DB (for testing purposes).
========================================== ==========================================================

In addition, all the general Seal and Hadoop configuration properties apply.

.. note:: **Config File Section Title**: RecabTable


Counters
+++++++++++

RecabTable has a number of counters to help you monitor what it's doing.  Here's
an explanation of what they mean.

Read Counters
..................

============================ ===========================================================
**Counter name**              **Explanation**
---------------------------- -----------------------------------------------------------
Processed                     Reads seen by RecabTable.
Filtered                      Reads seen and discarded.
FilteredUnmapped              Reads seen and discarded because they were unmapped.
FilteredMapQ                  Reads seen and discarded because they had a mapq of 0
FilteredDuplicate             Reads seen and discarded because they were marked as 
                              duplicates.
FilteredQC                    Reads seen and discarded because they were marked
                              as having failed QC.
FilteredSecondaryAlignment    Reads seen and discarded because they were marked
                              as secondary alignments.
============================ ===========================================================


Base Counters
...................

======================== ===========================================================
**Counter name**         **Explanation**
------------------------ -----------------------------------------------------------
Used                      Bases used in table calculation
BadBases                  Bases skipped because they were unreadable (N) or base 
                          quality was 0.
SnpMismatches             Base not matching the reference at a known variant
                          location.
SnpBases                  Reference mismatch at a known variant location.
NonSnpMismatches          Reference mismatch at a regular location.
======================== ===========================================================



Differences from GATK CountCovariates
+++++++++++++++++++++++++++++++++++++++

RecabTable produces results almost identical to GATK CountCovariates, but there
are some small differences.

Read adapter clipping
........................

While unusual, it can happen that a sequenced template is shorted than the read
itself.  In this case, the sequencer ends up reading part of the read adapter.
Both GATK and Seal RecabTable take this into account, but GATK as of version 
1.2-24-g6478681 has a `small bug which causes it to clip the wrong bases in some
circumstances
<http://getsatisfaction.com/gsa/topics/understanding_when_countcovariates_skips_bases>`_.
The GATK developers know about this issue and will surely address it quickly.
However, at the moment this causes small differences in the covariates produced
and the number of bases used by the two tools given the same input.  In any
case, this effect should be negligible for most sequencing runs.





Limitations
++++++++++++++++

Currently, the set of covariates used by RecabTable is hard-coded and thus 
cannot be altered without editing the code and recompiling Seal.  If you would
like this feature to be added soon please let the Seal developers know by filing
a feature request through `the Seal web site
<http://sourceforge.net/tracker/?group_id=536922&atid=2180423>`.


