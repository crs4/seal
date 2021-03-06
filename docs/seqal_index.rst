.. _seqal_index:

Seqal
======


Seqal is a distributed short read mapping and duplicate removal tool.  Seqal
uses the RAPI_ aligner plug-in, which wraps the BWA-MEM_ aligner.  It also adds
duplicate read identification using the same criteria as the `Picard
MarkDuplicates`_ command (see below for details).

Usage
++++++

Like BWA, Seqal takes as input read pairs to be aligned, and an indexed
reference sequence.  The input read pairs need to be in :ref:`prq format
<file_formats_prq>` (PairReadsQSeq may help you format them quickly).

Preparing the reference index archive
-------------------------------------

Seqal needs the indexed reference generated by BWA-MEM.  See the BWA docs or
RAPI documentation for instructions.

One you have indexed your reference, you have two options:

#. Put the reference index on a shared storage volume mounted on all Hadoop nodes.
   In this case, you can reference the index with the ``--ref-prefix``.  Give the
   entire absolute path including the entire common part of all the reference
   file names (generally, you need to remove the first file extension); e. g.,::

    ref.fa.ann  ref.fa.bwt  -> ref.fa

#. Build an archive containing the index files at the top level and put it in
   Hadoop's distributed cache with the ``--ref-archive`` argument::

    tar cf ref.tar ref.fa.*


The latter strategy may be advantageous if you think you may repeatedly use the
same reference before it gets evicted from the Hadoop cache.  However, it will
increase the start-up time of the first run, since Hadoop will have to untar the
archive before starting the job.  Also, we recommend not compressing the archive
since we have found that decompressing it adds several minutes to the start-up
time, even if copying the file may be faster.  Using a light compression (e.g.
``gzip --fast``) may be the optimal solution but we have yet to test it.


Running the application
-----------------------

To run seqal, use the ``seal seqal`` command::

  seal seqal [ --ref-prefix | --ref-archive ] INPUT OUTPUT

The command takes the following arguments:

#. Reference index (see previous section).

#. Input path (a file or a directory), containing paired sequence data in prq
   format.  If the path references a directory, then all the files inside it
   will be processed.

#. Output directory, where results in SAM format will be written.



Command line options
.......................

======= =============== =========================================================
 Short  Long             Description
======= =============== =========================================================
 -q Q   --trimq Q       trim quality, like BWA's -q argument (default: 0).    
 -a     --align-only    Only perform alignment and skip duplicates detection  
                        (default: false).                                     
 -i     --input-format  Input format (:ref:`prq <file_formats_prq>`, :ref:`bdg <file_formats_bdg>`)
 -o     --output-format Output format (:ref:`sam <file_formats_hl_sam>`, :ref:`bdg <file_formats_bdg>`, :ref:`avo <file_formats_avo>`)
======= =============== =========================================================

Seqal also provides a number of properties to control its behaviour.
For a full description see the :ref:`seqal_options` page.

In addition to the mandatory arguments and options listed here, Seqal supports
the usual Seal command line options.  See the :ref:`program_usage` section for
details.

Input Formats
..................



Criteria for duplicate reads
++++++++++++++++++++++++++++++

The criteria applied by Seqal (and by Picard at the time of this writing) to
identify duplicate reads roughly equates to aligned finding reads (or pairs)
whose mappings start at the same reference position.

These are the steps that explain the criteria in more detail.

1. Find the mapping orientation for each read
----------------------------------------------

Each read can be mapped on the forward or reverse strand.


2. Find untrimmed mapping position
--------------------------------------

For each read, we're look for the reference position of the
first base from each read (in the example below, ``S1`` and ``S2``).

Example
..........

Take a single fragment that we're going to sequence::


             S1                                           S2
  sequence   AAACCCGGGTTTAAAGTTCAAGCAATTCTCACCTCCACCTTCCAGAACCGGTTAACCGGT
             |-------------|                              |-------------|
                  Read 1                                       Read 2

Suppose the last 3 bases from Read 1 are trimmed::


                          | trimmed
             S1           v 
             AAACCCGGGTTTaaa
             |-------------|
                  Read 1


If Read 1 is mapped on the forward strand, the reference position ``M`` of ``S1``
is then simply the mapping position reported by the aligner for this read::

             M
             S1
             AAACCCGGGTTTaaa
             |-------------|
                  Read 1

If instead Read 1 is mapped to the reverse strand, its mapping position will
refer to its last bases, since the read is reversed (and complemented)::


                M         S1
             aaaTTTGGGCCCAAA
             |-------------|
                  Read 1

Therefore, to find our "start" position ``S1`` we'll have to look at the
alignment (through the CIGAR string) and find the reference position of the
right-most read (note that for simplicity we didn't complement the bases in the
example above).  We'll use the reference position of ``S1`` when deciding
whether this read has duplicates.


Dealing with Read 2 with trimming can be a little more complicated, since the
trimming happens at the ``S2`` side of the read.  Consider Read 2 with 4 bases
trimmed and mapped to the forward strand::

             S2  M
             aaccGGTTAACCGGT
             |-------------|
                  Read 2

In this case the alignment reports the reference position of the first ``G``,
which is the 5th base in the read.  To find the reference position of ``S2`` we
have to count backwards.  The number of position to back up is indicated by the soft
and/or hard clipping operations in the CIGAR---for the example above it could be
``4S11M``, so we would need to subtract 4 positions from our alignment
coordinate.

The final case is Read 2 on the reversed strand.  Again, in this case the read
is reversed and complemented so ``S2`` is on the "right" and the mapping position ``M`` is on
the other end::

             M            S2
             TGGCCAATTGGccaa
             |-------------|
                  Read 2

We therefore have to count forward, including any trimmed bases to find our
canonical position ``S2`` which will be used to evaluate duplicates.


In the end, for any given read we will have its corresponding start coordinate
``S``.


3. Find pairs with identical orientation and coordinates
------------------------------------------------------------------

Match the pairs by whether they align on the same strand and by the
reference coordinates of start of each read, ``S1`` or ``S2``, from step (2).
With this criteria we identify sets of equivalent reads.

.. note:: To calculate the equivalency classes of reads we form a key ``(S1,
          orientation read 1, S2)``.  All pairs which result in identical
          instances of this tuple will be considered duplicates.

Given a set of pairs, leave the pair with the highest base qualities as is,
while we label the rest as duplicates.

To decide which pair has the best quality, we sum all base qualities >= 15.  The
pair with the highest sum "wins" (we implicitly assume reads have the same
length).

4. Identify duplicate unpaired reads
----------------------------------------

For unpaired reads (or reads whose mate is unmapped), if the read's ``S``
coordinate (as in step 2) and mapping orientation falls on a paired read, it
will be marked as a duplicate---i.e.  paired reads are given precedence.

If instead for a particular coordinate and orientation we only find unpaired
reads, then we apply the same base quality-based criteria that we used for
pairs:  the one with the highest ``sum( base qualities >= 15 )`` is left as is,
while the rest are marked as duplicates.

Unmapped reads
--------------------

Unmapped reads cannot be marked as duplicates, since our criteria for
identifying duplicates is based on mapping coordinates.  Seqal does not try to
match reads by identical nucleotide sequence.



.. _RAPI: https://github.com/crs4/rapi
.. _BWA-MEM:  http://bio-bwa.sourceforge.net/
.. _Picard MarkDuplicates:  http://sourceforge.net/apps/mediawiki/picard/index.php?title=Main_Page#Q:_How_does_MarkDuplicates_work.3F
.. _BWA manpage: http://bio-bwa.sourceforge.net/bwa.shtml
