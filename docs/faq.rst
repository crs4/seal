.. _faq:

FAQ
=====

Are the alignments produced by Seal equivalent to this produced by BWA?
----------------------------------------------------------------------------

Yes.  Seal internally uses the alignment code from BWA (version 0.5.8c as of
Seal 0.1.0).  


To verify the correctness of Seal's output, we aligned a data set consisting of
5 million read pairs (the first 5M from run id ERR020229 of the 1000 Genomes 
Project [#durbin]_) to the UCSC HG18 reference genome [#fujita]_ with both Seal
and BWA ver. 0.5.8c.  With BWA, we ran ``bwa aln`` and ``bwa sampe``, while
with Seal we ran the PairReadsQseq and Seqal applications.

We compared the resulting mappings, and observed that the result was identical 
for 99.5% of the reads.  The remaining 0.5% had
slightly different map quality scores (mapq), while the mapping coordinates
were identical for all but two reads. The latter two cases both had multiple
best hits, but resulted in different alignment choices probably due to insert
size statistics, in turn due to the particular input read batch. Slight differences
in mapq scores are expected because their calculation takes into account the
insert size statistics, which are calculated from windows of the sample of
sequences BWA analyses. Since the sample windows seen by the command
line version of BWA and Seal are different for each read, a slight change
in the mapq value is expected. To verify this hypothesis, we ran BWA with
varying input data sets while keeping 3000 of those reads that produced
mapq variations in the original experiment. We observed that the mapq
values for those reads varied between runs.



Can I output a file in BAM format?
-------------------------------------

For the moment, you can't generate BAM files from directly from the Hadoop jobs,
but you can create one on-the-fly as you download your output from HDFS.  

For instance, you can merge and download all part SAM files with
``merge_sorted_alignments``::

  bin/merge_sorted_alignments --annotations=file://${RefPath}.ann read_sort_output_dir 
  
The command above will write a proper SAM to standard output.  Therefore, you
can pipe it to samtools, and have it generate a BAM on-the-fly::

  bin/merge_sorted_alignments --annotations=file://${RefPath}.ann read_sort_output_dir | \
    samtools view -bST  ${RefPath}.fai /dev/stdin -o final_output.bam

Unfortunately this method is relatively slow, because the BAM is created serially on
one machine.  An ideal solution would be to have ReadSort optionally output
parts of the BAM file, whose computation would be distributed and thus fast, and
then merge those parts at the end as necessary.  Alas we haven't implemented 
this solution yet.



How do I decide how many reduce tasks to use?
-----------------------------------------------

You should follow the standard Hadoop advice to set the number of reduce tasks
in which to split your problem.  Generally, it should be a multiple of the
number of reduce tasks your cluster can run simultaneously, so that they may 
all finish in one iteration.  For instance, if your cluster is configured with 5
reduce slots per node, and has 10 nodes, try using 50 reduce tasks.

If the input is too big, you may find that the reducers require too much memory
to complete the job in one reduce iteration.  In this case, double the number of 
reduce tasks to 100.

Finally, we have found it useful to subtract a small percentage from the total
number of reduce tasks calculated following the instructions above.  In this way
we leave a few free slots to re-run tasks that may fail, keeping stragglers at
the end from disproportionately increasing run times (and from time to time some 
tasks will fail and be re-started automatically by Hadoop).

``run_seqal.sh`` by default uses 6 reduce tasks per active node.  You can override 
that value from the command line if you wish.


.. [#durbin] Durbin, R. M., Altshuler, D. L., et al. (2010). A map of human genome variation from population-scale sequencing. Nature, 467(7319), 1061–1073.
.. [#fujita] Fujita, P. A., Rhead, B., et al. (2010). The UCSC Genome Browser database: update 2011. Nucleic Acids Res.

