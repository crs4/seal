.. _seqal_options:

Seqal Options
=============

Seqal has a number of options that configure its operation.  You may need to
change some of the default values to make Seqal work for you.  For the time
being, values for these settings must be set in the ``run_seqal.sh`` script.  We
will implement a separate configuration file in a future release.


bl.seqal.log.level 
  Logging level. This value is used to configture the Python logging module
  used by Seqal.  Its value must be one of:  CRITICAL, ERROR, WARNING, INFO, 
  DEBUG.  Default value:  'INFO'.

bl.seqal.alignment.max.isize
  If the inferred insert size is greater than this value, Smith-Waterman alignment
  for unmapped reads will be skipped.  Default value:  1000.

bl.seqal.pairing.batch.size
  The number of sequences to be processed at a time by alignment functions.  
  The batch size influences the insert size statistics (bigger should give more
  accurate estimates).  However, increasing the batch size increases memory
  usage, and increasing it too much will result in Hadoop task timeouts.
  Default value:  10000.

bl.seqal.fastq-subformat
  Specifies base quality score encoding.  Supported values are 'fastq-sanger'
  for base qualities encoded in Sanger Phred+33 format (ASCII range 33-126) and
  'fastq-illumina' base qualities encoded in Illumina Phred+64 format 
  (ASCII range 64-126).  Default value: 'fastq-illumina'.

bl.seqal.min_hit_quality
  Minimum mapping quality (mapq) score.  Mappings with mapq below this 
  threshold will be discarded.  Default value:  1.

bl.seqal.remove_unmapped
  Discard unmapped reads.  MUST be set to 'true' for rmdup.  Default value:
  true.

bl.seqal.discard_duplicates
  Discard duplicate reads.  If true, Seqal will only keep the duplicate read or
  read pair with the best average base quality.  If false, the duplicates will
  be marked by setting the duplicate bit (0x0400) in the SAM flag.  Default
  value:  false.

bl.seqal.nthreads
  Number of threads to use when finding the SA coordinates of the input reads.  
  We recommend this value be left at 1.  To take advantage of multi-core machines
  configure appropriately the number of Hadoop map tasks per node.  
  Default value:  1.

mapred.reduce.tasks
  This option is set through the ``run_seqal.sh`` command line arguments.  
  Number of Hadoop reduce tasks to launch.  If this property is set
  to 0 then Seqal will run in "alignment-only" mode.  If set to a value greater
  than 0 then Seqal will run both alignment and rmdup duplicates removal phases.
  See the :ref:`faq` for information on deciding how may reduce tasks to use.


mapred.cache.archives
  Don't modify this.

mapred.create.symlink
  Don't modify.  Must be set to 'yes'.
