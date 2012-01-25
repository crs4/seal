.. _seqal_options:

Seqal Properties
=================

Seqal has a number of properties that configure its operation.  You may need to
change some of the default values to make Seqal work for you.  This settings can
be set on the command line with the ``-D`` option (see :ref:`program_usage` for 
details) or they can be set in a Seal configuration file (see
:ref:`seal_config`).


.. note:: **Config File Section Title**: Seqal


seal.seqal.log.level 
  Logging level. This value is used to configure the logging module
  used by Seqal.  Its value must be one of:  CRITICAL, ERROR, WARNING, INFO, 
  DEBUG.  Default value:  'INFO'.

seal.seqal.alignment.max.isize
  If the inferred insert size is greater than this value, Smith-Waterman alignment
  for unmapped reads will be skipped.  Default value:  1000.

seal.seqal.pairing.batch.size
  The number of sequences to be processed at a time by alignment functions.  
  The batch size influences the insert size statistics (bigger should give more
  accurate estimates).  However, increasing the batch size increases memory
  usage, and increasing it too much will result in Hadoop task timeouts.
  Default value:  10000.

seal.seqal.fastq-subformat
  Specifies base quality score encoding.  Supported values are 'fastq-sanger'
  for base qualities encoded in Sanger Phred+33 format (ASCII range 33-126) and
  'fastq-illumina' base qualities encoded in Illumina Phred+64 format 
  (ASCII range 64-126).  Default value: 'fastq-illumina'.

seal.seqal.min_hit_quality
  Minimum mapping quality (mapq) score.  Mappings with mapq below this 
  threshold will be discarded.  Default value:  0.

seal.seqal.remove_unmapped
  Discard unmapped reads.  Default value: false.

seal.seqal.discard_duplicates
  Discard duplicate reads.  If true, Seqal will only keep the duplicate read or
  read pair with the best average base quality.  If false, the duplicates will
  be marked by setting the duplicate bit (0x0400) in the SAM flag.  Default
  value:  false.

seal.seqal.nthreads
  Number of threads to use when finding the SA coordinates of the input reads.  
  We recommend this value be left at 1.  To take advantage of multi-core machines
  configure appropriately the number of Hadoop map tasks per node.  
  Default value:  1.

seal.seqal.trim.qual
  q-value for read trimming.  This is equivalent to the ``-q`` 
  `BWA option <http://bio-bwa.sourceforge.net/bwa.shtml>`_.  You can also
  specify the value of this property with the ``--trimq`` command line option.


Deprecated Properties
-------------------------

All the properties to configure Seqal have been renamed, from ``bl.seqal.*`` to
``seal.seqal.*``.  The old property names have been deprecated, but for the time
being will be accepted with a warning.  Future versions of Seal will ignored
them altogether, so you are urged to update your scripts or configuration files.
The table below presents the complete list of deprecated Seqal properties.


The following properties, recognized in previous versions of Seal, have been
deprecated and replaced.  They are still functional for the moment, but will be
completely removed in future versions so you are urged to update your
configurations and scripts.

================================== ===========================================================
**Deprecated property**             **Replacement**
---------------------------------- -----------------------------------------------------------
bl.seqal.log.level                  seal.seqal.log.level           
bl.seqal.alignment.max.isize        seal.seqal.alignment.max.isize 
bl.seqal.pairing.batch.size         seal.seqal.pairing.batch.size  
bl.seqal.fastq-subformat            seal.seqal.fastq-subformat     
bl.seqal.min_hit_quality            seal.seqal.min_hit_quality     
bl.seqal.remove_unmapped            seal.seqal.remove_unmapped     
bl.seqal.discard_duplicates         seal.seqal.discard_duplicates  
bl.seqal.nthreads                   seal.seqal.nthreads            
bl.seqal.trim.qual                  seal.seqal.trim.qual           
bl.seqal.log.level                  seal.seqal.log.level           
bl.seqal.discard_duplicates         seal.seqal.discard_duplicates  
================================== ===========================================================


