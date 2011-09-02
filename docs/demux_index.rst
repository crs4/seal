.. _demux_index:

Demux -- QSeq demultiplexer 
=============================

Demux is a Hadoop utility to demultiplex data from multiplexed Illumina
runs.  

Multiplexed runs are used to sequence multiple samples together.  
Each sample's fragments are tagged with a barcode sequence, which is annotated 
in a sample sheet.  Illumina's base calling software then emits 3 reads for
each DNA/RNA fragment:

1. read 1
2. barcode
3. read 2

Illumina provides a utility to separate the multiplexed reads by barcode, but it
is a simple serial implementation.  Seal Demux replaces this utility with a
Hadoop-based implementation which can drastically reduce run times when run on a
cluster and can leverage HDFS storage like the rest of the Seal tools.

Demux load the sample sheet and in particular keeps track of which lane/barcode
combination is associated with each sample.  For each flow-cell location
present in the input, Demux gets the barcode (read 2), looks it up in the sample
sheet and assigns the reads from that location to the appropriate sample.

*Currently, Demux cannot handle errors in the barcode sequence.*  An error in
the barcode will most likely result in the reads being placed in the "unknown"
bucket.



Usage
+++++

To run Demux, you first need to copy the sample sheet to HDFS.

::

  hadoop dfs -put sample_sheet.csv /user/me/

Then, run the ``bin/demux`` command in the Seal distribution::

  ./bin/demux --sample-sheet /user/me/sample_sheet.csv /user/me/qseq_input /user/me/demuxed_output


The arguments are:

#.  ``--sample_sheet``:  the HDFS path to the sample sheet file;
#. Input paths:  files or directories, containing files for reads 1, 2 and 3 in qseq_ format;
#. Output path:  path to directory where demux will write its output.  This directory must not already exist; demux will create it.

``demux`` follows the normal Seal usage convention.  See the section
:ref:`program_usage` for details.



Output
++++++++++

At the specified output path, ``demux`` creates a directory for each sample
represented in the input.  The directory name corresponds to the sample id in
the sample sheet.  Inside the sample's directory you'll find regular qseq files,
where the records with read number 3 from the input have been shifted to read
number 2.

In addition, you may find an ``unknown`` directory containing qseq files with
all the reads whose barcode was not found in the sample sheet.

::

  $ hadoop dfs -ls /user/me/demuxed_output
  Found 5 items
  drwxr-xr-x   - me supergroup          0 2011-06-23 17:17 /user/me/demuxed/_logs
  drwxr-xr-x   - me supergroup          0 2011-06-23 17:29 /user/me/demuxed/snia_000186
  drwxr-xr-x   - me supergroup          0 2011-06-23 17:29 /user/me/demuxed/snia_000268
  drwxr-xr-x   - me supergroup          0 2011-06-23 17:29 /user/me/demuxed/snia_000269
  drwxr-xr-x   - me supergroup          0 2011-06-23 17:29 /user/me/demuxed/unknown


Sample Sheet
++++++++++++++

The sample sheet is a table in comma-separated value format with the following
colums, in order:

:FCID:
	flow-cell ID

:Lane:
	flow-cell lane

:SampleID:
	name of the sample

:SampleRef:
	sample reference

:Index:
	barcode used to tag this sample, excluding the last base, A

:Description:
	whatever you want

:Control:
	N

:Recipe:
	experimental protocol

:Operator:
	operator name

Here's an excerpt from a sample sheet file::

"FCID","Lane","SampleID","SampleRef","Index","Description","Control","Recipe","Operator"
"b02tgkkio",1,"csbb_001234","Human","ATCACG","Sequencing Project","N","tru-seq multiplex","Peter"
"b02tqacee",1,"csbb_004312","Human","CGATGT","Sequencing Project","N","tru-seq multiplex","Peter"



Counters
+++++++++++


In addition to the counters from the Hadoop framework, Demux counts the number
of reads found for each sample, and the unknowns.  You'll find them in the
*Sample reads* counter group.


Configurable Properties
++++++++++++++++++++++++++

Demux does not have any program-specific configurable properties at the
moment.  You can still use its section to configure Hadoop property values
specific to Demux.

.. note:: **Configuration Section Title**: Demux


.. _qseq: file_formats.html#qseq-file-format-input
