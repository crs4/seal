.. _demux_index:

Demux -- QSeq demultiplexer
=============================

Demux is a Hadoop progra to demultiplex data from multiplexed Illumina
runs.  Reads are demultiplexed by barcode and/or lane, according to the sample
sheet configuration.


Features
+++++++++

  * Barcode mismatches
  * Separate data by read number (in addition to project/sample)
  * Separate data for runs that aren't multiplexed (only by lane)
  * Input formats:  qseq_, fastq_
  * Output formats: qseq_, fastq_ (independent of input, so you can transcode qseq_
    to fastq_ or viceversa)
  * Output compression: part files can be automatically compressed


Usage
+++++

To run Demux, the sample sheet needs to be accessible from all nodes.  It's
easiest if you copy it to HDFS.

::

  hadoop dfs -put sample_sheet.csv /user/me/

Then, run the ``seal demux`` command in the Seal distribution::

  seal demux --sample-sheet /user/me/sample_sheet.csv /user/me/qseq_input /user/me/demuxed_output


The arguments are:

#.  ``--sample_sheet``:  the HDFS path to the sample sheet file;
#. Input paths:  files or directories, containing files for reads 1, 2 and 3
   in the specified input format (default: qseq_);
#. Output path:  path to directory where demux will write its output.  This
   directory must not already exist; demux will create it.

``seal demux`` follows the normal Seal usage convention.  See the section
:ref:`program_usage` for details.

Command line options
.......................


======= ==================================== =========================================================
 Short  Long                                 Description
======= ==================================== =========================================================
 -s      --sample-sheet <FILE>               Sample sheet for the experiment

 -m      --mismatches <N>                    Maximum number of acceptable barcode
                                             substitution errors (default: 0)

 -ni     --no-index                          Dataset doesn't contain index reads.
                                             Sort reads only by lane (default: false)

 -if     --input-format <FORMAT>             Input format name (qseq,fastq; default: qseq)

 -of     --output-format <FORMAT>            Output format name(qseq,fastq; default: qseq)

 -oc     --compress-output <CODEC>           Compress output files with CODEC (one of gzip bzip2,
                                             snappy, auto)

 -sepr   --separate-reads                    Generate separate directories for each read number (default: false)

 -r      --num-reducers <INT>                Number of reduce tasks to use.

 -sc     --seal-config <FILE>                Override default Seal config file

         -D hbam.input.filter-failed-qc=true Discard reads that failed the machine quality check.
======= ==================================== =========================================================

Output
++++++++++

At the specified output path, ``seal demux`` creates a directory for each
combination of project, sample and, optionally (see ``--separate-reads``), read
present in the input.  If the sample sheet doesn't contain a "Project" column
(older CASAVA releases) the project name "DefaultProject" will be used.

Example::

  DefaultProject/
    my_sample_1/
      read files
    my_sample_2
      read files

On the other hand, with ``--separate-reads``::

  DefaultProject/
    my_sample_1/
      1/
        read files
      2/
        read files
    my_sample_2
      1/
        read files
      2/
        read files

The project and sample directory names correspond to the sample and project
specifed in the sample sheet for the read's lane and barcode.

In addition, you may find an ``unknown`` directory containing qseq files with
all the reads whose barcode was not found in the sample sheet (likely due to
sequencing errors).

Inside the sample's directory you'll find regular qseq or
fastq files, according to the ``--output-format`` option.

.. note:: demux can also compress its output files as it writes them; see ``--output-compression``

.. note:: gzip files can be concatenated into a single larger archive. E.g.,

              ``cat part-1.gz part-2.gz > whole.gz``


Background
++++++++++++++


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

Demux loads the sample sheet and keeps track of which lane/barcode
combination is associated with each sample.  For each flow-cell location
present in the input (tile / xpos / ypos), Demux gets the barcode (read 2), looks it up in the sample
sheet and assigns the reads from that location to the appropriate project/sample.


Mismatches
++++++++++++++

Demux supports matching barcodes with substitution errors. You can specify the
number of errors you'll willing to tolerate with the ``--mismatches`` option. By
default, no mismatches are tolerated.

The maximu number of mismatches that can be tolerated depends on the barcode
sequences used in the run; the more "different" they are, the greater the number
of mismatches that can be handled.


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

:Project:
	Project for the sample (optional. If absent, "DefaultProject" will be used.

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

.. note:: **Config File Section Title**: Demux


.. _qseq: file_formats.html#qseq-file-format
.. _fastq: file_formats.html#fastq-file-format
