.. _file_formats:

File Formats
=============

.. _file_formats_qseq:

QSeq file format
------------------------

The QSeq file format is fully documented in the `Illumina pipeline user's
guide`_ (page 163).  In brief, the file format is as follows.

* One record per line
* Each record represents one read.
* Unknown sequence bases are represented as '.'

Each record includes the following tab-separated fields, in order.

Machine name
    (hopefully) Unique identifier of the sequencer.
Run number
    (hopefully) Unique number to identify the run on the sequencer.
Lane number
    Positive integer (currently 1-8).
Tile number
    Positive integer.
X
    X coordinate of the spot. Integer (can be negative).
Y
    Y coordinate of the spot. Integer (can be negative).
Index
    Sample tag in multiplexed runs. For regular runs without multiplexing this field should have a value of 0.
Read Number
    1 for single reads; 1 or 2 for paired ends.
Sequence
    The base sequence for this read.  Unknown bases are indicated with a '.'
Quality
    The calibrated base quality string.
Filter
    Did the read pass filtering? 0 - No, 1 - Yes.

Example
+++++++++

Here are two sample lines from a QSeq file.  Order doesn't matter, and read
mates do not have to be in the same file or any particular relative position

::

  CRESSIA	242	1	2204	1453	1918	0	1	.TTAATAAGAATGTCTGTTGTGGCTTAAAA	B[[[W][Y[Zccccccccc\cccac_____	1
  CRESSIA	242	1	2204	1490	1921	0	2	..GTAAAACCCATATATTGAAAACTACAAA	BWUTWcXVXXcccc_cccccccccc_cccc	1


.. _file_formats_prq:

PRQ file format
------------------------

The PRQ file format is another line-oriented format.  Each record contains a
read pair in the following tab-separated fields.

Id
  A unique id for the read pair.
Read 1 sequence
  The base sequence for read 1.  Unknown bases are indicated with a 'N'
Quality 1
  The base quality sequence for read 1 in Sanger Phred+33 encoding.
Read 2 sequence
  The base sequence for read 2.  Unknown bases are indicated with a 'N'
Quality 2
  The base quality sequence for read 2 in Sanger Phred+33 encoding.

Example
++++++++++

Here is a sample line from a PRQ file, constructed with the QSeq lines above::

  CRESSIA_242:1:2204:1453;1918#0	NTTAATAAGAATGTCTGTTGTGGCTTAAAA	#<<<8><:<;DDDDDDDDD=DDDBD@@@@@	NNGTAAAACCCATATATTGAAAACTACAAA	#8658D9799DDDD@DDDDDDDDDD@DDDD



.. _file_formats_fastq:

Fastq file format
------------------------

The Fastq is another text-based file format, quite popular although perhaps only
for historical reasons.

As of version 1.8 of Illumina's Casava software,
Illumina is returning to the fastq format (from the qseq format).  For this
reason we have implemented fastq input in :ref:`Seal PairReadsQseq<prq_index>`.

Seal by default assumes the Illumina-style fastq format (see the Casava v. 1.8
user's guide p. 41).  This Fastq format is defined as a series of records.  Each
record consists of 4 lines:

#. begins with '@' and followed by the sequence identifier and meta info;
#. the raw sequence;
#. begins with '+' and is *optionally* followed by the same sequence identifier as in line 1;
#. ASCII base quality values in the Sanger-style Phred+33 encoding.


In Illumina Fastq files the identifier line (line 1) contains several fields of
meta info about the sequence in the following format::

  @<Instrument>:<Run Number>:<Flowcell ID>:<Lane>:<Tile>:<X-pos>:<Y-pos>SPACE<Read>:<Is Filtered>:<Control Number>:<Index Sequence>

The meaning of each field is as follows.

Instrument
    (hopefully) Unique identifier of the sequencer.
Run Number
    Run number on the sequencer.
Flowcell ID
    The id of the flowcell used in the run that produced the read.
Lane
    Flowcell lane number.  Positive integer (currently 1-8).
Tile number
    Tile number within the lane.  Positive integer.
X-pos
    X coordinate of the cluster within the tile. Integer (can be negative).
Y-pos
    Y coordinate of the cluster within the tile. Integer (can be negative).
Read
    Read number.  1 for single reads; 1 or 2 for paired ends.
Is Filtered
    Did the read *fail* filtering? Y or N.
Control Number
    0 when none of the control bits are on, otherwise it is an even number
Index Sequence
    Sample tag in multiplexed runs.

The ``Y-pos`` and ``Read`` fields are separated by a SPACE character, while the
rest of the fields are separated by colon characters.

Example
+++++++++++++++

Here is an example of a fastq record from the Casava documentation::

  @EAS139:136:FC706VJ:2:5:1000:12850  1:Y:18:ATCACG
  AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
  +
  BBBBCCCC?<A?BC?7@@???????DBBA@@@@A@@


Standard Fastq files
+++++++++++++++++++++++

The format specified above for the id line of the fastq files has been invented
by Illumina and, as far as we know, is only used by Casava.  The "standard"
fastq format makes no specifications for the id line and gives no means to
express meta information about the read.  Still, Seal tries to let you work with
"plain" fastq files, as long as their id ends with a "/1" or "/2" so that it can
extract the read number for the sequence.

Seal will initially try to read a Fastq file as an Illumina file, and then
revert to the standard format after the first record that doesn't match the
Illumina format.


.. _Illumina pipeline user's guide: http://biowulf.nih.gov/apps/CASAVA_UG_15011196B.pdf
