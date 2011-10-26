.. _file_formats:

File Formats
=============

QSeq file format
------------------------ 
.. _file_formats_qseq:

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


PRQ file format
------------------------
.. _file_formats_prq:

The PRQ file format is another line-oriented format.  Each record contains a
read pair in the following tab-separated fields.

Id
  A unique id for the read pair.
Read 1 sequence
  The base sequence for read 1.  Unknown bases are indicated with a 'N'
Quality 1
  The base quality sequence for read 1.  No specific encoding is mandated.
Read 2 sequence
  The base sequence for read 2.  Unknown bases are indicated with a 'N'
Quality 2
  The base quality sequence for read 2.  No specific encoding is mandated.

Example
++++++++++

Here is a sample line from a PRQ file, constructed with the QSeq lines above::

  CRESSIA_242:1:2204:1453;1918#0	NTTAATAAGAATGTCTGTTGTGGCTTAAAA	#<<<8><:<;DDDDDDDDD=DDDBD@@@@@	NNGTAAAACCCATATATTGAAAACTACAAA	#8658D9799DDDD@DDDDDDDDDD@DDDD

  
.. _Illumina pipeline user's guide: http://biowulf.nih.gov/apps/CASAVA_UG_15011196B.pdf 
