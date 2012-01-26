==================
Seal
==================


Seal is a suite of distributed applications for aligning short DNA reads, and
manipulating and analyzing short read alignments.  Seal applications generally run on the Hadoop
framework and are made to scale well in the amount of computing nodes available
and the amount of the data to process.  This fact makes Seal particularly well
suited for processing large data sets.
Seal is part of the Biodoop suite.


Install
+++++++++++

Please see the Seal documentation at http://biodoop-seal.sourceforge.net/.
Alternatively, if you have Sphinx installed you can make your own local copy
of the documentation::

  make doc


Seal Components
++++++++++++++++



Seal currently includes the following Hadoop-based applications:  Demux,
PairReadsQSeq, Seqal, ReadSort.

Demux
  Demultiplex Illumina reads.

PairReadsQSeq
	A preprocessor to convert Illumina ``qseq`` files into ``prq`` file format;

Seqal
	A distributed short read mapping and duplicate removal tool, based on libbwa.
	Seqal produces the same read mappings produced by the well known BWA_ program,

ReadSort
	A Hadoop utility to sort read alignments.


Please see the full Seal documentation at  docs/_build/html/index.html.

Do you need Seal?
++++++++++++++++++++++++

Seal has been built with large data sets in mind, like those produced by whole
genome sequencing runs.  If you're aligning read datasets of more than a couple
of hundred MB, and you have a cluster of computers (even a small one, say 4 or 5
nodes, and up to hundreds of nodes) then Seal might be for you.

Seal provides a number of important features.

Scalability and speed
-------------

Seal can efficiently use the computer power of a large number of
nodes.  We have successfully tested SEAL of 500GB datasets, running on 16- to
128-node clusters.  Thanks to its ability to scale, Seal can achieve very high
throughputs by harnessing the computing power of many machines.  And when you
need more speed, you can simply add more machines.


Memory efficiency
-------------------

Seal can use your computer's resources more efficiently than other alignment
tools.  Thanks to its use of shared memory, as many as 7 or 8 alignment
processes can be run concurrently on a single workstation with 8 GB of memory,
using a Human reference genome (UCSC HG18, for instance).


Robustness
--------------

Thanks to Hadoop, Seal provides a start-and-forget solution,
resisting node failures and transient cluster conditions that may cause your
jobs to fail.  It also avoids basing all operations on a centralized shared
stored volume, which can represent a single point of failure.


Authors
++++++++++++

Seal was written by:
 * Luca Pireddu <luca.pireddu@crs4.it>
 * Simone Leo <simone.leo@crs4.it>
 * Gianluigi Zanetti <gianluigi.zanetti@crs4.it>


License
++++++++

Seal and its components are released under the `GPLv3 license <http://www.gnu.org/licenses/gpl.html>`_.


Copyright
++++++++++

Copyright CRS4, 2011.

.. _Hadoop: http://hadoop.apache.org/
.. _BWA: http://bio-bwa.sourceforge.net/
