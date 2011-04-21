
=============================
Seqal
=============================

Welcome to Seqal, a distributed short read mapping and duplicate removal tool.


Seqal can function in align-only mode, producing the same read alignments that
are computed by BWA_.  In addition, Seqal can also:

* remove duplicate reads from your dataset
* filter reads with a high number of unknown bases
* filter read mappings with low quality

The Seqal alignment feature is implemented using the BWA aligner, and it
uses Hadoop_ to distribute work over a cluster of
computers.


In addition, Seqal's aligner can also be run in stand-alone mode (see
seqal/bin/align_script).

PairReadsQSeq is part of Biodoop Seal.

Do you need Seqal?
++++++++++++++++++++++++

Seqal has been built with large data sets in mind, like those produced by whole
genome sequencing runs.  If you're aligning read datasets of more than a couple
of hundred MB, and you have a cluster of computers (even a small one, say 4 or 5
nodes, and up to hundreds of nodes) then Seqal might be for you.

Seqal provides a number of key features.

Scalability and speed
-------------

Seqal can efficiently use the computer power of a large number of
nodes.  We have successfully tested SEAL of 500GB datasets, running on 16- to
128-node clusters.  Thanks to its ability to scale, Seqal can achieve very high 
throughputs by harnessing the computing power of many machines.  And when you 
need more speed, you can simply add more machines.


Memory efficiency
-------------------

Seqal can use your computer's resources more efficiently than other alignment
tools.  Thanks to its use of shared memory, as many as 7 or 8 alignment
processes can be run concurrently on a single workstation with 8 GB of memory,
using a Human reference genome (UCSC HG18, for instance).


Robustness
--------------

Thanks to Hadoop, Seqal provides a start-and-forget solution,
resisting node failures and transient cluster conditions that may cause your
jobs to fail.  It also avoids basing all operations on a centralized shared
stored volume, which can represent a single point of failure.


Documentation
+++++++++++++++

Please see the full Seqal documentation in docs/html/index.html


Limitations
+++++++++++++


Reads:  only in pairs please
-------------------------------

Currently, Seqal can only process read pairs.  If you need to process single
reads, you can duplicate each read so it appears as its own mate
(with an obvious performance penalty) or wait until the next version of Seqal.
Our current users haven't requested this feature so it hasn't been given a high 
priority, but this would change if we were to receive external requests.


Authors
++++++++++++

Seqal was been written by:
  * Luca Pireddu <luca.pireddu@crs4.it>
  * Simone Leo <simone.leo@crs4.it>
  * Gianluigi Zanetti <gianluigi.zanetti@crs4.it>


License
++++++++

Seqal and its components are released under the `GPLv3 license <http://www.gnu.org/licenses/gpl.html>`_.


Copyright
++++++++++

Copyright CRS4, 2011.

.. _Hadoop: http://hadoop.apache.org/
.. _BWA: http://bio-bwa.sourceforge.net/
