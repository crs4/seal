.. _tuning:

Tuning
=========

Hadoop provides many properties to tune the behaviour of the framework.  This
can significantly affect performance.  There are some settings that we have
found to provide performance improvements, especially with PairReadsQSeq and
ReadSort, which on our cluster are I/O-bound applications.


**Compress map output**

  Generally a good idea, especially if you have CPU cycles to spare.
  

::

  mapred.compress.map.output=true


**More memory**

  More memory for the tasks means more memory for buffers, and thus less
  records spilled to disk.  You should probably give them as much
  memory as reasonable for your node configuration.

::

  mapred.child.java.opts=-Xmx1156m


**Sort memory**

  Akin to the last comment, some of the task's memory is going to be used for
  sort buffers.  We found it beneficial to increase the amount of memory dedicated
  to the sort buffer to over half the task memory.

::

  io.sort.mb=500



We welcome any feedback regarding tuning Hadoop settings for Seal, along with 
relevant benchmarking.  We would include such information in the documentation.
