Additional Information
=======================

Support
--------

Although Seal doesn't come with any support or warranty, feel free to contact us
if you have any problems installing or using it.  We'll try to help.


Known Issues
---------------

Seqal doesn't run on 32-bit platforms

  We have tested Seqal on 32-bit platforms and have found it not to run.  We
  haven't made this issue a priority since we expect most users to be running
  on a 64-bit platform.

Seal doesn't handle single reads

  At the moment PairReadsQSeq and Seqal only work with paired reads.  If you
  want to align single reads, you can "hackishly" assemble a prq file that
  contains the same read as 1 and 2 and feed it to Seqal.

  

Credits
------------

Seal was been written by:
  * Luca Pireddu <luca.pireddu@crs4.it>
  * Simone Leo <simone.leo@crs4.it>
  * Gianluigi Zanetti <gianluigi.zanetti@crs4.it>

The alignment algorithm is implemented in libbwa, which is a modified and 
refactored version of the BWA_ short read aligner.

License
--------

Seal is released under the `GPLv3 license <http://www.gnu.org/licenses/gpl.html>`_.


.. _BWA: http://bio-bwa.sourceforge.net/
