Additional Information
=======================

Support
--------

Although Seal doesn't come with any guarantee of support or warranty, feel free to contact us
if you have any problems installing or using it.  We'll try to help.

* `Submit a bug report <http://sourceforge.net/tracker/?func=add&group_id=536922&atid=2180420>`_
* Contact the authors (see addresses below)



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

Seal is written at CRS4_ by:
  * Luca Pireddu <luca.pireddu@crs4.it>
  * Simone Leo <simone.leo@crs4.it>
  * Gianluigi Zanetti <gianluigi.zanetti@crs4.it>

The alignment algorithm is implemented in libbwa, which is a modified and
refactored version of the BWA_ short read aligner.


Logo
.......

The Seal logo was pieced together from a `seal image by mushko
<http://www.openclipart.org/detail/20449>`_ released in the `Open Clip Art
Library <http://www.openclipart.org>`_, and the
`Hadoop elephant <http://svn.apache.org/repos/asf/hadoop/logos/out_rgb/elephant_rgb.jpg>`_.



Citing Seal
--------------

If you use Seal in your work, please cite:

  L. Pireddu, S. Leo, and G. Zanetti. SEAL: a distributed short read mapping and duplicate removal tool.
  *Bioinformatics*, 27(15), 2011, pp. 2159--2160. DOI: `10.1093/bioinformatics/btr325
  <https://doi.org/10.1093/bioinformatics/btr325>`_.

Bibtex::

    @article{pireddu:seal:2011:bioinfo,
      author = {Pireddu, Luca and Leo, Simone and Zanetti, Gianluigi},
      title = {SEAL: a distributed short read mapping and duplicate removal tool},
      volume = {27},
      number = {15},
      pages = {2159-2160},
      year = {2011},
      doi = {10.1093/bioinformatics/btr325},
      journal = {Bioinformatics}
    }

License
--------

Seal is released under the `GPLv3 license <http://www.gnu.org/licenses/gpl.html>`_.


.. _BWA: http://bio-bwa.sourceforge.net/
.. _CRS4:  http://www.crs4.it

