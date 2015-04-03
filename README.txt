==================
Seal
==================


Seal is a suite of distributed applications for aligning short DNA reads, and
manipulating and analyzing short read alignments.  Seal applications generally run on the Hadoop
framework and are made to scale well in the amount of computing nodes available
and the amount of the data to process.  This fact makes Seal particularly well
suited for processing large data sets.

See the documentation at http://biodoop-seal.sourceforge.net/ for more information.


Install
+++++++++++


Just for your workstation
-----------------------------

Set up Hadoop-BAM and, if you're using Hadoop 2 or greater, AvroParquet input
and output libraries.  You can do this on your own and then point Seal to them
during compilation through the environment variables::

    HADOOP_BAM
    PARQUETMR_JAR

To use the canned recipes run::

    python setup.py build_hadoop_bam
    python setup.py build_parquet_mr

You'll need an internet connection, among other things...

These commands will create these dependencies inside the `bundled` directory.
Once that's done, run::

    python setup.py build

And then install::

    python setup.py install --user

which will install the software under `$HOME/.local`.  If `$HOME/.local/bin` is
in your `PATH` you can start try it out by running::

    seal

from your command line.


For your cluster
------------------

Please see the Seal documentation at http://biodoop-seal.sourceforge.net/.
Alternatively, if you have Sphinx installed you can make your own local copy
of the documentation::

  make doc

Authors
++++++++++++

Seal was written by:
 * Luca Pireddu <luca.pireddu@crs4.it>
 * Simone Leo <simone.leo@crs4.it>
 * Gianluigi Zanetti <gianluigi.zanetti@crs4.it>

Contributors:
 * Guillerma Carrasco Hernandez <guillermo.carrasco.hernandez@gmail.com>
 * Roman Valls Guimera <brainstorm@nopcode.org>

License
++++++++

Seal and its components are released under the `GPLv3 license <http://www.gnu.org/licenses/gpl.html>`_.


Copyright
++++++++++

Copyright CRS4, 2011-2012.
