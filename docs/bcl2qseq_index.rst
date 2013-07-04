.. _bcl2qseq_index:

Bcl2Qseq -- BCL to Qseq converter
===================================

The Seal ``bcl2qseq`` program uses your Hadoop cluster to extract reads in
``qseq`` format from an Illumina run directories.  Internally it wraps
Illumina's own ``bclToQseq`` utility so it supports all the same features.

Since this program reads its input data directly from the run directory, *the
input data needs to be on a shared storage volume mounted at the same
point on all the Hadoop nodes*.

Bcl2Qseq will run one instance of Illumina's ``bclToQseq`` per flowcell tile;
thus, it will produce one output file per tile.

.. note:  You must install Illumina's ``bclToQseq`` to use this tool.

Usage
++++++

::

  seal bcl2qseq [OPTIONS] /path/to/run/directory /path/to/output/dir


Command line options
.......................


======= ========================= =========================================================
 Short  Long                       Description
======= ========================= =========================================================
 -l F    --logfile F               Write log output to file F (default: stderr).

         --bclToQseq-path P        Complete path to Illumina's ``bclToQseq`` utility, if
                                   it's not in the PATH.

         --append-ld-library-path  If your installation of ``bclToQseq`` needs
                                   addition library paths to run, you may provide a
                                   colon-separated list with this argument.

         --ignore-missing-bcl      Ignore missing or corrupt bcl files (see
                                   Illumina's ``bclToQseq`` documentation).

         --ignore-missing-control  Ignore missing or corrupt control files (see
                                   Illumina's ``bclToQseq`` documentation).

         --exclude-controls        See Illumina's ``bclToQseq`` documentation.

         --no-eamss                See Illumina's ``bclToQseq`` documentation.
======= ========================= =========================================================



Configurable Properties
++++++++++++++++++++++++++

Bcl2Qseq does not have any program-specific configurable properties at the
moment.  You can still use its section to configure Hadoop property values
specific to Bcl2Qseq.

.. note:: **Config File Section Title**: None yet -- TODO!

.. _qseq: file_formats.html#qseq-file-format-input
