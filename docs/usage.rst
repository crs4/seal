Usage
=====

To run PairReadsQSeq, use the ``run_prq.sh`` script from the ``bin``
subdirectory of the distribution root.  The script takes the following
command-line arguments (HDFS paths, possibly relative to the current
user's HDFS home directory, i.e., ``/user/<USERNAME>``):

#. input directory, containing individual reads in the QSeq format;

#. output directory, where paired reads will be written;

#. (optional) minimum number of known bases (default is 30).

It is recommended that you set the HADOOP_HOME environment variable to
point to your Hadoop installation; as an alternative, ensure that the 
``hadoop`` executable is in your ``PATH``.
In addition, if you use a non-standard Hadoop configuration directory,
the ``HADOOP_CONF_DIR`` environment variable has to be set to point to
that directory.

For instance, if Hadoop is installed in ``/opt/hadoop`` and its
configuration directory is in ``/etc/hadoopconf``::

 export HADOOP_HOME="/opt/hadoop"
 export HADOOP_CONF_DIR="/etc/hadoopconf"

Note that ``run_prq.sh`` expects ``PairReadsQSeq.jar`` to be in its
parent directory.
