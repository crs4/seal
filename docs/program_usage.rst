.. _program_usage:

Program Usage
======================

All Seal programs generally have the following invocation pattern::

  seal <program>  [options] <input> <output>

Most programs can accept more than one input path (Seqal being the only
exception).  Also, while some Seal tools have their own specific options, there
are a number of Seal options that all programs accept.


Input and output paths
+++++++++++++++++++++++++++

Careful how you specify your input and output paths.  If you don't specify a
full URI, your path will be interpreted using the Hadoop cluster's default file
system.  See the section below on :ref:`specifying paths <specifying_paths>` to see some examples.


Generic Seal options
++++++++++++++++++++++++


+-------------------------------------+-----------------+----------------------+
|  **Meaning**                        | **Short Opt**   | **Long Opt**         |
+=====================================+=================+======================+
| Number of reduce tasks to use.      | -r <INT>        |--num-reducers <INT>  |
+-------------------------------------+-----------------+----------------------+
| Override default Seal config file   | -sc <FILE>      |--seal-config <FILE>  |
| ($HOME/.sealrc)                     |                 |                      |
+-------------------------------------+-----------------+----------------------+
| Set the value of a property         | -D <prop=value> |                      |
+-------------------------------------+-----------------+----------------------+

In addition, you can use most Hadoop options directly with the Seal tools.  This
is handy to override the cluster's default settings.



Examples
--------------

::

  seal prq -D seal.prq.min-bases-per-read=54 input output


::

  seal seqal -D seal.seqal.remove_unmapped=true --trimq 15 input_1 input_2 output

::

  seal read_sort --num-reducers 96 -D mapred.compress.map.output=true  -ann ref.ann input output



Properties
+++++++++++++

If you find you're setting the same property all the time, you should consider
creating a Seal configuration file and saving your settings in it.  The Seal
tools will automatically load the file on start up.  See the section
:ref:`seal_config` for details.


Hadoop
++++++++++

Ensure the Hadoop environment variables are set (see :ref:`installation_deploying`)
so that the Seal tools can find the Hadoop cluster configuration and executables.

.. _specifying_paths:

Specifying paths
------------------

Here are some examples showing how to specify input and output paths.

====================== ====================================================================
====================== ====================================================================
Default file system:     ``hdfs://hadoop.mycluster.int:9000/``
Specified input path:    ``/user/me/input_data``
Fully resolved path:     ``hdfs://hadoop.mycluster.int:9000/user/me/input_data``
====================== ====================================================================

The example above specified an absolute path but doesn't specify a file system.
Therefore Seal resolves the path as being on the default Hadoop volume, ``hdfs://hadoop.mycluster.int:9000/``.

====================== ====================================================================
====================== ====================================================================
Default file system:     ``hdfs://hadoop.mycluster.int:9000/``
Specified input path:    ``input_data``
Fully resolved path:     ``hdfs://hadoop.mycluster.int:9000/user/me/input_data``
====================== ====================================================================

This example uses a relative path, ``input_data``. It is interpreted as being on
the default file system (``hdfs://hadoop.mycluster.int:9000/``) and in the
user's home directory for that file system.  In HDFS, the user's home directory
is always ``/user/${USER}`` (the ``${USER}`` variable expands to the current
user's Unix username).

====================== ====================================================================
====================== ====================================================================
Default file system:     ``hdfs://hadoop.mycluster.int:9000/``
Specified input path:    ``file:///home/me/input_data``
Fully resolved path:     ``/home/me/input_data``
====================== ====================================================================

In this case we see an input path specified as a full URI; in this way we can
access data that is not on Hadoop cluster's default file system.  In fact,
noticed how the default file system is ``hdfs://hadoop.mycluster.int:9000/``,
but we're accessing data that resides on a standard file system path.  Note that
for this to work the specified path must exist on all the Hadoop cluster's
nodes.

====================== ====================================================================
====================== ====================================================================
Default file system:     ``file://``
Specified input path:    ``/home/me/input_data``
Fully resolved path:     ``/home/me/input_data``
====================== ====================================================================

In this example Hadoop has been configured with the default file system being
the standard local file system.  The input path specified, which is not a full
URI, is therefore interpreted as being on the local file system.
