.. _program_usage:

Program Usage
======================

All Seal programs generally have the following invocation pattern::

  ./program  [options] <input> <output>

Most programs can accept more than one input path (Seqal being the only
exception).  Also, while some Seal tools have their own specific options, there
are a number of Seal options that all programs accept.


Input and output paths
+++++++++++++++++++++++++++

The input and output paths must be on an HDFS volume. If you like, you can use
paths relative to the current user's HDFS home directory, i.e., ``/user/<USERNAME>``.



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

  ./bin/prq -D bl.prq.min-bases-per-read=54 input output


::

  ./bin/seqal -D seal.seqal.remove_unmapped=true --trimq 15 input_1 input_2 output

::

  ./bin/read_sort --num-reducers 96 -D mapred.compress.map.output=true  -ann ref.ann input output



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
