.. _installation_deploying:

Installation - Deploying
========================

The Seal archive you built contains all the Seal components necessary to run the
Seal tools.  The launcher programs in its ``bin`` subdirectory are written to
first search for the Seal components in the known paths relative to their own
position, so many Seal tools will work even if you simply extract the archive
in any position and run everything from within it.

Unfortunately, there is an exception:  at the moment the **Seqal** application 
needs to be distributed manually to all the Hadoop cluster nodes. In addition,
*the dependencies need to be installed to all cluster nodes* (see the previous
installation sections).


Hadoop Cluster
++++++++++++++++++

First of all, note that you will need an operative Hadoop cluster to run most
interesting programs in the Seal suite.  Please refer to the Hadoop
documentation for information on setting up your cluster.  Some useful links may
be:

* http://hadoop.apache.org/common/docs/r0.20.2/quickstart.html
* http://wiki.apache.org/hadoop/GettingStartedWithHadoop


To tell the various Seal programs where to find the Hadoop executables and
cluster configuration you should set the following two environment variables:

:HADOOP_HOME:

  Path to your Hadoop installation, where ``bin/hadoop`` is found.  As an alternative, you can place the
  ``hadoop`` executable in your ``PATH``.

:HADOOP_CONF_DIR:

   Path to your Hadoop cluster configuration, if it's not under
   ``${HADOOP_HOME}/conf``.

For details, see the `Hadoop Quick Start Guide
<http://hadoop.apache.org/common/docs/r0.20.2/quickstart.html>`_.




Deployment
++++++++++++

Deployment strategies for Seal on your cluster will vary depending on your
particular set-up.

Shared Volume
---------------

If your cluster has a shared volume that is accessible from all nodes from the
same mount point, you can simply extract the archive ``seal-<release>.tar.gz`` on it and
the use it without any further configuration (except for setting Hadoop-specific
configuration variables, such as HADOOP_HOME and HADOOP_CONF_DIR).  This is the
simplest way to get going.

::

  export HADOOP_CONF_DIR=/shared_mount/hadoop_conf
  export HADOOP_HOME=/shared_mount/hadoop
  cd /shared_mount
  tar xzf seal-0.1.1.tar.gz
  cd seal-0.1.1
  ./bin/run_prq.sh input output


Manual Distribution
---------------------

For large clusters, it may be necessary to distribute the software to the
various nodes rather than relying on a shared volume.  In our tests, a volume
shared via NFS was not able to handle the demands of a cluster with 128 nodes,
resulting in tasks that never went past the "initializing" state and eventually
timed out.

You may extract the Seal archive on each node's local storage.  Here's an
example using the pdsh_, supposing you have 100 nodes named ``node001`` to
``node100``::

  user@mycomputer$ pdsh node[001-100] scp mycomputer:/home/user/seal-0.1.1.tar.gz /mount/local_storage/
  user@mycomputer$ pdsh node[001-100] tar xzf /mount/local_storage/seal-0.1.1.tar.gz
  user@mycomputer$ ssh node001
  user@node001$ cd /mount/local_storage/seal-0.1.1
  user@node001$ ./bin/run_prq.sh hdfs_input hdfs_output


Distributed Cache
------------------

A simpler way to distributed the software to all the nodes is to use Hadoop's
Distributed Cache.

This mode of operation is not implemented yet, but will be shortly.


Other custom installation
--------------------------

The Java components of Seal are trivial to install, since they are
entirely contained in a single jar file which Hadoop automatically distributes
to the slave nodes. You simply need to ensure that the Jar is accessible to the 
Seal start-up scripts.

On the other hand, the Python components are a little more tricky, and they 
need to be accessible directly to the map and reduces tasks that run on the 
slave nodes.  If you would like to handle a custom installation scenario, note
that Seal uses the standard Python distutils_.  Please run

  ``python setup.py --help``

to see the various options.

Typical scenarios might include installing to a shared user's home directory::

  python setup.py install --user

Or, to install to an arbitrary location::

  python setup.py install --home <desired path>

Finally, a system-wide installation::

  python setup.py build
  sudo python setup.py install --skip-build


Running unit tests
+++++++++++++++++++++

From the Seal source directory, after having built Seal, you can run the unit
tests with the following commands::

  ant run-tests
  PYTHONPATH=$(pwd)/build python tests/run_py_unit_tests.py

The syntax ``$(pwd)`` gets the current working directory in Bash.  If you're
using another shell substitute this syntax appropriately.


.. _Pydoop: https://sourceforge.net/projects/pydoop/
.. _Hadoop: http://hadoop.apache.org/
.. _Python: http://www.python.org
.. _Ant: http://ant.apache.org
.. _Protobuf: http://code.google.com/p/protobuf/
.. _JUnit 4: http://www.junit.org/
.. _pdsh: https://sourceforge.net/projects/pdsh/
.. _distutils: http://docs.python.org/install/index.html
.. _Oracle Java 6: http://java.com/en/download/index.jsp
