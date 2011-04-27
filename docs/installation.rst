Installation
==============


Supported Platforms
+++++++++++++++++++

Seal has been tested on `Gentoo <http://www.gentoo.org>`_ and `CentOS
<http://www.centos.org>`_, and `Ubuntu <http://www.ubuntu.com/>`_. Although 
we haven't tested it on other Linux distributions, we expect Seal to work 
on them as well. Platforms other than Linux are currently not supported.

Building from source
+++++++++++++++++++++

At the moment we don't provide a pre-compiled distribution of Seal.  However,
once the dependencies are installed building it should be relatively simple.


Dependencies
--------------

You will need to install the following software packages:

* `Oracle Java 6`_ JDK
* Python_ (tested with ver. 2.6)
* Hadoop_ (tested with version ver. 0.20)
* Ant_
* Pydoop_
* Protobuf_

To run the unit tests you'll also need:

* `JUnit 4`_
* ant-junit (on Ubuntu install the ant-optional package).

We recommend installing these tools/libraries as packaged by your favourite
distribution.

Debian/Ubuntu packages
.........................

If you're using a Debian-based distribution, you can satisfy all the 
dependencies except for Pydoop (which at the moment doesn't provide a deb 
package) by installing these packages:

* sun-java6-jdk
* python
* protobuf-compiler
* libprotobuf6
* libprotoc6
* python-protobuf

Install all of them with::

  sudo apt-get install sun-java6-jdk python protobuf-compiler libprotobuf6 libprotoc6 python-protobuf 


Building
-----------

Seal includes Java, Python and C components that need to be built.  A Makefile 
is provided that builds all components.  Simply go into the root Seal source
directory and run::

  make

This will create the archive ``build/seal.tar.gz`` containing all Seal
components.  Inside ``build`` you'll also find the individual components:

* ``seal.jar``;
* ``lib`` directory, containing Python modules.


Installation
--------------

Deployment strategies for Seal on your cluster will vary depending on your
particular set-up.

Shared Volume
...............

If your cluster has a shared volume that is accessible from all nodes from the
same mount point, you may simply extract the archive ``seal.tar.gz`` on it and
the use it without any further configuration (except for setting Hadoop-specific
configuration variables, such as HADOOP_HOME and HADOOP_CONF_DIR).  This is the
simplest way to get going.

::

  export HADOOP_CONF_DIR=/shared_mount/hadoop_conf
  export HADOOP_HOME=/shared_mount/hadoop
  cd /shared_mount
  tar xzf seal.tar.gz
  cd seal
  ./bin/run_prq.sh input output


Manual Distribution
.....................

You may extract the Seal archive on each node's local storage.  Here's an
example using the pdsh_::

  user@mycomputer$ pdsh node[001-100] scp mycomputer:/home/user/seal.tar.gz /mount/local_storage/
  user@mycomputer$ pdsh node[001-100] tar xzf /mount/local_storage/seal.tar.gz
  user@mycomputer$ ssh node001
  user@node001$ cd /mount/local_storage/seal
  user@node001$ ./bin/run_seqal.sh ...


Distributed Cache
..................

For large clusters, it may be necessary to distribute the software to the
various nodes rather than relying on a shared volume.  In our tests, a volume
shared via NFS was not able to handle the demands of a cluster with 128 nodes,
resulting in tasks that never went past the "initializing" state and eventually
timed out.

This mode of operation is not implemented yet, but will be shortly.


Other custom installation
..........................

While the Java components of Seal are trivial to install, since they are
entirely contained in a single jar file, the Python components can be a little
more tricky.  If you would like to handle a custom installation scenario, note
that Seal uses the standard Python distutils_.  Please run

  ``python setup.py --help``

to see the various options.


Hadoop
-------

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

.. _Pydoop: https://sourceforge.net/projects/pydoop/
.. _Hadoop: http://hadoop.apache.org/
.. _Python: http://www.python.org
.. _Ant: http://ant.apache.org
.. _Protobuf: http://code.google.com/p/protobuf/
.. _JUnit 4: http://www.junit.org/
.. _pdsh: https://sourceforge.net/projects/pdsh/
.. _distutils: http://docs.python.org/install/index.html
.. _Oracle Java 6: http://java.com/en/download/index.jsp
