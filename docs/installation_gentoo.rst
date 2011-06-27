.. _installation_gentoo:

Installation - Gentoo
=======================

Install Dependencies
++++++++++++++++++++++


::

  emerge sun-jdk python ant-core ant-junit4 protobuf boost


The activated use flags per dev-libs/protobuf are::


  + + java          : Adds support for Java
  + + python        : Adds support/bindings for the Python language

The activated use flags per dev-libs/boost are::


  + + python        : Adds support/bindings for the Python language

Except for Ant and JUnit, the dependencies **need to be installed on all cluster nodes**.
As such, you will need to either install the software to all the
nodes or install it to a shared volume.


Install Hadoop
+++++++++++++++++

If you haven't done so already, set up your Hadoop cluster.  Please refer to 
the Hadoop documentation for your chosen distribution:

* `Instructions for Apache Hadoop <http://hadoop.apache.org/common/docs/r0.20.2/cluster_setup.html>`_
* `Instructions for Cloudera Hadoop <https://ccp.cloudera.com/display/CDHDOC/CDH3+Installation>`_

Seal has been developed with Apache Hadoop 0.20, but we've have also tested it
with Cloudera CDH3.



Build Pydoop
++++++++++++++++

With Tarball distributions of Hadoop (Apache and Cloudera)
------------------------------------------------------------


Download the latest version of Pydoop from here:  http://sourceforge.net/projects/pydoop/files/.
Set the ``HADOOP_HOME`` environment variable so that it points to where the
Hadoop tarball was extracted::

  export HADOOP_HOME=<path to Hadoop directory>

Then, in the same shell::

  tar xzf pydoop-0.4.0_rc2.tar.gz
  cd pydoop-0.4.0_rc2
  python setup.py build



Install Pydoop
++++++++++++++++

You need to decide where to install Pydoop.  Remember that it needs to be accessible by
all the cluster nodes running Seal tasks.  We recommend installing to a shared
volume, except for medium-large clusters (more than 100 nodes) where local
installation may be necessary.

If your user's home directory is accessible on all cluster nodes, then
installing it there may be a good idea::

  python setup.py install --user

Otherwise, to install to a specific path::

  python setup.py install --home <path>

For a system-wide (local) installation::

  sudo python setup.py install --skip-build

.. note::
  If you had to export HADOOP_HOME to build Pydoop, make sure the variable is also set when you call ``setup.py install``.
  The `Pydoop documentation <http://pydoop.sourceforge.net/docs/>`_ has more details regarding its installation.

Build Seal
++++++++++++++


Seal needs the Hadoop jars to compile.  Tell the build script where to find them
by setting the ``HADOOP_HOME`` environment variable.

Set ``HADOOP_HOME`` to point to the
extracted copy of the Hadoop archive. For instance::

  export HADOOP_HOME=/home/me/hadoop-0.20



The build process expects to find the Hadoop jars in the
``${HADOOP_HOME}`` and ``${HADOOP_HOME}/lib`` directories.


Seal includes Java, Python and C components that need to be built.  A Makefile 
is provided that builds all components.  Simply go into the root Seal source
directory and run::

  make

This will create the archive ``build/seal-<release>.tar.gz`` containing all Seal
components.  Go to the section on :ref:`Deploying <installation_deploying>` to see
what to do with it.



Creating the documentation
----------------------------

You can find the documentation for Seal at http://biodoop-seal.sourceforge.net/.

If however you want to build yourself a local copy, you can do so in three steps:

#. install Sphinx_: ``emerge sphinx``
#. go to the Seal directory
#. run: ``make doc``


You'll find the documentation in HTML in ``docs/_build/html/index.html``.

.. _Sphinx:  http://sphinx.pocoo.org/
