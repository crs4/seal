.. _installation_dependencies:

Installation - Dependencies
=============================

Installing Hadoop
+++++++++++++++++++

Seal's first and main dependency is Hadoop_.

The Hadoop framework enables the creation of scalable and robust distributed
data-intensive applications without worrying about many of the intrinsic
complexities.  It is one of the factors at the root of Seal's scalability, as
well as behind the `success of many data-intensive operations <http://wiki.apache.org/hadoop/PoweredBy>`_.

For a thorough explanation of how Hadoop works and how to install it please see
Hadoop's own documentation.  The "official" version of Hadoop is the one made
available by the `Apache Hadoop <http://hadoop.apache.org>`_ project.  Other
vendor distributions are also available that supply additional tools, features and may simplify installation.

Supported versions
...................

At the moment, Seal is developed using **Apache Hadoop 1.0.x**.  Seal may also work
with Hadoop 2.0 using MRv1, but we havent' thoroughly tested that solution.

Other tested Hadoop distributions:

* CDH3


Other Dependencies
++++++++++++++++++++

Run-time dependencies:

* JDK (tested with OpenJDK6 and Oracle Java 6)
* HadoopBAM_ (latest)
* Python_ (tested with ver. 2.7)
* Hadoop_ version 1.x or Hadoop version 2.x with MRv1 (tested with ver. 1.0.x)
* Pydoop_ (at least ver. 0.8)
* Protobuf_ (tested with ver. 2.5)


Additional build-time dependencies:

* Ant_ (tested with ver. 1.8)
* build essentials: ``make``, ``gcc``, etc.

To run the unit tests you'll also need:

* `JUnit 4`_
* ant-junit

The run-time dependencies **need to be installed on all cluster nodes** (see
:ref:`installation_deploying`.  On the
other hand, Ant and JUnit only need to be installed on the node you use to build Seal.

We recommend installing these tools/libraries as packaged by your favourite
distribution.

.. note::

  Installing Seal's dependencies from scratch can be a lengthy process.  If you
  must install them from scratch, arm yourself with patience.  We encourage you
  to report any installation problems so we can use that information to make
  Seal easier to install.


Installing on Ubuntu and Debian
++++++++++++++++++++++++++++++++++

To install the dependencies on Ubuntu and Debian::

  sudo apt-get update

  sudo apt-get install openjdk-6-jdk python protobuf-compiler \
  libprotobuf7 libprotoc7 python-protobuf ant ant-optional \
  libboost-python-dev build-essential python-pip python-sphinx

  sudo pip install pydoop

HadoopBAM needs to be built by hand.  Please see `its web page for details
<HadoopBAM>`_.  Remember that you only need to build it in a location accessible when you'll be building Seal; no other installation is required.


Installing on Scientific Linux
++++++++++++++++++++++++++++++++++

::

  sudo yum install  \
  ant \
  ant-junit \
  ant-nodeps \
  java-1.6.0-openjdk   \
  protobuf-c \
  protobuf-compiler \
  protobuf-python \
  python \
  python-devel \
  boost-python-devel \
  gcc-c++ \
  openssl-devel \
  junit4

.. note::
 openssl-devel only required for Hadoop 0.20.203;
 junit4  only required for Hadoop < 0.20.203


HadoopBAM needs to be built by hand.  Please see `its web page for details
<HadoopBAM>`_.  Remember that you only need to build it in a location accessible when you'll be building Seal; no other installation is required.


Scientific Linux 6.1 / Python 2.6
++++++++++++++++++++++++++++++++++++++++

If you're using SL with Python 2.6, you'll need to install a couple of other
packages that add functionality that is included in the Python 2.7 standard
library.

::

  yum install python-importlib python-argparse



Installing on Gentoo
+++++++++++++++++++++++

::

  emerge sun-jdk python ant-core ant-junit4 protobuf boost


The activated use flags per dev-libs/protobuf are::


  + + java          : Adds support for Java
  + + python        : Adds support/bindings for the Python language

The activated use flags per dev-libs/boost are::


  + + python        : Adds support/bindings for the Python language

HadoopBAM needs to be built by hand.  Please see `its web page for details
<HadoopBAM>`_.  Remember that you only need to build it in a location accessible when you'll be building Seal; no other installation is required.


Finally, you'll need to install Pydoop.  Please `refer to its own
documentation <http://pydoop.sourceforge.net/docs/installation.html>_`.


Python 2.6
++++++++++++


If you're Python 2.6 you'll need to install backports of importlib and
argparse.

Done?
++++++++

Once you've installed the dependencies, you may proceed to
:ref:`installing Seal itself <installing_seal>`.


.. _HadoopBAM: http://hadoop-bam.sf.net
.. _Pydoop: https://sourceforge.net/projects/pydoop/
.. _Hadoop: http://hadoop.apache.org/
.. _Python: http://www.python.org
.. _Ant: http://ant.apache.org
.. _Protobuf: http://code.google.com/p/protobuf/
.. _distutils: http://docs.python.org/install/index.html
.. _Sphinx:  http://sphinx.pocoo.org/
.. _JUnit 4:  http://junit.org/
.. [#build-time-deps] The following packages should only be required at build-time: ``protobuf-compiler libprotoc* ant ant-optional g++``
