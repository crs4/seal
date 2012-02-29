.. _installation_sl:

Installation - Scientific Linux
==================================

The procedure for Scientific Linux is pretty much the same as for Ubuntu, with
the main difference being the package manager used and the names of the packages
to install.  As such, we'll only list the yum commands necessary to install the
dependencies.  For the rest of the procedure refer to the :ref:`Ubuntu installation
page <installation_ubuntu>`.


Installing Dependencies
+++++++++++++++++++++++++++

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
  # pydoop dependencies \
  boost-python-devel \
  gcc-c++ \
  openssl-devel # only for Hadoop 0.20.203


Building Hadoop native libraries
------------------------------------

If you need to build the Hadoop native libraries yourself, you'll also need to
install the following packages::

  sudo yum install  \
  gcc \
  autoconf \
  automake \
  libtool \
  ant \
  ant-nodeps \
  lzo \
  lzo-devel


Remember that the run-time dependencies **need to be installed on all cluster nodes**.  As
such, you will need to either install the software to all the nodes or install
it to a shared volume.  On the other hand, the build-time dependencies [#build-time-deps]_
only need to be installed on the node you use to build Seal.


Packaged distributions of Cloudera Hadoop
--------------------------------------------------

Cloudera provides packages for Scientific Linux and other Red Hat-compatible
systems.  See `the Cloudera page for details <https://ccp.cloudera.com/display/CDHDOC/CDH3+Installation#CDH3Installation-InstallingCDH3OnRedHatcompatiblesystems>`_.


.. [#build-time-deps] The following packages should only be required at build-time: protobuf-compiler protobuf-c ant ant-nodeps ant-junit gcc-g++

