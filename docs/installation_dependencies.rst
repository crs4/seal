.. _installation_dependencies:

Installation - Dependencies
=============================

Hadoop
+++++++++++++++++++

We assume you have a working Hadoop_ installation.  If not, see Hadoop's own
documentation for help on getting it running.  The "official" version of Hadoop
is the one made available by the `Apache Hadoop <http://hadoop.apache.org>`_
project.  Other vendor distributions are also available that supply additional
tools, features and may simplify installation.

Supported versions
...................

At the moment, Seal is developed using **Apache Hadoop 2.x**.  Because of its
dependencies, Seal no longer supports running on Hadoop 1.x.

Other Dependencies
++++++++++++++++++++

Run-time dependencies:

* JDK (tested with OpenJDK7 and Oracle Java 7)
* Rapi_
* Python_ (ver. 2.7)
* Pydoop_ (at least ver. 0.8)


Additional build-time dependencies:

* Sbt_ (tested with ver. 0.12)
* build essentials: ``make``, ``gcc``, etc.

To run the unit tests you'll also need:

* `JUnit 4`_

The run-time dependencies **need to be installed on all cluster nodes** (see
:ref:`installation_deploying`.  On the
other hand, Ant and JUnit only need to be installed on the node you use to build Seal.

We recommend installing these tools/libraries as packaged by your favourite
distribution.

Done?
++++++++

Once you've installed the dependencies, you may proceed to
:ref:`installing Seal itself <installing_seal>`.


.. _Rapi: http://github.com/crs4/rapi
.. _Sbt: http://www.scala-sbt.org/
.. _Pydoop: https://sourceforge.net/projects/pydoop/
.. _Hadoop: http://hadoop.apache.org/
.. _Python: http://www.python.org
.. _Ant: http://ant.apache.org
.. _Protobuf: http://code.google.com/p/protobuf/
.. _distutils: http://docs.python.org/install/index.html
.. _Sphinx:  http://sphinx.pocoo.org/
.. _JUnit 4:  http://junit.org/
.. [#build-time-deps] The following packages should only be required at build-time: ``protobuf-compiler libprotoc* ant ant-optional g++``
