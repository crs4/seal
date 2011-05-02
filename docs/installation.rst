.. _installation_building:

Installation - Building
========================

Supported Platforms
+++++++++++++++++++

Seal has been tested on `Gentoo <http://www.gentoo.org>`_ and `Ubuntu <http://www.ubuntu.com/>`_. Although 
we haven't tested it on other Linux distributions, we expect Seal to work 
on them as well. Platforms other than Linux are currently not supported.


Downloading
+++++++++++++++++

We recommend you download the latest release from here:  http://sourceforge.net/projects/biodoop-seal/files/.

On the other hand, if you want to try the latest improvements or contributed, you can checkout the latest code 
from our repository::

  bzr checkout bzr://biodoop-seal.bzr.sourceforge.net/bzrroot/biodoop-seal/trunk seal-trunk


Dependencies
++++++++++++++

You will need to install the following software packages:

* `Oracle Java 6`_ JDK
* Python_ (tested with ver. 2.6)
* Hadoop_ (tested with version ver. 0.20)
* Ant_
* Pydoop_
* Protobuf_

To run the unit tests you'll also need:

* `JUnit 4`_
* ant-junit

Ant and JUnit and only build-time dependencies, so they don't need to be
installed on all your cluster nodes.  On the other hand, the rest of the
software does.  As such, you will need to either install the software to all the
nodes, or install it to a shared volume.

We recommend installing these tools/libraries as packaged by your favourite
distribution. 


Gentoo and Ubuntu distributions
-----------------------------------------

If you're using Gentoo or Ubuntu, you can satisfy all the 
dependencies **except** for Pydoop_ (which at the moment doesn't provide a
package) by installing packaged software (See :ref:`table below <table_packages>`).
For convenience, we've also added Pydoop's dependencies to the list, ``g++`` and
``libboost-python-dev``.

.. _table_packages:

+--------------------+----------------------------+-------------------------+
| Software           | Gentoo package(s)          |    Ubuntu package(s)    |
+====================+============================+=========================+
| Java               | dev-java/sun-jdk           | sun-java6-jdk [#pner]_  |
+--------------------+----------------------------+-------------------------+
| Python             | dev-lang/python            | python                  |
+--------------------+----------------------------+-------------------------+
| Ant                | dev-java/ant-core          | ant ant-optional        |
|                    | dev-java/ant-junit4        |                         |
+--------------------+----------------------------+-------------------------+
| Protobuf           | dev-libs/protobuf          | libprotobuf6 libprotoc6 |
|                    |                            | protobuf-compiler       |
|                    |                            | python-protobuf         |
+--------------------+----------------------------+-------------------------+
| g++ [#pydoop_dep]_ | *(already installed)*      | g++                     |
+--------------------+----------------------------+-------------------------+
| Boost python       | dev-libs/boost             | libboost-python-dev     |
| [#pydoop_dep]_     |                            |                         |
+--------------------+----------------------------+-------------------------+


Installing dependencies on Gentoo
....................................

::

  emerge sun-jdk python ant-core ant-junit4 protobuf


The activated use flags per dev-libs/protobuf are::


  + + java          : Adds support for Java
  + + python        : Adds support/bindings for the Python language

The activated use flags per dev-libs/boost are::


  + + python        : Adds support/bindings for the Python language


Installing dependencies on Ubuntu
....................................

You'll need to uncomment the "partner" repositories in
``/etc/apt/sources.list`` to install the Java package.

::

  sudo apt-get install sun-java6-jdk python protobuf-compiler \
  libprotobuf6 libprotoc6 python-protobuf ant ant-optional g++ \
  libboost-python-dev


Building
+++++++++++

Seal includes Java, Python and C components that need to be built.  A Makefile 
is provided that builds all components.  Simply go into the root Seal source
directory and run::

  make

This will create the archive ``build/seal.tar.gz`` containing all Seal
components.  Inside ``build`` you'll also find the individual components:

* ``seal.jar``;
* ``lib`` directory, containing Python modules.


Creating the documentation
----------------------------

You can find the documentation for Seal at http://biodoop-seal.sourceforge.net/.

If however you want to build yourself a local copy, you can do so by:

#. installing Sphinx_
#. going to the Seal directory
#. running:

::

  make doc



You'll find the documentation in HTML in ``docs/_build/html/index.html``.


Installing Sphinx
....................


:On Gentoo:

  emerge sphinx

:On Ubuntu:

  sudo apt-get install python-sphinx

:Generic:

  easy_install -U Sphinx



Footnotes
++++++++++++


.. _Pydoop: https://sourceforge.net/projects/pydoop/
.. _Hadoop: http://hadoop.apache.org/
.. _Python: http://www.python.org
.. _Ant: http://ant.apache.org
.. _Protobuf: http://code.google.com/p/protobuf/
.. _JUnit 4: http://www.junit.org/
.. _distutils: http://docs.python.org/install/index.html
.. _Oracle Java 6: http://java.com/en/download/index.jsp
.. [#pner] You'll have to enable the "partner" repository in ``/etc/apt/sources.list`` to access the package
.. [#pydoop_dep] This package is required by Pydoop
.. _Sphinx:  http://sphinx.pocoo.org/
