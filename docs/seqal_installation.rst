Installation
============


Supported Platforms
-------------------

Seqal has been tested on `Gentoo <http://www.gentoo.org>`_ and `CentOS
<http://www.centos.org>`_, and `Ubuntu <http://www.ubuntu.com/>`_. Although 
we haven't tested it on other Linux distributions, we expect Seqal to work 
on them as well. Platforms other than Linux are currently not supported.


Dependencies
-------------

In order to build and install Seqal, you need the following software:

* `Python <http://www.python.org>`_ (tested with 2.6)
* `Pydoop <http://pydoop.sourceforge.net>`_ version 0.3.7_rc1
* `Protobuf <http://code.google.com/p/protobuf>`_ (tested with version 2.3.0)
* `Biopython <http://biopython.org>`_ (tested with version 1.56)
* `Hadoop <http://hadoop.apache.org>`_ (tested with version 0.20)

Your distribution probably has packages for Pydoop, Protobuf, and Biopython.

Seqal and its dependencies must be installed so that they are accessible from
all cluster nodes.  For small clusters (less than 32 nodes) it should be ok to
install everything on a shared file system.  On the other hand, for larger 
clusters it may be necessary to install locally to all cluster nodes, depending
on the speed of the shared volume.

And of course, to run Seqal you'll need a working `Hadoop <http://hadoop.apache.org>`_
cluster.  Please refer to the Hadoop web site for instructions.  We tested Seqal 
with Hadoop version 0.20.2.


Debian/Ubuntu packages
+++++++++++++++++++++++++

If you're using a Debian-based distribution, you can satisfy all the 
dependencies except for Pydoop (which at the moment doesn't provide a deb 
package) by installing these packages:

* sun-java6-jdk
* python
* protobuf-compiler
* libprotobuf6
* libprotoc6
* python-protobuf
* python-biopython


Setup instructions
-------------------

System-wide installation
+++++++++++++++++++++++++

Run ``python setup.py install`` (as root) in the Seqal distribution
root directory. 

To install as an unprivileged (but sudoer) user you can run::

  python setup.py build
  sudo python setup.py install --skip-build

Installing to a specific location
++++++++++++++++++++++++++++++++++

To install Seqal to an arbitrary location::

  python setup.py build
  python setup.py install --home <desired path>

You can then ``export SeqalPath=<desired path>/lib/python`` so that ``run_seqal.sh`` 
may find the files, and set ``PYTHONPATH=<desired path>/lib/python`` when
running the unit tests.

Local installation
+++++++++++++++++++

To install into the user's home directory (i.e., into ``~/.local/lib/python2.6/site-packages``\ )::

  python setup.py install --user

Wherever you decide to install Seqal, it needs to be accessible to all the
cluster nodes.

Testing Your Installation
-------------------------

After Seqal has been successfully installed, you might want to run
unit tests to verify that everything works fine. Move to the ``tests``
subdirectory and run::

  python run_py_unit_tests.py

Remember to export PYTHONPATH appropriately if Seqal isn't installed to a
default Python path.
