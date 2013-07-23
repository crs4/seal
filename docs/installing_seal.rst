.. _installing_seal:

Installing Seal
===================

To begin, Seal needs to know where to find the HadoopBAM jars (if you don't have
it yet, see the :ref:`installation_dependencies` page before continuing).  To tell
it, set the environment variable `HADOOP_BAM` to point to the root HadoopBAM
path::

  export HADOOP_BAM=/home/me/hadoop-bam

Then follow the typical procedure for building Python programs::

  python setup.py build
  python setup.py install

The above will install to the current system's `/usr/local/`, which is probably
not what you want unless you're testing on a single-node installation (see
:ref:`installation_deploying`).  To specify a different installation directory
try

::

  python setup.py install --user

to install to `$HOME/.local/` or

::

  python setup.py install --home <PATH>

to choose a specific installation path.



Creating the documentation
----------------------------

You can find the documentation for the latest Seal release at
http://biodoop-seal.sourceforge.net/.

If however you want to build yourself a local copy, you can do so by installing
Sphinx, going to the Seal directory and running

::

  python setup.py build_docs


You'll find the documentation in HTML in ``docs/_build/html/index.html``.


Running unit tests
+++++++++++++++++++++

From the Seal source directory, after having built Seal, you can run the unit
tests with the following commands::

  PYTHONPATH=`ls -d $(pwd)/build/lib*` python setup.py run_unit_tests

The expression assigned to `PYTHONPATH` tries to find the `lib` or
`lib.linux...` subdirectory created by the build process under `build`. If it
doesn't work, just look it up and insert the value manually; e.g.,

::

  18:40 [pireddu@slynx seal.git] pwd
  /u/pireddu/Projects/seal.git
  18:40 [pireddu@slynx seal.git] ls build
  lib.linux-x86_64-2.7  scripts-2.7
  18:40 [pireddu@slynx seal.git] export PYTHONPATH=/u/pireddu/Projects/seal.git/build/lib.linux-x86_64-2.7/
  18:40 [pireddu@slynx seal.git] python setup.py run_unit_tests  


Running integration tests
++++++++++++++++++++++++++

Seal also includes a number of integration tests that actually run the
applications on known input and verify that the output is as expected.  To run
these tests **you must have correctly installed Seal and your Hadoop must be
running and accessible**.  In fact, running these is a good way to ensure your
whole system is working properly.

To run the integration tests try::

  python setup.py run_integration_tests

