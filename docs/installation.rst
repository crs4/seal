.. _installation:

Installation
========================

Supported Platforms
+++++++++++++++++++

Seal is tested and developed on `Ubuntu <http://www.ubuntu.com/>`_ and `CentOS
<http://www.centos.org>`_.  However, in principle Seal should work on any Linux
platform.

If Seal doesn't build or run on your Linux distribution of interest please
submit a bug report and we can try to fix it.


Get the source
+++++++++++++++++

We recommend you download from here:  http://github.com/crs4/seal.

Currently, we recommend you use the development version.  It may at times be a
little unstable, but it's up-to-date with technology.  The official releases are
rather stale.  So, clone the development branch from here::

  git clone https://github.com/crs4/seal.git


Installation Instructions
+++++++++++++++++++++++++++++++++++++

The typical installation procedure is:

* :ref:`Install dependencies <installation_dependencies>`, which will vary according
  to your platform;
* :ref:`Build and install Seal <installing_seal>`

Note that the software you installed (both Seal and its dependencies) need to be
accessible to all nodes in the Hadoop cluster.  See the deployment
instructions/suggestions in the section
:ref:`Installation - Deploying <installation_deploying>`.


Upgrading from previous versions
+++++++++++++++++++++++++++++++++++++

There is no particular upgrade procedure (but see the notes below).  Just
build and deploy Seal following the instructions in the section above and take
noticed of the notes below.


Upgrading from 0.3.x to 0.4.x
-----------------------------------

If you were setting configuration properties, either through the command line
(-D argument) or through a `.sealrc` file, make sure you update the names.
The :ref:`news page <news_changes_in_property_names>` contains a full list of
the names that changed.

The names of the seal executables have changed.  Now everything is invoked as::

  seal <subcommand>

Make sure you update any scripts.  A table with the name conversions is
available on the :ref:`news page <news_repackaging>`.

There are several new options in the Seal programs.  Have a look at them to make
sure you make the most of the new features.



Upgrading from 0.1.x or 0.2.x
-----------------------------------

Make sure you **copy any custom property settings from the old launcher
scripts** under bin/ into a :ref:`Seal configuration file <seal_config>`.
