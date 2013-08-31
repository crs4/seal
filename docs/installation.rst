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

We recommend you download the latest release from here:  http://sourceforge.net/projects/biodoop-seal/files/.

On the other hand, if you want to try the latest improvements or contributions, you can checkout the latest code
from our repository::

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

Fill

Upgrading from 0.1.x or 0.2.x
-----------------------------------

Make sure you **copy any custom property settings from the old launcher
scripts** under bin/ into a :ref:`Seal configuration file <seal_config>`.
