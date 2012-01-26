.. _installation:

Installation
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


Detailed Installation Instructions
+++++++++++++++++++++++++++++++++++++


* :ref:`Installing on Ubuntu <installation_ubuntu>` (should work for Debian as well)
* :ref:`Installing on Gentoo <installation_gentoo>`
* :ref:`Generic installation <installation_generic>`

And then, see the deployment instructions/suggestions in the section
:ref:`Installation - Deploying <installation_deploying>`.



Upgrading from previous versions
+++++++++++++++++++++++++++++++++++++

There is no particular upgrade procedure (but see the notes below).  Just
build and deploy Seal following the instructions in the section above.

Upgrading from 0.1.x or 0.2.x
-----------------------------------

Make sure you **copy any custom property settings from the old launcher
scripts** under bin/ into a :ref:`Seal configuration file <seal_config>`.
