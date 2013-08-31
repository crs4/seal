.. _installation_deploying:

Installation - Deploying
========================

The Seal installation path **needs to be accessible to all the nodes in your
Hadoop cluster**.  In addition, Seal's runtime dependencies also need to be
accessible to all cluster nodes.


Deployment
++++++++++++

Deployment strategies for Seal on your cluster will vary depending on your
particular set-up.

Shared Volume
---------------

If your cluster has a shared volume that is accessible from all nodes from the
same mount point, you can install Seal to a directory on that volume.  For
instance::

  python setup.py install --home /my/shared/volume

Just make sure that the `PYTHONPATH` variable includes the `site-packages`
directory created in your selected install path.

Manual Distribution
---------------------

For large clusters, it may be necessary to distribute the software to the
various nodes rather than relying on a shared volume.  In our tests, a volume
shared via NFS was not able to handle the demands of a cluster with 128 nodes,
resulting in tasks that never went past the "initializing" state and eventually
timed out.

If you have a cluster this large, you should probably ask your system
administrators for advice on how to install Seal.


.. _Pydoop: https://sourceforge.net/projects/pydoop/
.. _Hadoop: http://hadoop.apache.org/
.. _Python: http://www.python.org
.. _Ant: http://ant.apache.org
.. _Protobuf: http://code.google.com/p/protobuf/
.. _JUnit 4: http://www.junit.org/
.. _pdsh: https://sourceforge.net/projects/pdsh/
.. _distutils: http://docs.python.org/install/index.html
.. _Oracle Java 6: http://java.com/en/download/index.jsp
