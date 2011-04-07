Installation
==============

A pre-built jar is included in the distribution root.  If you want to
rebuild it from source, run `ant <http://ant.apache.org>`_ in the ReadSort root
directory.

Note that ``read_sort`` expects ``ReadSort.jar`` to be in its
parent directory.

Requirements
++++++++++++++

* Hadut (tested with ver. 0.1.1)
* `Python <http://www.python.org>`_ (tested with ver. 2.6)
* `Hadoop <http://hadoop.apache.org>`_ (tested with version ver. 0.20)

To run the unit tests you'll also need:

* JUnit 4 (tested with vers. 4.8.2);
* ant-junit (on Ubuntu install the ant-optional package).


Hadoop
+++++++

It is recommended that you set the HADOOP_HOME environment variable to
point to your Hadoop installation; as an alternative, ensure that the 
``hadoop`` executable is in your ``PATH``.
In addition, if you use a non-standard Hadoop configuration directory,
the ``HADOOP_CONF_DIR`` environment variable has to be set to point to
that directory.

For instance, if Hadoop is installed in ``/opt/hadoop`` and its
configuration directory is in ``/etc/hadoopconf``::

 export HADOOP_HOME="/opt/hadoop"
 export HADOOP_CONF_DIR="/etc/hadoopconf"

