.. _tsv_sort_index:

TsvSort
==========

TsvSort is a Hadoop utility to sort text files.  It allows you to specify a
field delimiter charater/string and the indices of the keys by which to sort.
TsvSort is based on the Terasort algorithm so it provides a scalable, distributed sorting program.



Usage
++++++++

TsvSort interprets your data as string-delimited, line-oriented records.  The
default field delimiter is the TAB character.  They default sort behaviour is to
use the entire lines as the key.

You can specify a custom field delimiter string with ``-t`` or ``--field-separator``.

You can specify the key fields to use with the ``-k`` or ``--key`` options.
About specifying fields:

* you can specify any list of field numbers separated by commas;
* you can specify field ranges by separating two numbers with '-';
* the list of fields you specify must be in ascending order.


Examples
...............

Sorting your data by entire lines::

 seal tsvsort input_dir output_dir

TsvSort will produce a series of numbered sorted files.  By reading them in
order, from 0 to `n`, you will see the lines in sorted order.

Creating a single sorted file requires two steps:  sorting and concatenating.

#. ``seal tsvsort input_dir output_dir``
#. ``hadoop dfs -cat output_dir/part-r-* > myfile``

Sorting by the first column::

  seal tsvsort -k 1 input_dir output_dir

Sorting by the second and fourth columns::

  seal tsvsort -k 2,4 input_dir output_dir

Sorting by the second, third, fourth, and seventh columns::

  seal tsvsort -k 2-4,7 input_dir output_dir

Sorting a comma-delimited file by the second column::

  seal tsvsort -t , -k 2 input_csv_dir output_dir


``seal tsvsort`` follows the normal Seal usage convention.  See the section
:ref:`program_usage` for details.


Configurable Properties
++++++++++++++++++++++++++


================================ ===========================================================
**Name**                           **Meaning**
-------------------------------- -----------------------------------------------------------
textsampler.sample.size          The number of sample lines to use when to calibrate the
                                 partitions.  Before starting to sort the data, TsvSort
                                 reads this many lines to estimate the distribution of the
                                 data and set partition points.  *Default value*:  100000.
================================ ===========================================================

You can use TsvSort config section to configure any property values
specific to TsvSort.

.. note:: **Config File Section Title**: TsvSort


Weaknesses
+++++++++++++++

Note that TsvSort was implemented as a simple utility, which has since grown,
but just slightly!  As such, you may find it's a little spartan.  Patches are
welcome, as always!

Here are some particular weaknesses we may decide to address in the future.

* TsvSort only sorts alphabetically.  You currently can't sort numerically.
* Key fields must be in ascending order.  We may in the future add the ability
  to use fields out-of-order as the sort key.

