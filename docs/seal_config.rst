.. _seal_config:

Seal Config File
===========================

All Seal tools can read properties from a Seal configuration file.  This allows you
to write the properties you normally use to a file and re-use them for all runs,
thus favouring manageability and simplicity.

.. note::  Default configuration file: **$HOME/.sealrc**

If a configuration file exists at the default location, the Seal tools will load
it **automatically**.

You can override the default configuration filename with the option
``--seal-config``.  Thus, you can have multiple preset configurations.  For
instance,

::

  ./bin/seqal --seal-config rna-runs.cfg input output reference.tar

  ./bin/seqal --seal-config full_sample_runs.cfg input output reference.tar



Config File Format
+++++++++++++++++++++++++++++++++

The configuration file format resembles very much the traditional Windows INI
file format and the Python ConfigParser file format; the main difference is that
the Seal configuration is case-sensitive.  Here are the main points:

* A section starts with the syntax ``[SectionName]``.  All properties set in the
  lines following the section title will be part of the named section.
* Properties are set as in ``property_name = value`` or ``property_name: value``.  
* There is a "DEFAULT" section whose properties are inherited by all tools.
  Properties set before any section is declared also go into this section.
* Blank lines are ignored.
* Line comments start with ``#`` or ``;``
* Whitespace around property names and values is ignored.

Here is an example::

  # Before any section you can specify properties that will apply to all sections.
  # They go in the DEFAULT section.
  # You can also use this file to specify Hadoop configuration properties.
  mapred.compress.map.output=true

  [DEFAULT]
  # You can also explicitly specify the DEFAULT section
  
  [Prq]
  # this property will only be read by Prq
  bl.prq.min-bases-per-read = 40

  [Seqal]
  # maybe you usually have sanger-encoded base qualities
  bl.seqal.fastq-subformat=fastq-sanger

  # you can also specify Hadoop configuration options specific to one 
  # tool (seqal in this case). This property setting will override the default
  # setting above, but only for seqal.
  mapred.compress.map.output=false

Precedence
+++++++++++++

When the same property is set in various places, the following precedence order
is used to decide which one to use:


#. Command line. Values set with ``-D`` on the command line have the highest precedence.
#. Tool's own section in config file.
#. DEFAULT section in config file.
#. Default property values, if any.


