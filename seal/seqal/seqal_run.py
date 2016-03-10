# Copyright (C) 2011-2012 CRS4.
#
# This file is part of Seal.
#
# Seal is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version.
#
# Seal is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with Seal.  If not, see <http://www.gnu.org/licenses/>.

import seal
import seal.lib.deprecation_utils as deprecation
from seal.seqal.seqal_config import SeqalConfig, SeqalConfigError
import seal.seqal.properties as props

import pydoop.hdfs as phdfs
import pydoop.hadut as hadut
from pydoop.app.main import main as pydoop_main


import logging
import os
import sys
import traceback

def _write_env_info(io):
    io.write("=================================================\n")
    io.write("Python version: %s\n" % sys.version)
    import pydoop
    io.write("Pydoop version: %s\n" % pydoop.__version__)
    io.write("Seal version:   %s\n" % seal.__version__)
    io.write("=================================================\n")

def run_avro_job():
    """
    Runs the Hadoop pipes task through Pydoop
    """
    _write_env_info(sys.stderr)
    from pydoop.mapreduce.pipes import run_task, Factory
    from pydoop.avrolib import AvroContext
    from seal.seqal.mapper import mapper
    # seqal reducer is currently disabled
    #from seal.seqal.reducer import reducer
    #return run_task(Factory(mapper, reducer), context_class=AvroContext)
    return run_task(Factory(mapper), context_class=AvroContext)


def run_standard_job():
    """
    Runs the Hadoop pipes task through Pydoop
    """
    _write_env_info(sys.stderr)
    from pydoop.mapreduce.pipes import run_task, Factory
    from seal.seqal.mapper import mapper
    # seqal reducer is currently disabled
    #from seal.seqal.reducer import reducer
    #return run_task(Factory(mapper, reducer))
    return run_task(Factory(mapper))


def format_str_to_prop(string):
    formats = {
      'bdg': props.Bdg,
      'prq': props.Prq,
      'sam': props.Sam,
      'avo': props.Avo,
    }

    try:
        return formats[string]
    except IndexError:
        raise ValueError("Unrecognized format '%s'" % string)


class SeqalSubmit(object):

    DefaultReduceTasksPerNode = 6
    LogName = "seqal"
    DefaultLogLevel = 'INFO'

    ConfLogLevel = 'seal.seqal.log.level'
    ConfLogLevel_deprecated = 'bl.seqal.log.level'

    def __init__(self):
        self.parser = SeqalConfig()

        # set default properties
        self.properties = {
            self.ConfLogLevel: self.DefaultLogLevel,
            'mapred.create.symlink': 'yes',
            'mapred.compress.map.output': 'true',
        }
        self.options = None
        self.left_over_args = None
        self.logger = None

    def parquet_args(self, in_out=None, typename=None):
        args = []

        if in_out == 'input':
            args.extend( (
                '--input-format', 'org.apache.parquet.avro.AvroParquetInputFormat',
                '--avro-input', 'v',
                ))
        elif in_out == 'output':
            if not typename:
                raise ValueError("You must provide a typename for the output schema")

            schema_file = os.path.join(seal.avro_schema_dir(), "%s.avsc" % typename)

            if not os.path.exists(schema_file):
                raise RuntimeError("Unknown typename %s" % typename)

            with open(schema_file) as f:
                avro_schema = f.read()
            args.extend( (
                '--output-format', 'org.apache.parquet.avro.AvroParquetOutputFormat',
                '--avro-output', 'v',
                '-Dpydoop.mapreduce.avro.value.output.schema=%s' % avro_schema,
                '-Dparquet.avro.schema=%s' % avro_schema,
            ))
        else:
            args.append('--mrv2')

        return args

    def parse_cmd_line(self, args):
        self.options, self.left_over_args = self.parser.load_config_and_cmd_line(args)

        # set the job name.  Do it here so the user can override it
        self.properties['mapred.job.name'] = 'seqal_aln_%s' % self.options.output

        # now collect the property values specified in the options and
        # copy them to properties
        for k,v in self.options.properties.iteritems():
            self.properties[k] = v

        # create a logger
        seal.config_logging()
        self.logger = logging.getLogger(self.__class__.LogName)
        # temporarily set to a high logging level in case we have to print warnings
        # regarding deprecated properties
        self.logger.setLevel(logging.DEBUG)
        # warn for deprecated bl.seqal.log.level property
        if self.properties.has_key(self.ConfLogLevel_deprecated):
            deprecation.deprecation_warning(self.logger, self.ConfLogLevel_deprecated, self.ConfLogLevel)
            if self.properties[self.ConfLogLevel] == self.DefaultLogLevel and \
                self.properties[self.ConfLogLevel_deprecated] != self.DefaultLogLevel:
                # the deprecated property is different from default, while the new property is not.  Therefore,
                # the user has set the deprecated property to a new value.  We'll use that one.
                self.properties[self.ConfLogLevel] = self.properties[self.ConfLogLevel_deprecated]
                self.logger.warning("Using value %s for property %s (value taken from its deprecated equivalent property %s).",
                    self.properties[self.ConfLogLevel], self.ConfLogLevel, self.ConfLogLevel_deprecated)

        # Set proper logging level
        log_level = getattr(logging, self.properties['seal.seqal.log.level'], None)
        if log_level is None:
            self.logger.setLevel(logging.DEBUG)
            self.logger.warning("Invalid configuration value '%s' for %s.  Check your configuration.",
                    self.ConfLogLevel, self.properties['seal.seqal.log.level'])
            self.logger.warning("Falling back to DEBUG")
            self.logger.warning("Valid values for seal.seqal.log.level are: DEBUG, INFO, WARNING, ERROR, CRITICAL; default: %s",
                    SeqalSubmit.DefaultLogLevel)
        else:
            self.logger.setLevel(log_level)

        self._setup_reference()

        # set the number of reduce tasks
        if self.options.align_only:
            n_red_tasks = 0
            if self.options.num_reducers and self.options.num_reducers > 0:
                self.logger.warning("Number of reduce tasks must be 0 when doing --align-only.")
                self.logger.warning("Ignoring request for %d reduce tasks", self.options.num_reducers)
        elif self.options.num_reducers:
            n_red_tasks = self.options.num_reducers
        else:
            n_red_tasks = SeqalSubmit.DefaultReduceTasksPerNode * hadut.get_num_nodes()

        self.properties['mapred.reduce.tasks'] = n_red_tasks

        self.properties[props.InputFormat] = format_str_to_prop(self.options.input_format)
        self.properties[props.OutputFormat] = format_str_to_prop(self.options.output_format)

    def _setup_reference(self):
        if self.options.ref_archive:
            if phdfs.path.exists(self.options.ref_archive):
                symlink = 'reference'
                self.properties['mapred.create.symlink'] ='yes'
                self.properties['mapred.cache.archives'] = '%s#%s' % (self.options.ref_archive, symlink)
                self.properties[props.LocalReferenceDir] = symlink
            else:
                raise SeqalConfigError("reference archive %s doesn't exist" % self.options.ref_archive)
        elif self.options.ref_prefix:
            if not os.path.exists(os.path.dirname(self.options.ref_prefix)):
                raise SeqalConfigError("Reference directory %s doesn't exist" % os.path.dirname(self.options.ref_prefix))
            abs_path = os.path.abspath(self.options.ref_prefix)
            self.properties[props.LocalReferencePrefix] = abs_path
        else:
            raise RuntimeError("Shouldn't get here if neither reference archive nor a prefix are specified")

    def run(self):
        self.logger.setLevel(logging.DEBUG)
        if self.options is None:
            raise RuntimeError("You must call parse_cmd_line before run")

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Running Seqal")
            self.logger.debug("Properties:\n%s",
                    "\n".join( sorted([ "%s = %s" % (str(k), str(v)) for k,v in self.properties.iteritems() ]) ))
        self.logger.info("Input: %s; Output: %s; reference: %s",
                self.options.input, self.options.output, self.options.ref_archive or self.options.ref_prefix)

        self.__validate()

        pydoop_argv = [ 'submit' ]

        pydoop_argv.extend(('--log-level', logging.getLevelName(self.logger.getEffectiveLevel())))

        # some properties have "pydoop submit" command line arguments which should be preferred
        if self.properties.has_key('mapred.job.name'):
            pydoop_argv.extend( ('--job-name', self.properties.pop('mapred.job.name')) )
        if self.properties.has_key('mapred.reduce.tasks'):
            pydoop_argv.extend( ('--num-reducers', str(self.properties.pop('mapred.reduce.tasks'))) )
        if self.properties.has_key('mapred.cache.archives'):
            pydoop_argv.extend( ('--cache-archive', self.properties.pop('mapred.cache.archives')) )

        if self.properties[props.InputFormat] in (props.Bdg, props.Avo):
            pydoop_argv.extend(self.parquet_args('input'))
        if self.properties[props.OutputFormat] == props.Bdg:
            pydoop_argv.extend(self.parquet_args(in_out='output', typename='Fragment'))
        elif self.properties[props.OutputFormat] == props.Avo:
            pydoop_argv.extend(self.parquet_args(in_out='output', typename='AlignmentRecord'))

        if set((props.Avo, props.Bdg)) & set((self.properties[props.InputFormat], self.properties[props.OutputFormat])):
            pydoop_argv.extend(self.parquet_args())
            pydoop_argv.extend( ('--entry-point', 'run_avro_job' ))
        else:
            pydoop_argv.extend( ('--entry-point', 'run_standard_job' ))

        pydoop_argv.extend(seal.libjars('python'))
        pydoop_argv.extend( "-D{}={}".format(k, v) for k, v in self.properties.iteritems() )

        pydoop_argv.append('seal.seqal.seqal_run')
        pydoop_argv.extend(self.left_over_args)
        pydoop_argv.append(self.options.input)
        pydoop_argv.append(self.options.output)

        self.logger.debug("Calling pydoop.app.main with these args:")
        self.logger.debug(pydoop_argv)
        self.logger.info("Lauching job")
        pydoop_main(pydoop_argv)
        # XXX: pydoop main doesn't return an exit code
        self.logger.info("finished")
        return 0

    def __validate(self):
        try:
            import pyrapi
        except ImportError:
            raise SeqalConfigError("Failed to import 'pyrapi' module")

        if self.properties['mapred.reduce.tasks'] == 0:
            self.logger.info("Running in alignment-only mode (no rmdup).")

        # validate conditions
        if phdfs.path.exists(self.options.output):
            raise SeqalConfigError(
                    "Output directory %s already exists.  "
                    "Please delete it or specify a different output directory." % self.options.output)

        if sum(1 for e in (self.options.ref_archive, self.options.ref_prefix) if e) != 1:
            raise SeqalConfigError("You must specify either a reference archive or a reference prefix")

def main(argv=None):
    retcode = 0

    logger = logging.getLogger(SeqalSubmit.LogName)
    run = SeqalSubmit()
    try:
        run.parse_cmd_line(argv)
        retcode = run.run()
    except SeqalConfigError as e:
        logger.critical("Error in Seqal run configuration")
        logger.critical(">>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        logger.critical(e)
        logger.critical(">>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        retcode = 2
    except Exception as e:
        logger.critical("Error running Seqal")
        logger.critical("%s: %s", type(e).__name__, e)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(traceback.format_exc())
        retcode = 1

    if retcode != 0:
        print >>sys.stderr, "Error running Seqal"
    return retcode
