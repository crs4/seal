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
        self.hdfs = None
        self.options = None
        self.left_over_args = None
        self.logger = None

    def parquet_args(self, in_out=None):
        args = []

        if in_out == 'input':
            args.extend( (
                '--input-format', 'parquet.avro.AvroParquetInputFormat',
                '--avro-input', 'v',
                ))
        elif in_out == 'output':
            with open(os.path.join(seal.seal_dir(), 'lib', 'io', 'Fragment.avsc')) as f:
                avro_schema = f.read()
            args.extend( (
                '--output-format', 'parquet.avro.AvroParquetOutputFormat',
                '--avro-output', 'v',
                '-Dpydoop.mapreduce.avro.value.output.schema=%s' % avro_schema,
                '-Dparquet.avro.schema=%s' % avro_schema,
            ))
        else:
            args.extend( (
                '--libjars', seal.parquet_jar_path(),
                '--mrv2',
                ))

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
        logging.basicConfig()
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

        # reference
        self.properties['mapred.cache.archives'] = '%s#reference' % self.options.reference

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

        self.properties[props.InputFormat] = props.Bdg if self.options.input_format == 'bdg' else props.Prq
        self.properties[props.OutputFormat] = props.Bdg if self.options.output_format == 'bdg' else props.Sam

    def run(self):
        self.logger.setLevel(logging.DEBUG)
        if self.options is None:
            raise RuntimeError("You must call parse_cmd_line before run")

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Running Seqal")
            self.logger.debug("Properties:\n%s",
                    "\n".join( sorted([ "%s = %s" % (str(k), str(v)) for k,v in self.properties.iteritems() ]) ))
        self.logger.info("Input: %s; Output: %s; reference: %s",
                self.options.input, self.options.output, self.options.reference)

        pydoop_argv = [ 'submit' ]

        # some properties have "pydoop submit" command line arguments which should be preferred
        if self.properties.has_key('mapred.job.name'):
            pydoop_argv.extend( ('--job-name', self.properties.pop('mapred.job.name')) )
        if self.properties.has_key('mapred.reduce.tasks'):
            pydoop_argv.extend( ('--num-reducers', str(self.properties.pop('mapred.reduce.tasks'))) )
        if self.properties.has_key('mapred.cache.archives'):
            pydoop_argv.extend( ('--cache-archive', self.properties.pop('mapred.cache.archives')) )

        if self.properties[props.InputFormat] == props.Bdg:
            pydoop_argv.extend(self.parquet_args('input'))
        if self.properties[props.OutputFormat] == props.Bdg:
            pydoop_argv.extend(self.parquet_args('output'))

        if props.Bdg in (self.properties[props.InputFormat], self.properties[props.OutputFormat]):
            pydoop_argv.extend(self.parquet_args())
            pydoop_argv.extend( ('--entry-point', 'run_avro_job' ))
        else:
            pydoop_argv.extend( ('--entry-point', 'run_standard_job' ))

        pydoop_argv.extend( "-D{}={}".format(k, v) for k, v in self.properties.iteritems() )

        pydoop_argv.append('seal.seqal.seqal_run')
        pydoop_argv.extend(self.left_over_args)
        pydoop_argv.append(self.options.input)
        pydoop_argv.append(self.options.output)

        self.logger.debug("Calling pydoop.app.main with these args:")
        self.logger.debug(pydoop_argv)
        self.logger.info("Lauching job")
        pydoop_main(pydoop_argv)
        self.logger.info("finished")

    def __validate(self):
        if self.properties['mapred.reduce.tasks'] == 0:
            self.logger.info("Running in alignment-only mode (no rmdup).")

        # validate conditions
        if phdfs.path.exists(self.options.output):
            raise SeqalConfigError(
                    "Output directory %s already exists.  "
                    "Please delete it or specify a different output directory." % self.options.output)
        if not phdfs.path.exists(self.options.reference):
            raise SeqalConfigError("Can't read reference archive %s" % self.options.reference)



def run_avro_job():
    """
    Runs the Hadoop pipes task through Pydoop
    """
    from pydoop.mapreduce.pipes import run_task, Factory
    from pydoop.avrolib import AvroContext
    from seal.seqal.mapper import mapper
    from seal.seqal.reducer import reducer
    return run_task(Factory(mapper, reducer), context_class=AvroContext)


def run_standard_job():
    """
    Runs the Hadoop pipes task through Pydoop
    """
    from pydoop.mapreduce.pipes import run_task, Factory
    from seal.seqal.mapper import mapper
    from seal.seqal.reducer import reducer
    return run_task(Factory(mapper, reducer))


def main(argv=None):
    retcode = 0

    run = SeqalSubmit()
    try:
        run.parse_cmd_line(argv)
        retcode = run.run()
    except SeqalConfigError as e:
        logger = logging.getLogger(SeqalSubmit.LogName)
        logger.critical("Error in Seqal run configuration")
        logger.critical(">>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        logger.critical(e)
        logger.critical(">>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        retcode = 1

    if retcode != 0:
        print >>sys.stderr, "Error running Seqal"
    return retcode
