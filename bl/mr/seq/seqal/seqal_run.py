# Copyright (C) 2011 CRS4.
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


import bl.lib.tools.hadut as hadut
from bl.mr.seq.seqal.seqal_options import SeqalOptions
import seal_path

import pydoop.hdfs

import logging
import os
import sys
import random
import tempfile

class SeqalRun(object):

	DefaultReduceTasksPerNode = 6

	def __init__(self):
		self.parser = SeqalOptions()

		# set default properties
		self.properties = {
			'bl.seqal.log.level': 'INFO',
			'hadoop.pipes.java.recordreader': 'true',
			'hadoop.pipes.java.recordwriter': 'true',
			'mapred.create.symlink': 'yes',
			'bl.libhdfs.opts': '-Xmx48m'
		}
		self.hdfs = None
		self.options = None

	def parse_cmd_line(self):
		self.options, self.left_over_args = self.parser.load_config_and_cmd_line()

		# set the job name.  Do it here so the user can override it
		self.properties['mapred.job.name'] = 'seqal_aln_%s' % self.options.output

		# now collect the property values specified in the options and
		# copy them to properties
		for k,v in self.options.properties.iteritems():
			self.properties[k] = v

		self.properties['mapred.cache.archives'] = '%s#reference' % self.options.reference
		self.properties['bl.seqal.trim.qual'] = self.options.trimq

		# set the number of reduce tasks
		if self.options.num_reducers:
			n_red_tasks = self.options.num_reducers
		else:
			n_red_tasks = SeqalRun.DefaultReduceTasksPerNode * hadut.num_nodes()

		self.properties['mapred.reduce.tasks'] = n_red_tasks

		logging.basicConfig(level=self.properties['bl.seqal.log.level'])

	def __write_pipes_script(self, fd):
		ld_path = ":".join( filter(lambda x:x, [seal_path.SealDir, os.environ.get('LD_LIBRARY_PATH', None)]) )
		pypath = os.environ.get('PYTHONPATH', '')
		self.logger.debug("LD_LIBRARY_PATH for tasks: %s", ld_path)
		self.logger.debug("PYTHONPATH for tasks: %s", pypath)

		fd.write("#!/bin/bash\n")
		fd.write('""":"\n')
		fd.write('export LD_LIBRARY_PATH="%s" # Seal dir + LD_LIBRARY_PATH copied from the env where you ran %s\n' % (ld_path, sys.argv[0]))
		fd.write('export PYTHONPATH="%s"\n' % pypath)
		fd.write('exec "%s" -u "$0" "$@"\n' % sys.executable)
		fd.write('":"""\n')
		fd.write('from bl.mr.seq.seqal import run_task\n')
		fd.write('run_task()\n')


	def run(self):
		if self.options is None:
			raise RuntimeError("You must call parse_cmd_line before run")

		self.logger = logging.getLogger("seqal")
		if self.logger.isEnabledFor(logging.DEBUG):
			self.logger.debug("Running Seqal")
			self.logger.debug("Input: %s; Output: %s; reference: %s", self.options.input, self.options.output, self.options.reference)
			self.logger.debug("Properties:\n%s", "\n".join( sort([ "%s = %s" % (str(k), str(v)) for k,v in self.properties.iteritems() ]) ))

		try:
			self.hdfs = pydoop.hdfs.hdfs('default', 0)
			self.__validate()

			self.remote_bin_name = tempfile.mktemp(prefix='seqal_bin.', suffix=str(random.random()), dir='')
			try:
				with self.hdfs.open_file(self.remote_bin_name, 'w') as script:
					self.__write_pipes_script(script)
		
				return hadut.run_pipes(self.remote_bin_name, self.options.input, self.options.output, 
					properties=self.properties, args_list=self.left_over_args)
			finally:
				try:
					self.hdfs.delete(self.remote_bin_name) # delete the temporary pipes script from HDFS
					self.logger.debug("pipes script %s deleted", self.remote_bin_name)
				except:
					self.logger.error("Error deleting the temporary pipes script %s from HDFS", self.remote_bin_name)
					pass
		finally:
			if self.hdfs:
				tmp = self.hdfs
				self.hdfs = None
				tmp.close()
				self.logger.debug("HDFS closed")

	def __validate(self):
		if self.properties['mapred.reduce.tasks'] == 0:
			self.logger.warning("Running in BWA-only mode (no rmdup).  Number of reduce tasks: %d", n_red_tasks)

		# validate conditions
		if self.hdfs.exists(self.options.output):
			raise RuntimeError("Output directory %s already exists.  Please delete it or specify a different output directory." % self.options.output)
		if not self.hdfs.exists(self.options.reference):
			raise RuntimeError("Can't read reference archive %s" % self.options.reference)
