
import bl.lib.tools.hadut
from bl.mr.seq.seqal.seqal_parser import SeqalParser
import seal_dir

import pydoop.hdfs

import os
import sys
import random
import tempfile

class SeqalRun(object):

	DefaultReduceTasksPerNode = 6

	def __init__(self):
		parser = SeqalParser()

		# set default properties
		self.properties = {
			'bl.seqal.log.level': 'INFO',
			'hadoop.pipes.java.recordreader': 'true',
			'hadoop.pipes.java.recordwriter': 'true',
			'mapred.create.symlink': 'yes',
			'bl.libhdfs.opts': '-Xmx48m'
		}
		self.hdfs = None

		def parse_cmd_line(self):
			self.options, self.left_over_args = self.parser.parse()

			# set the job name.  Do it here so the user can override it
			self.properties['mapred.job.name'] = 'seqal_aln_%s' % options.output

			# now collect the property values specified in the options and
			# copy them to properties
			for k,v in options.properties.iteritems():
				self.properties[k] = v

			self.properties['mapred.cache.archives'] = '%s#reference' % options.reference
			self.properties['bl.seqal.trim.qual'] = options.trimq

			# set the number of reduce tasks
			if options.num_reducers:
				n_red_tasks = options.num_reducers
			else:
				n_red_tasks = SeqalRun.DefaultReduceTasksPerNode * hadut.num_nodes()

			self.properties['mapred.reduce.tasks'] = n_red_tasks

		def __set_env_vars(self):
			evars = {}

		def run(self):
			try:
				self.hdfs = pydoop.hdfs.hdfs('default', 0)
				self.__validate()
				self.__set_env_vars()

				self.remote_bin_name = tempfile.mktemp(prefix='seqal_bin.', suffix=str(random.random()), dir='')
				try:
					with self.hdfs.open_file(self.remote_bin_name, 'w') as script:
						script.write("#!/bin/bash\n")
						script.write('""":"\n')
						script.write('export LD_LIBRARY_PATH="%s:%s" # LD_LIBRARY_PATH copied from the env where you ran %s\n' % (seal_dir.SealDir, os.environ.get('LD_LIBRARY_PATH', ''), sys.argv[0])
						script.write('export PYTHONPATH="%s"\n' % os.environ.get('PYTHONPATH', ''))
						script.write('exec "%s" -u "$0" "$@"\n' % sys.executable)
						script.write('":"""\n')
						script.write('from bl.mr.seq.seqal import run_task\n')
						script.write('run_task()\n')

					return hadut.run_pipes(self.remote_bin_name, self.options.input, self.options.output, 
						properties=self.properties, args_list=self.left_over_args)
				finally:
					self.hdfs.delete(self.remote_bin_name) # delete the temporary pipes script from HDFS
			finally:
				if self.hdfs:
					tmp = self.hdfs
					self.hdfs = None
					tmp.close()

		def __validate(self)
			if self.properties['mapred.reduce.tasks'] == 0:
				print "Running in BWA-only mode (no rmdup).  Number of reduce tasks:", n_red_tasks

			# validate conditions
			if hdfs.exists(self.options.output):
				raise RuntimeError("Output directory %s already exists.  Please delete it or specify a different output directory." % self.options.output)
			if not hdfs.exists(self.options.reference):
				raise RuntimeError("Can't read reference archive %s" % self.options.reference)





