#!/usr/bin/env python

import os
import sys
import random
import subprocess
import logging
import pydoop.hdfs

tool, galaxy_input, galaxy_output, logfile = sys.argv[1:5]

if len(sys.argv) > 5:
	options = sys.argv[5:]
else:
	options = []

logging.basicConfig(filename=logfile)
log = logging.getLogger(os.path.basename(tool))

hdfs = pydoop.hdfs.hdfs("default", 0)
log.debug("connected to hdfs at %s", hdfs.host)


# hack to read the input path directly from the command line
#input_paths = [ galaxy_input ]

with open(galaxy_input) as f:
	input_paths = [ s.rstrip("\n") for s in f.readlines() ]

output_path = os.path.join( hdfs.working_directory(), "galaxy-wrapper-%f" % random.random())

hdfs.close()
log.debug("hdfs closed")

log.debug("options: %s", options)
log.debug("input: %s", input_paths)
log.debug("output: %s", output_path)

with open(galaxy_output, 'w') as f:
	f.write(output_path)

command = [tool]
command.extend(options)
command.extend(input_paths)
command.append(output_path)

try:
	with open(logfile, 'w') as output_handle:
		retcode = subprocess.call(command, stdout=output_handle, stderr=output_handle)
	if retcode < 0:
		log.critical("%s was terminated by signal %d",tool, -retcode)
		sys.stderr.write("%s was terminated by signal %d" % (tool, -retcode))
	elif retcode > 0:
		log.critical("%s returned %d", tool, retcode)
		sys.stderr.write("%s returned %d" % (tool, retcode))
	sys.exit(retcode)
except OSError, e:
	log.critical("Execution failed: %s", e)
	sys.stderr.write("Execution failed: %s" % e)
	sys.exit(1)
