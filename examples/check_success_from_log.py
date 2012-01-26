#!/usr/bin/env python

# Copyright (C) 2011-2012 CRS4.
#
# This file is part of Seal.
#
# Seal is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Seal is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Seal.  If not, see <http://www.gnu.org/licenses/>.

################################################################################
# This little script is required by the example workflow HBwa_workflow_pe.sh
################################################################################

# Given a Hadoop log directory, checks whether the job was successful.

import os
import re
import sys

def find_last_job_status_line(log_name):
	last_job_status = None
	with open(log_name) as f:
		for line in f:
			if line.startswith("Job JOBID") and re.search("JOB_STATUS=", line):
				last_job_status = line
	return last_job_status

def get_success_value(job_line):
	m = re.search('\sJOB_STATUS="(\w+)"', job_line)
	if m and m.group(1) == "SUCCESS":
		return True
	else:
		return False

def check_success_from_log(fname):
	line = find_last_job_status_line(fname)
	if line:
		return get_success_value(line)
	else:
		print >>sys.stderr, "Could not get job status line from log"
		sys.exit(1)

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print >>sys.stderr, "Wrong number of arguments.  Usage:  %s <log file>" % sys.argv[0]
		sys.exit(1)
	elif not os.access(sys.argv[1], os.R_OK):
		print >>sys.stderr, "Can't read log file %s." % sys.argv[1]
		sys.exit(1)
	else:
		if check_success_from_log(sys.argv[1]):
			sys.exit(0)
		else:
			sys.exit(2)
