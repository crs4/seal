#!/usr/bin/env python

# Copyright (C) 2011-2016 CRS4.
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

# usage:  ls <data_files> | make_output_name.py <LaneContent filename> <OutputDir>
# e.g. ls s_1_[12]_*.txt | make_output_name.py 100910_seqmachine_0056/LaneContent bwa_100910_seqmachine_0056

import sys
import os
import re

if len(sys.argv) != 3:
	raise RuntimeError("Wrong number of arguments.  Expecting <lane content file> <basename of output dir>")

### Get lanes
data_files = sys.stdin.readlines()
if len(data_files) == 0:
	raise RuntimeError("Not input files provided on standard input")


def get_lane_number(line):
	filename = os.path.basename(line.rstrip("\n\r"))
	m = re.match("s_(\d)_\d_\d{4}_qseq.txt", filename)
	if not m:
		raise ValueError("unrecognized qseq file name format %s" % filename)
	return int(m.group(1))


lanes = set(map(get_lane_number, data_files))

### get lane content

laneContent = dict()

with open(sys.argv[1]) as lane_content_file:
	for line in lane_content_file:
		lane_number, sample = line.rstrip("\n\r").split(":")
		laneContent[ int(lane_number) + 1 ] = sample

samples = set( [laneContent[laneno] for laneno in lanes] )
if len(samples) > 1:
	raise ValueError("lanes cover more than one sample! (%s)" % ', '.join(samples))

outputDir = sys.argv[2]
fields = outputDir.split("_", 4)
if len(fields) < 4:
	raise ValueError("unrecognized output name format")
date = fields[1]
machine_and_run_id = "_".join(fields[2:4])

print "%s#%s_%s#%s#" % (date, machine_and_run_id, '.'.join(map(str, lanes)), list(samples)[0])

