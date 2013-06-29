# Copyright (C) 2012 CRS4.
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

import argparse
import subprocess
import sys

import seal.lib.hadut as hadut

Header = "ReadGroup,QualityScore,Cycle,Dinuc,nObservations,nMismatches,Qempirical"
DefaultOutput = "/dev/stdout"

def main(args=None):
	parser = argparse.ArgumentParser(description='Fetch output produced by recab_table and format into a single csv table.')
	parser.add_argument('input_dir', metavar='INPUT_DIR', help="Input directory (recab_table output path")
	parser.add_argument('output', nargs='?', metavar="OUTPUT", default=DefaultOutput, help="Desired output file.  If not specified output will be written to stdout.")

	args = parser.parse_args(args)

	if not hadut.hdfs_path_exists(args.input_dir):
		parser.error("Can't find specified input HDFS path %s" % args.input_dir)

	output = None
	if args.output == DefaultOutput:
		output = sys.stdout
	else:
		try:
			output = open(args.output, 'w')
		except Exception as e:
			sys.stderr.write("Error opening output file %s\n" % args.output)
			sys.stderr.write("Message: %s\n" % e)
			parser.error()

	try:
		# write the header
		output.write(Header)
		output.write("\n")
		output.flush()
		retcode = subprocess.call([hadut.hadoop, "dfs", "-cat", args.input_dir + "/part-r-*"], stdout=output)
		if retcode != 0:
			sys.stderr.write("Error writing output file\n")
			sys.exit(retcode)
	finally:
		output.close()

	return 0
