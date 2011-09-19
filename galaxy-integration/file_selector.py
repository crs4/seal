#!/usr/bin/env python

import glob
import os
import sys
import subprocess
import pydoop.hdfs

SealDir = "/ELS/els3/pireddu/seal/trunk"

Usage = """
file_selector.py glob <fs> <pattern> <output path>
file_selector.py upload <input path> <output path>
"""

def usage_error(msg = None):
	print >>sys.stderr, msg
	print >>sys.stderr, Usage
	sys.exit(1)

def error(msg = None):
	if msg:
		print >>sys.stderr, msg
	else:
		print >>sys.stderr, "unknown error"
	sys.exit(2)
	
def do_local_glob(pattern):
	paths = glob.glob(pattern)
	return "\n".join( map(lambda p: "file://" + os.path.abspath(p), paths) )

def filter_hdfs_path(path):
	b = os.path.basename(path)
	if b.startswith("_"):
		return False
	return True

def do_hdfs_glob(pattern):
	## XXX: Hack!

	# load hadut to find the Hadoop executable.  Use the globbing provided
	# by hadoop dfs -ls
	sys.path.append(SealDir)
	import bl.lib.tools.hadut as hadut

	# get the URI to the HDFS namenode
	fs = pydoop.hdfs.hdfs("default", 0)
	hdfs_path = "hdfs://%s:%d" % (fs.host, fs.port)
	fs.close()

	# This isn't such a smart function.  We do a -ls with the pattern, which causes hadoop dfs
	# to first expand the pattern, and then run -ls for each one.  Therefore, if the pattern matches
	# directories we get their contents rather than just the directory name.  While this isn't perfect,
	# doing a better job would require to either reimplement hdfs globbing here (perhaps with -lsr 
	# and fnmatch, like the python glob module) or implement a support command in Java that can leverage
	# the globbing provided by the Hadoop FileSystem class.
	proc = subprocess.Popen([hadut.hadoop, "dfs", "-ls", pattern], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	stdout, stderr = proc.communicate()
	if proc.returncode == 0:
		output_lines = stdout.splitlines()
		# The first line is something like "Found 12 items"
		# The rest each contain a file in the last column
		files = [ hdfs_path + line.split()[-1] for line in output_lines[1:] ]
		files = filter(filter_hdfs_path, files)
		return "\n".join(files)
	else:
		if stderr.index("No such file or directory"):
			return ""
		else:
			raise RuntimeError(stderr)



######################################################
if __name__ == "__main__":
	# usage:
	# file_selector.py glob <fs> <pattern> <output path>
	# file_selector.py upload <input path> <output path>
	if len(sys.argv) not in (4,5):
		usage_error("Wrong number of arguments")

	mode = sys.argv[1]

	if mode == "glob":
		if len(sys.argv) != 5:
			usage_error("Wrong number of arguments for 'glob'")
		fs, pattern, output_path = sys.argv[2:5]
		if fs == "local":
			output = do_local_glob(pattern)
		elif fs == "hdfs":
			output = do_hdfs_glob(pattern)
		else:
			usage_error("unrecognized fs value %s.  FS must be either 'local' or 'hdfs'" % fs)
	elif mode == "upload":
		if len(sys.argv) != 4:
			usage_error("Wrong number of arguments for 'upload'")
		uri_list_path, output_path = sys.argv[2:4]
		with open(uri_list_path) as f:
			output = f.read()
	else:
		usage_error("unrecognized mode value %s.  Mode must be either 'glob' or 'upload'" % mode)

	if not output:
		print >>sys.stderr, "No files selected"
		sys.exit(1)

	with open(output_path, 'w') as f:
		f.write(output)
	sys.exit(0)
