#!/usr/bin/env python

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

#########################################################
# You need a working Hadoop cluster to use this.
#########################################################


import traceback
import subprocess
import os
import sys
import shutil
import site
SealDir = os.path.realpath( os.path.join( os.path.dirname( os.path.realpath(__file__) ), "..", "..") )
site.addsitedir(SealDir)

import bl.lib.tools.hadut as hadut

class SealIntegrationTest(object):

	def __init__(self, test_dir):
		#super(self.__class__, self).__init__(self) 
		self.test_dir = test_dir
		self.test_name = os.path.basename(test_dir)
		self.jar = hadut.find_seal_jar(SealDir)
		self.output_dir = "/tmp/%s.%d" % (self.test_name, os.getpid())

	def setup(self):
		#   Jar: the Seal jar
		#   wd:  working HDFS directory (same as the test name).
		#   OutputDir:  temporary output directory where to store temp files.
		hadut.run_hadoop_cmd_e("dfsadmin", args_list=["-safemode", "wait"])
		self.log("hdfs in safe mode")

		hadut.dfs("-mkdir", self.make_hdfs_test_path())
		input_dir = self.make_hdfs_input_path()
		hadut.dfs("-put", self.make_local_input_path(), input_dir)

	def test_method(self):
		self.log(self.__class__.__name__, "running test")
		self.setup()

		try:
			success = False
			self.log("running program")
			self.run_program(self.make_hdfs_input_path(), self.make_hdfs_output_path())
			self.log("now going to process input")
			self.log("""hadut.dfs("-get", %s, %s)""" % (self.make_hdfs_output_path(), self.output_dir))
			hadut.dfs("-get", self.make_hdfs_output_path(), self.output_dir)
			success = self.process_output()
		finally:
			self.log("cleaning up")
			self.clean_up()

		return success

	def clean_up(self):
		hadut.dfs("-rmr", self.make_hdfs_input_path())
		hadut.dfs("-rmr", self.make_hdfs_output_path())
		hadut.dfs("-rmr", self.make_hdfs_test_path())

	def run_program(hdfs_input, hdfs_output):
		raise NotImplementedError()

	# Compares a file containing the expected test output to
	# the test output, sorted with a plain call to "sort".
	# Shows the diff if there are any.
	# 
	# @param expected_file:  file containing expected output
	# @param hadoop_output_dir:  directory where the hadoop part-r-xxxx files are.  If
	#         not specified, this is assumed to be self.output_dir
	# @returns: False if they they are the same, True if they are different
	def sorted_output_different(self, expected_file, hadoop_output_dir=None):
		if hadoop_output_dir is None:
			hadoop_output_dir = self.output_dir
		sorted_output = "%s/sorted_output" % self.output_dir

		retcode = os.system( """cat "%s"/part-* | LC_ALL=C sort > "%s" """ % (hadoop_output_dir, sorted_output) )
		if retcode != 0:
			raise RuntimeError("%d return code when running cat|sort" % retcode)
		print """diff "%s" "%s" """ % (expected_file, sorted_output)
		retcode = os.system("""diff "%s" "%s" """ % (expected_file, sorted_output))
		return retcode != 0

	# print a message with the test result (successful/unsuccessful).
	def show_test_msg(self, successful):
		if successful:
			print "Test successful!"
		else:
			print >>sys.stderr, "Error!  Unexpected result in test %s" % self.test_name

	# Self explanatory.  
	def rm_output_dir(self):
		try:
			shutil.rmtree(self.output_dir)
		except OSError as e:
			print >>sys.stderr, "Tried deleting output directory '%s' but got an error (%s)" % (self.output_dir, e)

	# A canned function for generic test output processing.
	# If it doesn't fit your needs use the functions above to piece
	# together the required functionality.
	#
	# Reads the output ${OutputDir}/part-*
	# Compares it with the sorted text in the file "expected"
	# returns True if the output is as expected, else False
	def process_output(self):
		different = self.sorted_output_different( self.make_local_expected_output_path(), self.output_dir )
		self.show_test_msg( not different )
		self.rm_output_dir()
		return not different

	def get_test_dir(self):
		return self.test_dir

	def make_hdfs_test_path(self):
		return os.path.basename(self.test_dir)

	def make_hdfs_input_path(self):
		return os.path.join( self.make_hdfs_test_path(), "input")

	def make_local_input_path(self):
		return os.path.join(self.get_test_dir(), "input")

	def make_hdfs_output_path(self):
		return os.path.join( self.make_hdfs_test_path(), "output")

	def make_local_expected_output_path(self):
		return os.path.join(self.get_test_dir(), "expected")

	def log(self, *args):
		print >>sys.stderr, " ".join(args)
