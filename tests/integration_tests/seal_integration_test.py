#!/usr/bin/env python

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

#########################################################
# You need a working Hadoop cluster to use this.
#########################################################


import traceback
import subprocess
import os
import sys
import shutil

import logging
logging.basicConfig(level=logging.INFO)

import seal
import seal.lib.hadut as hadut

class SealIntegrationTest(object):

	def __init__(self, test_dir):
		self.test_dir = test_dir
		self.test_name = os.path.basename(test_dir)
		self.jar = seal.jar_path()
		self.output_dir = "/tmp/%s.%d" % (self.test_name, os.getpid())
		self.seal_dir = os.path.abspath( os.path.join(os.path.dirname(__file__), '..', '..') )
		self.logger = logging.getLogger(self.test_name)

	def setup(self):
		"""
		* Creates an hdfs directory with the name of this test (self.make_hdfs_test_path())
		* uploads the local 'input' directory into the hdfs directory
		"""
		hadut.run_hadoop_cmd_e("dfsadmin", args_list=["-safemode", "wait"])
		self.logger.debug("hdfs out of safe mode")

		if hadut.hdfs_path_exists(self.make_hdfs_test_path()):
			error_msg = "hdfs test path '%s' already exists.  Please remove it" % self.make_hdfs_test_path()
			self.logger.fatal(error_msg)
			raise RuntimeError(error_msg)

		hadut.dfs("-mkdir", self.make_hdfs_test_path())
		input_dir = self.make_hdfs_input_path()
		hadut.dfs("-put", self.make_local_input_path(), input_dir)

	def test_method(self):
		self.logger.info( ('-'*20 + " %s " + '-'*20), self.test_name)
		self.setup()

		success = False
		try:
			self.logger.info("running %s program", self.test_name)
			self.run_program(self.make_hdfs_input_path(), self.make_hdfs_output_path())

			self.logger.info("now going to process output")
			self.logger.debug("""hadut.dfs("-get", %s, %s)""" % (self.make_hdfs_output_path(), self.output_dir))
			hadut.dfs("-get", self.make_hdfs_output_path(), self.output_dir)
			success = self.process_output()
		except Exception as e:
			self.logger.error("*"*72)
			self.logger.error("Test %s raised an exception" % self.test_name)
			self.logger.error(e)
			self.logger.error("*"*72)
		finally:
			self.logger.info("cleaning up")
			self.clean_up()
			self.logger.info( '-'*(42 + len(self.test_name)) ) # close the test section with a horizontal line
			self.show_test_msg(success)

		return success

	def clean_up(self):
		try:
			hadut.dfs("-rmr", self.make_hdfs_input_path())
		except Exception as e:
			self.logger.warning(e)
			pass

		try:
			hadut.dfs("-rmr", self.make_hdfs_output_path())
		except Exception as e:
			self.logger.warning(e)
			pass

		try:
			hadut.dfs("-rmr", self.make_hdfs_test_path())
		except Exception as e:
			self.logger.warning(e)
			pass

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
		self.logger.info("""diff "%s" "%s" """, expected_file, sorted_output)
		retcode = os.system("""diff "%s" "%s" """ % (expected_file, sorted_output))
		return retcode != 0

	# print a message with the test result (successful/unsuccessful).
	def show_test_msg(self, successful):
		if successful:
			self.logger.info("success!")
		else:
			self.logger.error("FAILED!  Unexpected result in test")

	# Self explanatory.
	def rm_output_dir(self):
		try:
			shutil.rmtree(self.output_dir)
		except OSError as e:
			self.logger.error("Tried deleting output directory '%s' but got an error (%s)", self.output_dir, e)

	# A canned function for generic test output processing.
	# If it doesn't fit your needs use the functions above to piece
	# together the required functionality.
	#
	# Reads the output ${OutputDir}/part-*
	# Compares it with the sorted text in the file "expected"
	# returns True if the output is as expected, else False
	def process_output(self):
		different = self.sorted_output_different( self.make_local_expected_output_path(), self.output_dir )
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
		self.logger.info(*map(str, args))

	def run_cmd_and_output_if_failure(self, args):
		"""
		Run a command that may produce output.  The output will only be
		logged if the program exits with an error.

		param args:  The arguments array to be passed to subprocess.check_output
		"""
		try:
			subprocess.check_output(args, stderr=subprocess.STDOUT)
		except subprocess.CalledProcessError as e:
			self.logger.fatal("exit status: %d", e.returncode)
			self.logger.fatal("***** command output follows *****")
			self.logger.fatal(e.output) # print the command output only in case of an error
			self.logger.fatal("***** command output ends *****")
			raise
