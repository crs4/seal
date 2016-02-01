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


import argparse
import os
import shutil
import subprocess
import sys

import logging
logging.basicConfig(level=logging.INFO)

import seal
import pydoop.hdfs as hdfs

# This module uses code that is not available on all supported Python
# versions (namely, subprocess)
import seal.backports

import site
site.addsitedir(
        os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..'))) # tests directory

class SealTestException(Exception):
    pass

class OutputValidator(object):
    def __init__(self, work_dir, logger=None):
        self._work_dir = work_dir
        self._logger = logger or logging.getLogger('OutputComparison')

    def validate_output(self, expected_data_file, output_dir_path):
        raise NotImplementedError()
        # raises SealTestException if output isn't correct

class DiffOutputValidator(OutputValidator):

    # Compares a file containing the expected test output to
    # the test output, sorted with a plain call to "sort".
    # Shows the diff if there are any.
    #
    # @param expected_file:  file containing expected output
    # @param hadoop_output_dir:  directory where the hadoop part-r-xxxx files are.  If
    #         not specified, this is assumed to be self.output_dir
    # @throws SealTestException if they are different
    def validate_output(self, expected_data_file, output_dir_path):
        sorted_output = "%s/sorted_output" % self._work_dir

        retcode = os.system( """cat "%s"/part-* | LC_ALL=C sort > "%s" """ % (output_dir_path, sorted_output) )
        if retcode != 0:
            raise RuntimeError("%d return code when running cat|sort" % retcode)
        self._logger.debug("data downloaded and sorted in %s\nData:", sorted_output)
        if self._logger.isEnabledFor(logging.DEBUG):
            os.system("cat %s" % sorted_output)

        cmd = ["diff", expected_data_file, sorted_output]
        self._logger.info(cmd)
        try:
            subprocess.check_call(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            self._logger.debug("Found differences.  diff returned non-zero")
            self._logger.debug("output:\n%s", e.output)
            self._logger.debug("raising a SealTestException")
            # diff returns non-zero when the inputs differ
            raise SealTestException("Output from test is not as expected\n%s" % e.output)

class SealIntegrationTest(object):

    def __init__(self, test_dir):
        self.test_dir = test_dir
        self.test_name = os.path.basename(test_dir)
        self.jar = seal.jar_path()
        self.output_dir = "/tmp/%s.%d" % (self.test_name, os.getpid())
        self.seal_dir = os.path.abspath( os.path.join(os.path.dirname(__file__), '..', '..') )
        self.logger = logging.getLogger(self.test_name)

        self.parser = argparse.ArgumentParser(description="Run this integration test")
        self.parser.add_argument('--debug', action='store_true',
                help="Turn on debug mode")
        self.parser.add_argument('--no-cleanup', action='store_true',
                help="Don't delete temporary files (for debugging)")
        self.options = None
        self.output_validator_class = DiffOutputValidator

    def setup(self):
        """
        * Creates an hdfs directory with the name of this test (self.make_hdfs_test_path())
        * uploads the local 'input' directory into the hdfs directory
        """
        self.logger.debug("Test setup")
        #hadut.run_hadoop_cmd_e("dfsadmin", args_list=["-safemode", "wait"])
        #self.logger.debug("hdfs out of safe mode")

        if hdfs.path.exists(self.make_hdfs_test_path()):
            error_msg = "hdfs test path '%s' already exists.  Please remove it" % self.make_hdfs_test_path()
            self.logger.fatal(error_msg)
            raise RuntimeError(error_msg)
        hdfs.mkdir(self.make_hdfs_test_path())
        local_input = self.make_local_input_path()
        hdfs_input = self.make_hdfs_input_path()
        hdfs.put(local_input, hdfs_input)
        self.logger.info("Copied local input %s to %s", local_input, hdfs_input)
        self.logger.debug("Setup complete")

    def test_method(self):
        """
        "main" method
        """
        self.options = self.parser.parse_args()
        if self.options.debug:
            self.logger.setLevel(logging.DEBUG)

        self.logger.info( ('-'*20 + " %s " + '-'*20), self.test_name)
        self.setup()

        self.logger.debug("setup complete")
        success = False
        try:
            self.logger.info("running %s program", self.test_name)
            hdfs_input = self.make_hdfs_input_path()
            hdfs_output = self.make_hdfs_output_path()
            self.logger.debug("hdfs input path: %s", hdfs_input)
            self.logger.debug("hdfs output path: %s", hdfs_output)
            self.run_program(hdfs_input, hdfs_output)

            self.logger.info("now going to process output")
            self.logger.debug("hdfs.get(%s, %s)", self.make_hdfs_output_path(), self.output_dir)
            hdfs.get(self.make_hdfs_output_path(), self.output_dir)
            self.process_output()
            success = True
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
        if self.options.no_cleanup:
            self.logger.warn("User specified --no-cleanup.  Not deleting temporary files")
            self.logger.warn("output dir: %s", self.output_dir)
            self.logger.warn("hdfs input path: %s", self.make_hdfs_input_path())
            self.logger.warn("hdfs output path: %s", self.make_hdfs_output_path())
            self.logger.warn("hdfs test path: %s", self.make_hdfs_test_path())
            return

        self.rm_output_dir()
        try:
            hdfs.rmr(self.make_hdfs_input_path())
        except Exception as e:
            self.logger.warning(e)

        try:
            hdfs.rmr(self.make_hdfs_output_path())
        except Exception as e:
            self.logger.warning(e)

        try:
            hdfs.rmr(self.make_hdfs_test_path())
        except Exception as e:
            self.logger.warning(e)

    def run_program(self, hdfs_input, hdfs_output):
        raise NotImplementedError()

    def verify_sorted_output(self, expected_file, hadoop_output_dir=None):
        if hadoop_output_dir is None:
            hadoop_output_dir = self.output_dir
        output_validator = self.output_validator_class(self.output_dir, self.logger)
        output_validator.validate_output(expected_file, hadoop_output_dir)
        self.logger.debug("Verify returning without problems.")

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
    # Raises a SealTestException if the output is not as expected
    def process_output(self):
        self.logger.info("verifying that sorted output matches expected value")
        self.verify_sorted_output( self.make_local_expected_output_path(), self.output_dir )

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
        Run a command and return its output, if any.  The output will be
        logged if the program exits with an error.

        param args:  The arguments array to be passed to subprocess.check_output
        """
        try:
            return subprocess.check_output(args, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            self.logger.fatal("exit status: %d", e.returncode)
            self.logger.fatal("***** command output follows *****")
            self.logger.fatal(e.output) # print the command output only in case of an error
            self.logger.fatal("***** command output ends *****")
            raise
