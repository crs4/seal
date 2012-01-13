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

# Functions resulting from refactoring the test scripts.

# Call with $@
#
# Sets a number of global variables:
#   Jar: name of the Jar to execute (first cmd line parameter expected)
#   TestName:  name of the directory containing the run file.
#   HADOOP:  hadoop executable (works only if HADOOP_HOME is set.
#   WD:  working HDFS directory (same as the test name).
#   OutputDir:  temporary output directory where to store temp files.
#
function prep() {
	if [ $# -ne 1 ] || [ ! -f "${1}" ]; then
		echo "$0 <jar>" >&2
		exit 1
	fi

	Jar="${1}"
	# Test name defined as the name of the directory containing the run script.
	# We first get the absolute path:
	TestName="$(dirname `readlink -f $0`)"
	# Use it to get the Seal directory
	SealDir="$(readlink -f ${TestName}/../../../../)"
	# The trim the path and only leave the base name
	TestName=${TestName##*/}

	HADOOP=${HADOOP_HOME}/bin/hadoop

	WD=${TestName}
	OutputDir="/tmp/${WD}.$$"
}

# Compares a file containing the expected test output to
# the test output, sorted with a plaing call to "sort".
# Shows the diff if there are any.
# 
# Input:  file containing expected output
# Output:  sets the exit_code variable
function compare_sorted_output() {
	cat "${OutputDir}"/part-* | LC_ALL=C sort > ${OutputDir}/sorted_output
	exit_code=""
	if diff "${1}" ${OutputDir}/sorted_output ; then
		exit_code=0
	else
		exit_code=1
	fi
}

# Shows a message with the test result (successful/unsuccessful).
#
# Input:  
#   1: error code (0 => OK, other => unsuccessful)
#   2: Test name (defaults to $TestName
#
# Output: none
function show_test_msg() {
	local code=${1}
	local test_name="${2:-""}"

	if [ ${code} == 0 ]; then
		echo 'Test successful!'
	else
		echo "Error!  Unexpected result in test ${test_name}" >&2
	fi
}

# Self explanatory.  
function rm_output_dir() {
	if [ -d "${OutputDir}" ]; then
		rm -rf "${OutputDir}"
	else
		echo "rm_output_dir: Tried deleting output directory '${OutputDir}' but it doesn't exist" >&2
		exit 1
	fi
}

# A canned function for generic test output processing.
# If it doesn't fit your needs use the functions above to piece
# together the required functionality.
#
# Reads the output ${OutputDir}/part-*
# Compares it with the sorted text in the file "expected"
# exits with 1 or 0
function process_output() {
	compare_sorted_output "${TestDir}/expected"  # sets exit_code
  show_test_msg $exit_code "${TestName}"
	rm_output_dir

	exit $exit_code
}
