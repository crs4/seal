#!/bin/bash

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

###############################################################################
# You need a working Hadoop cluster to run this.
# export PYTHONPATH to point to the seal build directory.  For instance, from
# the integration_tests directory you could do something like
#   export PYTHONPATH=`pwd`/../../build/lib.*
#   ./run_all.sh
###############################################################################

cd $(dirname $0)

TestsPath="${1:-.}"

for run_script in $(find "${TestsPath}" -name run -print); do
	echo "===================================="
	echo "   running ${run_script}"
	echo "===================================="
	${run_script}
	test_code=$?
	echo "                ===                 "
	if [ $test_code == 0 ]; then
		echo "Test ${run_script} successful"
	else
		echo "ERROR Test ${run_script} returned non-zero" >&2
		some_failed="true"
	fi
done

if [ "${some_failed:-false}" == "true" ]; then
	printf "**********************************************\n"
	printf "##### Some of the integration tests FAILED. #####\n"
	printf "If your installation seems to be set up correctly please inform the Seal developers\n"
	printf "via the appropriate tracker at\n"
	printf "  https://sourceforge.net/tracker/?group_id=536922\n"
	printf "**********************************************\n"
	exit 1
fi
