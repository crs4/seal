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

# You need a working Hadoop cluster to run this.

BaseDir=$(readlink -f "`dirname $0`/../../../")
Jar="${BaseDir}/build/seal.jar"

cd $(dirname $0)

for test_dir in test_*; do
	echo "===================================="
	echo "   running ${test_dir}"
	echo "===================================="
	${test_dir}/run "${Jar}"
	test_code=$?
	echo "                ===                 "
	if [ $test_code == 0 ]; then
		echo "Test ${test_dir} successful"
	else
		echo "ERROR Test ${test_dir} returned non-zero"
	fi
done
