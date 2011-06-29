#!/bin/bash

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


# Run the PairReadsQSeq (PRQ) Map-Reduce application.
# 
############################################################################
# Usage: run_prq.sh [ --reducers=xx ] <hdfs input directory> <hdfs output prq dir> [ min bases per read (default: DefaultMinBasesThreshold) ]
############################################################################

SealDir="$(dirname $(readlink -f "$0") )/../"
SealDir="$(readlink -f "${SealDir}")"
Jar="$(PYTHONPATH="${PYTHONPATH:-${SealDir}}" python -c "import bl.lib.tools.hadut; print bl.lib.tools.hadut.find_seal_jar('${SealDir}') or ''" )"

DefaultMinBasesThreshold=30

# Input is a directory of QSeq files.
#
# All paths are HDFS paths, and may be relative to your HDFS home
# (/user/<your username>).
#
# hadoop is expected to be either in $HADOOP_HOME/bin or in the PATH;
# if you use a non-standard Hadoop configuration directory, set
# HADOOP_CONF_DIR accordingly.
# 
# At the moment, this script isn't very flexible since it doesn't allow you
# to customize any of the application's properties.  Edit this file if you need
# to customize things (see towards the end of the file).  However, the default
# settings may work for you.

function error_msg() {
	echo $* >&2
	exit 1
}

################ main ################ 

# parse command line arguments

declare -a leftover_args

for arg in $*; do
	case ${arg} in
		-r=*|--reducers=*)
			num_reducers=$(echo $arg | sed -e 's/.*=//')
			if [ ${num_reducers} -le 0 ]; then
				error_msg "Invalid number of reducers (must be >= 1)"
			fi
			;;
		*)
			leftover_args[${#leftover_args[@]}]="${arg}" # append to leftover_args
			;;
	esac
done

if [ ${#leftover_args[@]} -ne 2 -a ${#leftover_args[@]} -ne 3 ]; then
	echo "Wrong number of arguments (${#leftover_args[@]})" >&2
	echo "Usage: $0 [ --reducers=xx ] <hdfs input directory> <hdfs output prq dir> [ min bases per read (default ${DefaultMinBasesThreshold}) ]" >&2
	exit 1
fi

Input=${leftover_args[0]}
Output=${leftover_args[1]}
MinBases=${leftover_args[2]:-${DefaultMinBasesThreshold}}

set -o errexit
set -o nounset

###########  check preconditions ##############
# check the jar
if [ -z "${Jar}" ]; then
	error_msg "Error:  can't find seal.jar"
elif [ ! -r "${Jar}" ]; then
	error_msg "Error:  can't read ${Jar}"
else
	echo "Using jar ${Jar}" >&2
fi


# Find hadoop
# First in HADOOP_HOME
if [ -n "${HADOOP_HOME:-""}" -a -x "${HADOOP_HOME:-""}/bin/hadoop" ]; then
	Hadoop="${HADOOP_HOME:-""}/bin/hadoop"
else
	# The look in PATH
	Hadoop="$(which hadoop || true)"
	if [ -z "${Hadoop}" ]; then
		echo "Can't find hadoop executable.  Please set HADOOP_HOME or add the Hadoop bin directory to you PATH" >&2
		exit 1
	fi
fi
echo "Using ${Hadoop}" >&2


if [ -z ${num_reducers:-""} ]; then
	# number of reducers hasn't been specified.  We'll try to use x per tracker.
	num_trackers=$(${Hadoop} job -list-active-trackers 2>/dev/null | wc -l)
	if [ ${num_trackers} -le 0 ]; then
		error_msg "unable to set number of reducers"
	else
		num_reducers=$((3*${num_trackers}))
		echo "automatically using ${num_reducers} reducers"
	fi
fi

# ensure input path exists
if ! ${Hadoop} dfs -stat "${Input}" > /dev/null 2>&1; then
	error_msg "Cannot read input path '${Input}'"
fi

# ensure the output directory doesn't already exist
if ${Hadoop} dfs -stat "${Output}" > /dev/null 2>&1; then
	error_msg "Output directory '${Output}' already exists"
fi

################ run the command ###################

MoreOpts="-D \
mapred.reduce.tasks=${num_reducers} \
-D mapred.compress.map.output=true \
-D bl.prq.min-bases-per-read=${MinBases} \
-D bl.prq.drop-failed-filter=true \
"

HADOOP_CLASSPATH="${Jar}" ${Hadoop} it.crs4.seal.prq.PairReadsQSeq ${MoreOpts} "${Input}" "${Output}"
