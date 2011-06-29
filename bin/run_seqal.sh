#!/bin/bash

# Copyright (C) 2011 CRS4.
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


# Run the seqal Map-Reduce alignment application.
# 
############################################################################
# Usage:  run_seqal.sh <input> <output> <reference tar>  [ trim quality ]
############################################################################
#
#
# Input should be the output directory of the PairReadsQSeq application.
#
# All paths are HDFS paths, and may be relative to your HDFS home
# (/user/<your username>); trim quality is equivalent to the -q
# parameter of the bwa aln and bwa sampe applications.  It will be set
# to 0 by default.
#
# hadoop is expected to be either in $HADOOP_HOME/bin or in the PATH;
# if you use a non-standard Hadoop configuration directory, set
# HADOOP_CONF_DIR accordingly.
#
# At the moment, this script isn't very flexible since it doesn't allow you to
# customize any of the application's properties.  Edit this file if you need
# to customize things (see towards the end of the file).  However, the default
# settings may work for you.

# By default run_seqal appends the Seal installation containing bin/run_seqal.sh.
# Therefore, it appends the Seal directory to the PYTHONPATH.  If you want to 
# override this behaviour, export a variable called SeqalPath and make it point
# to the desired path.
#
# E.g.
#    $ tar xzf seal.tar.gz
#    $ mv seal/bl /home/hadoop
#    $ SeqalPath=/home/hadoop ./seal/bin/run_seqal.sh ...
#

SealDir="$(dirname $(readlink -f "$0") )/../"
SealDir="$(readlink -f "${SealDir}")"

export PYTHONPATH="${PYTHONPATH:-}:${SealDir}"
Jar="$(python -c "import bl.lib.tools.hadut; print bl.lib.tools.hadut.find_seal_jar('${SealDir}')" )"

SeqalPath="${SeqalPath:-${PYTHONPATH}}"
echo "Using installation at ${SeqalPath}"

set -o errexit
set -o nounset

function usage_error {
	echo "Usage:  run_seqal.sh [ --reducers=xx ] <hdfs prq directory> <hdfs output aln dir> <reference tar>  [ trim quality ]" >&2
	exit 1
}

function error_msg() {
	echo $* >&2
	exit 1
}

## parse command line arguments
declare -a leftover_args

for arg in $*; do
	case ${arg} in
		-r=*|--reducers=*)
			num_reducers=$(echo $arg | sed -e 's/.*=//')
			if [ ${num_reducers} -lt 0 ]; then
				error_msg "Invalid number of reducers (must be >= 0)"
			fi
			;;
		*)
			leftover_args[${#leftover_args[@]}]="${arg}" # append to leftover_args
			;;
	esac
done

if [ ${#leftover_args[@]} -ne 4 -a ${#leftover_args[@]} -ne 3 ]; then
	echo "Wrong number of arguments (${#leftover_args[@]})" >&2
	usage_error
fi

## parameters
Input=${leftover_args[0]}
Output=${leftover_args[1]}
Reference=${leftover_args[2]}
Qvalue=${leftover_args[3]:-0}

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

CleanUpPaths=""

function cleanup() {
	rm -f ${CleanUpPaths}
	if [ -n "${HdfsCleanup:-}" ]; then
		${Hadoop} dfs -rmr "${HdfsCleanup}"
	fi
}
trap "cleanup" EXIT TERM INT

function clean_this_too() {
	CleanUpPaths="${CleanUpPaths} $* "
}

PythonPath=$(which python)

# check preconditions:

### Python and pydoop
if ! ${PythonPath} -c "import pydoop.pipes" >/dev/null 2>&1 ; then
	echo "Your python installation (${PythonPath}) does not have Pydoop." >&2
	echo "Please install Pydoop and try again" >&2
	exit 1
fi

# valid quality parameter
if [ ${Qvalue} -lt 0 ]; then
	echo "Qvalue must be >= 0 (was ${Qvalue})" >&2
	exit 1
fi

# requested reference archive on HDFS
if ! ${Hadoop} dfs -stat "${Reference}" > /dev/null; then
	echo "Cannot read reference archive ${Reference}" >&2
	exit 1
fi

# ensure the output directory doesn't already exist
if ${Hadoop} dfs -stat "${Output}" > /dev/null 2>&1; then
	echo "Output directory ${Output} already exists" >&2
	exit 1
fi

# number of reducers.  
if [ -z ${num_reducers:-""} ]; then
	# number of reducers hasn't been specified.
	# Use a fixed number of reducers per active tracker.
	ntrackers=$(${Hadoop} job -list-active-trackers 2>/dev/null | wc -l)
	if [ ${ntrackers} -le 0 ]; then
		error_msg "unable to set number of reducers"
	else
		num_reducers=$((6*ntrackers))
		echo "automatically using ${num_reducers} reducers"
	fi
fi

if [ ${num_reducers} -eq 0 ]; then
	echo "reducers == 0.  Running BWA only (no rmdup)"
fi

RemoteBin="$(mktemp -u seqal_bin.XXXXXXXXXX)"

# We create a driver script from a template, then we copy it 
# onto the HDFS volume.  Then we'll execute it with hadoop pipes.
# It relies on the SeqalPath being accessible from all worker nodes,
# since each map/reduce program will load the seqal modules locally.

TempScript="$(mktemp)"
clean_this_too "${TempScript}"

cat > "${TempScript}" <<END
#!/bin/bash

""":"
export LD_LIBRARY_PATH="${SeqalPath}:${LD_LIBRARY_PATH:-}" # LD_LIBRARY_PATH copied from the env where you ran $0
export PYTHONPATH="${SeqalPath}"
exec ${PythonPath} -u "\$0" "\$@"
":"""

from bl.mr.seq.seqal import run_task
run_task()
END

${Hadoop} dfs -put "${TempScript}" "${RemoteBin}"
HdfsCleanup="${RemoteBin}" # the trap function will delete this


Opts="\
-D mapred.job.name='seqal_aln_${Output}' \
-D hadoop.pipes.executable=${RemoteBin} \
-D hadoop.pipes.java.recordreader=true \
-D hadoop.pipes.java.recordwriter=true \
-D mapred.cache.archives=${Reference}#reference \
-D mapred.create.symlink=yes \
-D bl.libhdfs.opts=-Xmx48m \
-D bl.seqal.fastq-subformat=fastq-illumina \
-D bl.seqal.max.isize=1000 \
-D bl.seqal.log.level=DEBUG \
-D bl.seqal.pairing.batch.size=10000 \
-D bl.seqal.discard_duplicates=false \
-D bl.seqal.trim.qual=${Qvalue} \
-D mapred.compress.map.output=false \
-D mapred.reduce.tasks=${num_reducers} \
"

${Hadoop} pipes ${Opts} -input "${Input}" -output "${Output}"
