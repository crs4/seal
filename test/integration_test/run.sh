#!/bin/bash

# You need a working Hadoop cluster to run this.

BaseDir="$(dirname $0)/../../"
Jar="${BaseDir}/dist/PairReadsQSeq.jar"

cd $(dirname $0)

for test_dir in test_*; do
	echo "===================================="
	${test_dir}/run "${Jar}"
	echo "                ===                 "
	if [ $? == 0 ]; then
		echo "Test ${test_dir} successful"
	else
		echo "ERROR Test ${test_dir} returned non-zero"
	fi
done
