#!/bin/bash

# You need a working Hadoop cluster to run this.

BaseDir="$(dirname $0)/../../"
Jar="${BaseDir}/PairReadsQSeq.jar"

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
