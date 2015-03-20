#!/bin/bash

./simulator qseq2pair_plain input_data.qseq output.paired
./simulator pair2fragments_plain output.paired output.parquet
