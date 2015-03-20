#!/bin/bash

./simulator pair_reads input_data.qseq output.paired
./simulator output_to_parquet output.paired output.parquet
