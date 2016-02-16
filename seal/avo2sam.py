#!/usr/bin/env python

import argparse
import os
import sys

import seal
import seal.avo2sam_mr
from pydoop.app.main import main as pydoop_main

def main(args):
    parser = argparse.ArgumentParser(description="Pydoop-based AVO to SAM converter.")
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'], default='INFO')
    parser.add_argument('input', help="Avro-parquet input data following AVO (Avocado) schema")
    parser.add_argument('output_dir', help="Path where the output sam files should be created")

    options, left_over = parser.parse_known_args(args)

    seal.config_logging()

    submit_args = [
        'submit',
        '--upload-file-to-cache', os.path.abspath(seal.avo2sam_mr.__file__),
        '--num-reducers', '0',
        '--input-format', 'org.apache.parquet.avro.AvroParquetInputFormat',
        '--output-format', 'it.crs4.pydoop.NoSeparatorTextOutputFormat',
        '--avro-input', 'v',
        '--libjars', seal.libjars(),
        '--log-level', options.log_level,
        '--job-name', 'avo2sam',
        '-Dmapred.child.java.opts=-Xmx1000m',
        '-Dmapreduce.map.memory.mb=1500',
        ]
    submit_args.extend(left_over)
    submit_args.extend( (
        'avo2sam_mr',
        options.input, options.output_dir
        ))
    pydoop_main(submit_args)

if __name__ == '__main__':
    main(sys.argv[1:])
