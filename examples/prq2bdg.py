#!/usr/bin/env python


LOGLEVEL = 'DEBUG'

import os
import sys
from pydoop.app.main import main as pydoop_main
import seal


import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.avrolib import AvroContext

class Mapper(api.Mapper):
    def map(self, ctx):
        p = ctx.value.strip().split('\t')
        sequences = [
            {'bases': p[1], 'qualities': p[2]},
            {'bases': p[3], 'qualities': p[4]}
        ]
        payload = {
            'readName': p[0],
            'sequences': sequences,
            'alignments': []
        }
        ctx.emit('', payload)

def run_hadoop_prog():
    factory = pp.Factory(Mapper)
    pp.run_task(factory, context_class=AvroContext)

def main():
    if len(sys.argv) < 3:
        print >> sys.stderr, "USAGE:  %s [options] PRQ_IN_PATH PARQUET_OUT_PATH" % os.path.basename(__file__)
        sys.exit(1)

    input_path, output_path = sys.argv[-2:]
    options = sys.argv[1:-2] if len(sys.argv) > 3 else []

    with open(seal.avro_fragment_schema_filename()) as f:
        out_schema = f.read()

    submit_args = [
        'submit',
        '--upload-file-to-cache', os.path.abspath(__file__),
        '-D', 'pydoop.mapreduce.avro.value.output.schema=%s' % out_schema,
        '-D', 'parquet.avro.schema=%s' % out_schema,
        '--num-reducers', '0',
        '--output-format', 'parquet.avro.AvroParquetOutputFormat',
        '--avro-output', 'v',
        '--mrv2',
        '--libjars', seal.parquet_jar_path(),
        '--log-level', LOGLEVEL,
        '--job-name', 'prq2bdg',
        '--entry-point', 'run_hadoop_prog',
        ]
    submit_args.extend(options)
    submit_args.extend((
        os.path.splitext(os.path.basename(__file__))[0],
        input_path, output_path
        ))
    pydoop_main(submit_args)

if __name__ == '__main__':
    main()
