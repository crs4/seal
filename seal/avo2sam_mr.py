# BEGIN_COPYRIGHT
#
# Copyright (C) 2009-2016 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

import seal.lib.io.avro_to_sam as avro_to_sam

import json

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.avrolib import AvroContext

class Mapper(api.Mapper):

    def __init__(self, ctx):
        super(Mapper, self).__init__(ctx)
        self._ctx = ctx

    @staticmethod
    def calc_insert_size(aln):
        isize = 0
        if aln['readMapped'] and aln['mateMapped'] \
                and aln['contig'] and aln['mateContig'] \
                and aln['contig']['contigName'] == aln['mateContig']['contigName']:
            # arithmetically this should be equivalent to bwa-mem's calculation
            p0 = aln['end'] if aln['readNegativeStrand'] else aln['start']
            p1 = aln['mateAlignmentEnd'] if aln['mateNegativeStrand'] else aln['mateAlignmentStart']
            if p0 < p1:
                z = 1
            elif p0 > p1:
                z = -1
            else:
                z = 0
            isize = p1 - p0 + z
        return isize

    def format_sam(self, aln):
        sam_record = []
        sam_record.append(aln['readName'])

        if aln.get('firstOfPair'):
            aln['readNum'] = 1
        elif aln.get('secondOfPair'):
            aln['readNum'] = 2
        else:
            aln['readNum'] = None

        mate_info = {
            'readMapped': aln.get('mateMapped'),
            'readNegativeStrand': aln.get('mateNegativeStrand')
            }
        sam_record.append(avro_to_sam.compute_sam_flag(aln, mate_info))
        aln_contig_name = aln['contig'].get('contigName', '*') if aln['contig'] else '*'
        sam_record.append(aln_contig_name)
        sam_record.append(aln['start'] + 1 if aln['start'] is not None else 0)
        sam_record.append(avro_to_sam.value_or(aln['mapq'], 0))
        sam_record.append(avro_to_sam.value_or(aln['cigar'], '*'))
        if aln['readPaired']:
            mate_contig_name = aln['mateContig'].get('contigName', '*') if aln['mateContig'] else '*'
            if aln['mateContig']:
                if aln_contig_name == mate_contig_name and mate_contig_name != '*':
                    sam_record.append('=')
                else:
                    sam_record.append(mate_contig_name)
            sam_record.append(aln['mateAlignmentStart'] + 1 if aln['mateAlignmentStart'] is not None else 0)
        else:
            sam_record.append('*')
            sam_record.append(0)

        sam_record.append(self.calc_insert_size(aln))
        if aln['primaryAlignment']:
            sam_record.append(avro_to_sam.value_or(aln['sequence'], '*'))
            sam_record.append(avro_to_sam.value_or(aln['qual'], '*'))
        else:
            sam_record.append('*')
            sam_record.append('*')

        if aln['mismatchingPositions']:
            sam_record.append("MD:Z:%s" % aln['mismatchingPositions'])
        if aln['attributes']:
            for label, value in json.loads(aln['attributes']).iteritems():
                sam_record.append(avro_to_sam.format_sam_attribute(label, value))

        return '\t'.join(map(str, sam_record))

    def map(self, ctx):
        aln = ctx.value
        sam = self.format_sam(aln)
        self._ctx.emit('', sam)

def __main__():
    factory = pp.Factory(Mapper)
    pp.run_task(factory, context_class=AvroContext)
