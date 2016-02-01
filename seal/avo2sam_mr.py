# BEGIN_COPYRIGHT
#
# Copyright 2009-2015 CRS4.
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

import json

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.avrolib import AvroContext

class Mapper(api.Mapper):

    def __init__(self, ctx):
        super(Mapper, self).__init__(ctx)
        self._ctx = ctx

    @staticmethod
    def compute_sam_flag(aln):
        # SAM_FPD = 1     # paired
        # SAM_FPP = 2     # properly paired
        # SAM_FSU = 4     # self-unmapped
        # SAM_FMU = 8     # mate-unmapped
        # SAM_FSR = 16    # self on the reverse strand
        # SAM_FMR = 32    # mate on the reverse strand
        # SAM_FR1 = 64    # this is read one
        # SAM_FR2 = 128   # this is read two
        # SAM_FSC = 256   # secondary alignment
        # SAM_FQC = 0x200   # failed quality checks
        # SAM_FDP = 0x400   # PCR or optical duplicate
        flag = 0

        bit_value = 1
        for bit in (
            aln['readPaired'],
            aln['properPair'],
            not aln['readMapped'],
            not aln['mateMapped'],
            aln['readNegativeStrand'],
            aln['mateNegativeStrand'],
            aln['firstOfPair'],
            aln['secondOfPair'],
            aln['secondaryAlignment'],
            # missing vendor quality checks and duplicate marking
            ):
            if bit:
                flag |= bit_value
            bit_value *= 2

        return flag

    @staticmethod
    def _value_or(v, otherwise):
        return v if v is not None else otherwise

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
        sam_record.append(self.compute_sam_flag(aln))
        aln_contig_name = aln['contig'].get('contigName', '*') if aln['contig'] else '*'
        sam_record.append(aln_contig_name)
        sam_record.append(aln['start'] + 1 if aln['start'] is not None else 0)
        sam_record.append(self._value_or(aln['mapq'], 0))
        sam_record.append(self._value_or(aln['cigar'], '*'))
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
            sam_record.append(self._value_or(aln['sequence'], '*'))
            sam_record.append(self._value_or(aln['qual'], '*'))
        else:
            sam_record.append('*')
            sam_record.append('*')

        if aln['mismatchingPositions']:
            sam_record.append("MD:Z:%s" % aln['mismatchingPositions'])
        if aln['attributes']:
            for tpl in json.loads(aln['attributes']).iteritems():
                sam_record.append("%s:Z:%s" % tpl)

        return '\t'.join(map(str, sam_record))

    def map(self, ctx):
        aln = ctx.value
        sam = self.format_sam(aln)
        self._ctx.emit('', sam)

def __main__():
    factory = pp.Factory(Mapper)
    pp.run_task(factory, context_class=AvroContext)
