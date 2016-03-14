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

import seal.lib.io.avro_to_sam as avro_to_sam

from collections import defaultdict
import json

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.avrolib import AvroContext

class Mapper(api.Mapper):

    def __init__(self, ctx):
        super(Mapper, self).__init__(ctx)
        self._ctx = ctx

    def make_read_id(self, record):
        parts = [
            record.get(k) or '' for k in (
                'instrument',
                # need run number
                'flowcellId',
                'lane',
                'tile',
                'xPosition',
                'yPosition')
                ]
        read_name = ':'.join(parts)
        return read_name

    def format_sam(self, fragment, aln, mate_aln=None):
        sam_record = []
        sam_record.append(fragment['readName'] or self.make_read_id(fragment))
        sam_record.append(avro_to_sam.compute_sam_flag(aln, mate_aln))
        sam_record.append(aln['contig'].get('contigName', '*') if aln['contig'] else '*')
        read_pos = aln['start'] + 1 if aln['start'] is not None else 0
        sam_record.append(read_pos)
        sam_record.append(avro_to_sam.value_or(aln['mapq'], 0))
        sam_record.append(avro_to_sam.value_or(aln['cigar'], '*'))
        fragment_sign = 1
        if mate_aln:
            if mate_aln['contig']:
                mate_contig_name = mate_aln['contig'].get('contigName', '*')
                aln_contig_name = aln['contig'].get('contigName', '*') if aln['contig'] else '*'
                if aln_contig_name == mate_contig_name and mate_contig_name != '*':
                    sam_record.append('=')
                else:
                    sam_record.append(mate_contig_name)
            mate_pos = mate_aln['start'] + 1 if mate_aln['start'] is not None else 0
            sam_record.append(mate_pos)
            if mate_pos < read_pos:
                fragment_sign = -1
        else:
            sam_record.append('*')
            sam_record.append(0)

        sam_record.append(fragment_sign * avro_to_sam.value_or(fragment['inferredInsertSize'], 0))
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

        self._ctx.emit('', '\t'.join(map(str, sam_record)))

    def map(self, ctx):
        fragment = ctx.value
        # we assume that alignments for read and mate are specified in the same
        # order, so if we have two alignments '1' and '2':
        # [ read1, mate1, read2, mate2 ]
        #   or
        # [ read1, read2, mate1, mate2 ]
        #   but never [ read1, mate2, read2, mate1 ].
        #
        # In other words, when we see an alignment we assume that the next one that
        # is not for the same read is it's mate.

        # collect alignments by read number, while maintaining the sequence
        alignments = defaultdict(list)
        for aln in fragment['alignments']:
            alignments[aln['readNum']].append(aln)

        n_reads = len(alignments)
        if n_reads > 2:
            raise NotImplementedError("Can't handle fragments with more than two reads")
        elif n_reads == 0:
            raise RuntimeError("No alignments found!")

        # zip alignments so that we go from
        #  { 1: [ aln1, aln2, aln3] , 2: [aln1, aln2, aln3]}
        # to
        #  [ (aln1, aln1), (aln2, aln2), (aln3, aln3) ]
        # For each tuple we need to emit two SAM records
        for tpl in zip(*[ alignments[r_num] for r_num in sorted(alignments.iterkeys()) ]):
            if len(tpl) == 1:
                self.format_sam(fragment, tpl[0])
            else:
                for idx in xrange(len(tpl) - 1):
                    self.format_sam(fragment, tpl[idx], tpl[idx+1])
                self.format_sam(fragment, tpl[-1], tpl[0])

def __main__():
    factory = pp.Factory(Mapper)
    pp.run_task(factory, context_class=AvroContext)
