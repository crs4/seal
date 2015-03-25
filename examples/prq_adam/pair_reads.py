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

import pydoop.mapreduce.api as api

# 0 - Machine name: unique identifier of the sequencer.
# 1 - Run number: unique number to identify the run on the sequencer.
# 2 - Lane number: positive integer (currently 1-8).
# 3 - Tile number: positive integer.
# 4 - X: x coordinate of the spot. Integer (can be negative).
# 5 - Y: y coordinate of the spot. Integer (can be negative).
# 6 - Index: positive integer. No indexing should have a value of 0.
# 7 - Read Number: 1 for single reads; 1 or 2 for paired ends.
# 8 - Sequence (BASES)
# 9 - Quality: the base quality string, in illumina ASCII+64 coding. (QUALITIES)
#10- Filter: Did the read pass filtering? 0 - No, 1 - Yes.

class Mapper(api.Mapper):
    def map(self, ctx):
        p = ctx.value.strip().split('\t')
        payload = {'read_number': p[7], 'bases': p[8], 'qualities': p[9]}
        if p[10] == '1': # else silently drop
            ctx.emit(tuple(p[:6]), payload)

def key_sort(x):
    return x['read_number']

class Reducer(api.Reducer):
    def reduce(self, ctx):
        vals = sorted(ctx.values, key=key_sort)
        if len(vals) != 2: # silently drop
            ctx.emit('FAILED', '')
            return
        machine, run, lane, tile, xpos, ypos = ctx.key
        sequences = []
        for v in vals:
            sequences.append({'bases': v['bases'], 'qualities': v['qualities']})
        payload = {'instrument': machine, 'runId': run,
                   'lane': int(lane), 'tile': int(tile),
                   'xPosition': int(xpos), 'yPosition': int(ypos),
                   'sequences': sequences, 'alignments': []}
        ctx.emit('', payload)
