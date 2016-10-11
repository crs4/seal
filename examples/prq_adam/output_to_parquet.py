#!/usr/bin/env python

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

import sys

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp

class Mapper(api.Mapper):
    def map(self, ctx):
        p = ctx.value.strip().split('\t')
        ctx.emit(p[:4], p[4:])

class Reducer(api.Reducer):
    def reduce(self, ctx):
        instrument, run_id, lane, tile = ctx.key
        datum = {'instrument' : instrument, 'runId' : run_id,
                 'lane' : lane, 'tile' : tile}
        for v in ctx.values:
            xpos, ypos, read1, qual1, read2, qual2 = v
            seq1 = {'bases': read1, 'qualities': qual1}
            seq2 = {'bases': read2, 'qualities': qual2}
            datum.update({'xPosition': xpos, 'yPosition': ypos,
                          'sequences': [seq1, seq2]})
            ctx.emit('', '%r' % datum)
            

def __main__():
    factory = pp.Factory(Mapper, Reducer)
    pp.run_task(factory, private_encoding=True)
