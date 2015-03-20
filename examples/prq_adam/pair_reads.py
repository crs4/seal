#!/usr/bin/env python

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

import sys

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp

class Mapper(api.Mapper):
    def map(self, ctx):
        p = ctx.value.strip().split('\t')
        ctx.emit(tuple(p[:6]), p[7:-1])

def key_sort(x):
    return x[0]

class Reducer(api.Reducer):
    def reduce(self, ctx):
        vals = sorted(ctx.values, key=key_sort)
        z = sum([_[1:] for _ in vals], list())
        ctx.emit(ctx.key[0], '\t'.join(map(str, list(ctx.key[1:]) + z)))
        
def __main__():
    factory = pp.Factory(Mapper, Reducer)
    pp.run_task(factory, private_encoding=True)

