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

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.avrolib import AvroContext

class Mapper(api.Mapper):
    def map(self, ctx):
        p = ctx.value.strip().split('\t')
        if len(p) != 5:
            raise ValueError("found %d fields instead of 5!  Here's the record:\n%s" % (len(p), ctx.value))
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

def __main__():
    factory = pp.Factory(Mapper)
    pp.run_task(factory, context_class=AvroContext)
