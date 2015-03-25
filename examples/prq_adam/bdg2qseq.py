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
import pydoop.mapreduce.pipes as pp
from pydoop.avrolib import AvroContext

class Mapper(api.Mapper):
    def map(self, ctx):
        payload = ctx.value
        tpl = [
            payload['instrument'],
            payload['runId'],
            str(payload['lane']),
            str(payload['tile']),
            str(payload['xPosition']),
            str(payload['yPosition']),
            str(0),
            '', # idx 7
            '', # idx 8
            '', # idx 9
            '1']
        for idx, seq in enumerate(payload['sequences']):
            tpl[7] = str(idx + 1)
            tpl[8] = seq['bases'] or ''
            tpl[9] = seq['qualities'] or ''
            ctx.emit('', '\t'.join(tpl))

def __main__():
    factory = pp.Factory(Mapper)
    pp.run_task(factory, context_class=AvroContext)
