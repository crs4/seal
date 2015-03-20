import sys

import pydoop.mapreduce.api as api

class Mapper(api.Mapper):
    def map(self, ctx):
        payload = ctx.value
        ctx.emit(tuple(payload[k] for k in ['instrument', 'runId', 'lane', 'tile']),
                payload)

class Reducer(api.Reducer):
    def reduce(self, ctx):
        for v in ctx.values:
            ctx.emit('', v)
