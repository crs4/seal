from pair_reads import Mapper, Reducer
import pydoop.mapreduce.pipes as pp
from pydoop.avrolib import AvroContext

def __main__():
    factory = pp.Factory(Mapper, Reducer)
    pp.run_task(factory, context_class=AvroContext)
