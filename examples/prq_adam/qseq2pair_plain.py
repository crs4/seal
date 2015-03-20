from pair_reads import Mapper, Reducer
import pydoop.mapreduce.pipes as pp

def __main__():
    factory = pp.Factory(Mapper, Reducer)
    pp.run_task(factory)
