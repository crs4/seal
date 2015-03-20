from pack_fragments import Mapper, Reducer
import pydoop.mapreduce.pipes as pp

class Context(pp.TaskContext):
    def get_input_value(self):
        # FIXME do not do this at home!
        return eval(super(Context, self).get_input_value())
        

def __main__():
    factory = pp.Factory(Mapper, Reducer)
    pp.run_task(factory, context_class=Context)
