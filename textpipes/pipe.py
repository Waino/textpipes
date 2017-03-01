"""Pipes are text processing operations expressed as Python generators,
which can be composed into Steps"""

class Pipe(Step):
    def __init__(self, inputs, outputs, components):
        super(self).__init__(inputs, outputs)
        self.components = components
    
    def side_inputs(self):
        return [inp for compo in self.components
                for inp in compo.side_inputs()]

    def side_outputs(self):
        return [outp for compo in self.components
                for outp in compo.side_outputs()]

    def make(self, conf, cli_args=None):
        pass
        

class MonoPipe(Pipe):
    pass

class ParellelPipe(Pipe):
    pass
