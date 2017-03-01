"""Pipes are text processing operations expressed as Python generators,
which can be composed into Steps"""

class Pipe(Step):
    def __init__(self, components,
                 main_inputs, main_outputs,
                 side_inputs=None, side_outputs=None):
        side_inputs = side_inputs if side_inputs is not None else tuple()
        side_outputs = side_inputs if side_inputs is not None else tuple()
        inputs = tuple(main_inputs) + tuple(side_inputs)
        outputs = tuple(main_inputs) + tuple(side_inputs)
        super(self).__init__(inputs, outputs)
        self.components = components
        self.main_inputs = main_inputs
        self.main_outputs = main_outputs
        self.side_inputs = side_inputs
        self.side_outputs = side_outputs
        

class MonoPipe(Pipe):
    def make(self, conf, cli_args=None):
        # Make a generator that reads from main_input
        # iterate over components
        #   give pipeline and appropriate sides to component
        # Drain pipeline into main_output
        pass
    pass

class ParellelPipe(Pipe):
    def make(self, conf, cli_args=None):
        # Make a tuple of generators that reads from main_inputs
        # iterate over components
        #   give pipeline and appropriate sides to component
        # Round-robin drain pipeline into main_outputs
    pass
