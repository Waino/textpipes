"""Pipes are text processing operations expressed as Python generators,
which can be composed into Rules"""

from .components import *
from .recipe import Rule
from .utils import safe_zip

class Pipe(Rule):
    def __init__(self, components,
                 main_inputs, main_outputs):
        side_inputs = tuple(set(inp for component in components
                                for inp in component.side_inputs
                                if inp is not None))
        side_outputs = tuple(set(out for component in components
                                 for out in component.side_outputs
                                 if out is not None))
        inputs = tuple(main_inputs) + tuple(side_inputs)
        outputs = tuple(main_outputs) + tuple(side_outputs)
        super().__init__(inputs, outputs)
        self.components = components
        self.main_inputs = main_inputs
        self.main_outputs = main_outputs
        self.side_inputs = side_inputs
        self.side_outputs = side_outputs

    def _make_helper(self, stream):
        # Open side inputs and outputs
        side_fobjs = {}
        for inp in self.side_inputs:
            side_fobjs[inp] = inp.open(conf, cli_args, mode='rb')
        for out in self.side_outputs:
            assert out not in side_fobjs
            side_fobjs[out] = out.open(conf, cli_args, mode='wb')

        for component in self.components:
            component.pre_make(side_fobjs)
        for component in self.components:
            stream = component(stream, side_fobjs=side_fobjs)
        for component in self.components:
            component.post_make(side_fobjs)

        return stream, side_fobjs
        

class MonoPipe(Pipe):
    def __init__(self, components, *args, **kwargs):
        for component in components:
            if not isinstance(component, MonoPipeComponent):
                raise Exception('MonoPipe expected MonoPipeComponent, '
                    'received {}'.format(component))
        super().__init__(components, *args, **kwargs)

    def make(self, conf, cli_args=None):
        if len(self.main_inputs) != 1:
            raise Exception('MonoPipe must have exactly 1 main input. '
                'Received: {}'.format(self.main_inputs))
        if len(self.main_outputs) != 1:
            raise Exception('MonoPipe must have exactly 1 main output. '
                'Received: {}'.format(self.main_outputs))
        # Make a generator that reads from main_input
        main_in_fobj = self.main_inputs[0].open(conf, cli_args, mode='rb')
        stream = main_in_fobj

        stream, side_fobjs = self._make_helper(stream)

        # Drain pipeline into main_output
        with self.main_outputs[0].open(conf, cli_args, mode='wb') as fobj:
            for line in stream:
                fobj.write(line)
                fobj.write('\n')
        # close all file objects
        main_in_fobj.close()
        for fobj in side_fobjs.values():
            fobj.close()


class ParallelPipe(Pipe):
    def __init__(self, components, *args, **kwargs):
        wrapped = []
        for component in components:
            if isinstance(component, SingleCellComponent):
                # SingleCellComponent automatically wrapped in ForEach,
                # which applies it to all 
                component = ForEach(component)
            if not isinstance(component, ParallelPipeComponent):
                raise Exception('ParallelPipe expected ParallelPipeComponent, '
                    'received {}'.format(component))
            wrapped.append(component)
        super().__init__(wrapped, *args, **kwargs)

    def make(self, conf, cli_args=None):
        # Make a tuple of generators that reads from main_inputs
        readers = [inp.open(conf, cli_args, mode='rb')
                   for inp in self.main_inputs]
        # read one line from each and yield it as a tuple
        stream = safe_zip(*readers)

        stream, side_fobjs = self._make_helper(stream)

        # Round-robin drain pipeline into main_outputs
        writers = [out.open(conf, cli_args, mode='wb')
                   for out in self.main_outputs]
        for (i, tpl) in enumerate(stream):
            if len(tpl) != len(writers):
                raise Exception('line {}: Invalid number of columns '
                    'received {}, expecting {}'.format(
                        i, len(tpl), len(writers)))
            for (val, fobj) in zip(tpl, writers):
                fobj.write(val)
                fobj.write('\n')
        for fobj in readers + writers + list[side_fobjs.values()]:
            fobj.close()


class DeadEndPipe(MonoPipe):
    """Has no main output.

    Useful e.g. if you just want to train a model (such as a truecaser)
    from a corpus file, and don't need the lines for anything else.
    """
    def __init__(self, components, *args, **kwargs):
        for component in components:
            if not isinstance(component, MonoPipeComponent):
                raise Exception('DeadEndPipe expected MonoPipeComponent, '
                    'received {}'.format(component))
        super().__init__(components, *args, **kwargs)

    def make(self, conf, cli_args=None):
        if len(self.main_inputs) != 1:
            raise Exception('DeadEndPipe must have exactly 1 main input. '
                'Received: {}'.format(self.main_inputs))
        if len(self.main_outputs) != 0:
            raise Exception('DeadEndPipe cannot have a main output. '
                'Received: {}'.format(self.main_outputs))
        # Make a generator that reads from main_input
        main_in_fobj = self.main_inputs[0].open(conf, cli_args, mode='rb')
        stream = main_in_fobj

        stream, side_fobjs = self._make_helper(stream)

        # Drain pipeline, throwing the output away
        for line in stream:
            pass
        # close all file objects
        main_in_fobj.close()
        for fobj in side_fobjs.values():
            fobj.close()
