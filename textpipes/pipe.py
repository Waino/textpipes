"""Pipes are composite Rules built out of Components.
Components are text processing operations expressed as Python generators.
"""

import itertools

from .components.core import *
from .recipe import Rule
from .utils import safe_zip, progress

class Pipe(Rule):
    def __init__(self, components,
                 main_inputs, main_outputs,
                 estimated_lines='conf',
                 **kwargs):
        side_inputs = tuple(set(inp for component in components
                                for inp in component.side_inputs
                                if inp is not None))
        side_outputs = tuple(set(out for component in components
                                 for out in component.side_outputs
                                 if out is not None))
        inputs = tuple(main_inputs) + tuple(side_inputs)
        outputs = tuple(main_outputs) + tuple(side_outputs)
        super().__init__(inputs, outputs, **kwargs)
        self.components = components
        self.main_inputs = main_inputs
        self.main_outputs = main_outputs
        self.side_inputs = side_inputs
        self.side_outputs = side_outputs
        self.estimated_lines = estimated_lines

    def _make_helper(self, stream, conf, cli_args):
        # Open side inputs and outputs
        side_fobjs = {}
        for inp in self.side_inputs:
            side_fobjs[inp] = inp.open(conf, cli_args, mode='rb')
        for out in self.side_outputs:
            assert out not in side_fobjs
            side_fobjs[out] = out.open(conf, cli_args, mode='wb')

        for component in self.components:
            component.pre_make(side_fobjs)
        # Actually apply components to stream
        for component in self.components:
            stream = component(stream,
                               side_fobjs=side_fobjs,
                               config=conf,
                               cli_args=cli_args)

        # progress bar
        out = self.main_outputs[0] if len(self.main_outputs) > 0 \
            else self.side_outputs[0]
        stream = progress(stream, self, conf, 
                          out(conf, cli_args),
                          total=self.estimated_lines)

        return stream, side_fobjs

    def _post_make(self, side_fobjs):
        for component in self.components:
            component.post_make(side_fobjs)
        

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

        stream, side_fobjs = self._make_helper(stream, conf, cli_args)

        # Drain pipeline into main_output
        with self.main_outputs[0].open(conf, cli_args, mode='wb') as fobj:
            for line in stream:
                fobj.write(line)
                fobj.write('\n')

        # post_make must be done after draining
        self._post_make(side_fobjs)
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

        stream, side_fobjs = self._make_helper(stream, conf, cli_args)

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
        # post_make must be done after draining
        self._post_make(side_fobjs)
        # close all file objects
        for fobj in readers + writers + list(side_fobjs.values()):
            fobj.close()


class DeadEndPipe(MonoPipe):
    """Has (potentially) multiple inputs (read in sequence and concatenated),
    but no main output.

    Useful e.g. if you just want to train a model (such as a truecaser)
    from a number of corpus files, and don't need the lines for anything else.
    """
    def __init__(self, components, *args, **kwargs):
        for component in components:
            if not isinstance(component, MonoPipeComponent):
                raise Exception('DeadEndPipe expected MonoPipeComponent, '
                    'received {}'.format(component))
        super().__init__(components, *args, main_outputs=[], **kwargs)

    def make(self, conf, cli_args=None):
        if len(self.main_outputs) != 0:
            raise Exception('DeadEndPipe cannot have a main output. '
                'Received: {}'.format(self.main_outputs))
        # Make a tuple of generators that reads from main_inputs
        readers = [inp.open(conf, cli_args, mode='rb')
                   for inp in self.main_inputs]
        stream = itertools.chain(*readers)

        stream, side_fobjs = self._make_helper(stream, conf, cli_args)

        # Drain pipeline, throwing the output away
        for line in stream:
            pass
        # post_make must be done after draining
        self._post_make(side_fobjs)
        # close all file objects
        for fobj in readers + list(side_fobjs.values()):
            fobj.close()
