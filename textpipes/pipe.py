"""Pipes are text processing operations expressed as Python generators,
which can be composed into Rules"""

import codecs
import gzip
import bz2

from .components import *
from .recipe import Rule
from .utils import safe_zip

class Pipe(Rule):
    def __init__(self, components,
                 main_inputs, main_outputs,
                 side_inputs=None, side_outputs=None):
        side_inputs = side_inputs if side_inputs is not None else tuple()
        side_outputs = side_outputs if side_outputs is not None else tuple()
        inputs = tuple(main_inputs) + tuple(side_inputs)
        outputs = tuple(main_outputs) + tuple(side_outputs)
        super().__init__(inputs, outputs)
        self.components = components
        self.main_inputs = main_inputs
        self.main_outputs = main_outputs
        self.side_inputs = side_inputs
        self.side_outputs = side_outputs
        

class MonoPipe(Pipe):
    def __init__(self, components, *args, **args):
        for component in components:
            if not isinstance(component, MonoPipeComponent):
                raise Exception('MonoPipe expected MonoPipeComponent, '
                    'received {}'.format(component))
        super(components, *args, **args)

    def make(self, conf, cli_args=None):
        if len(self.main_inputs) != 1:
            raise Exception('MonoPipe must have exactly 1 main input. '
                'Received: {}'.format(self.main_inputs))
        if len(self.main_outputs) != 1:
            raise Exception('MonoPipe must have exactly 1 main output. '
                'Received: {}'.format(self.main_outputs))
        # Make a generator that reads from main_input
        stream = open_text_file_read(self.main_inputs[0](conf, cli_args))
        # strip newlines
        stream = (line.rstrip('\n') for line in stream)

        for component in self.components:
            stream = component(stream)

        # Drain pipeline into main_output
        with open_text_file_write(self.main_outputs[0](conf, cli_args)) as fobj:
            for line in stream:
                fobj.write(line)
                fobj.write('\n')

class ParallelPipe(Pipe):
    def __init__(self, components, *args, **args):
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
        super(wrapped, *args, **args)

    def make(self, conf, cli_args=None):
        # Make a tuple of generators that reads from main_inputs
        readers = [open_text_file_read(inp(conf, cli_args))
                   for inp in self.main_inputs]
        # read one line from each and yield it as a tuple
        stream = self._merge(readers)

        for component in self.components:
            stream = component(stream)

        # Round-robin drain pipeline into main_outputs
        writers = [open_text_file_write(out(conf, cli_args))
                   for out in self.main_outputs]
        for (i, tpl) in enumerate(stream):
            if len(tpl) != len(writers)
                raise Exception('{} line {}: Invalid number of columns '
                    'received {}, expecting {}'.format(
                        self.file_path, i,
                        len(tpl), len(writers))
            for (val, fobj) in zip(tpl, writers):
                fobj.write(val)
                fobj.write('\n')
        for fobj in writers:
            fobj.close()

    def _merge(self, readers):
        for tpl in safe_zip(*incoming_pipes):
            result = []
            for sub_tpl in tpl:
                result.extend(sub_tpl)
            tpl = tuple(result)
            yield tpl


# FIXME: perhaps these should be in RecipeFile?
def open_text_file_read(file_path, encoding='utf-8'):
    """Open a file for reading with the appropriate decompression/decoding
    """
    if file_path.endswith('.gz'):
        file_obj = gzip.open(file_path, 'rb')
    elif file_path.endswith('.bz2'):
        file_obj = bz2.BZ2File(file_path, 'rb')
    else:
        file_obj = open(file_path, 'rb')
    return codecs.getreader(encoding)(file_obj)

def open_text_file_write(file_path, encoding='utf-8'):
    """Open a file for writing with the appropriate compression/encoding"""
    if file_path.endswith('.gz'):
        file_obj = gzip.open(file_path, 'wb')
    elif file_path.endswith('.bz2'):
        file_obj = bz2.BZ2File(file_path, 'wb')
    else:
        file_obj = open(file_path, 'wb')
    return codecs.getwriter(encoding)(file_obj)

