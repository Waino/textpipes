"""Pipes are text processing operations expressed as Python generators,
which can be composed into Rules"""

import codecs
import gzip
import bz2

from .recipe import Rule

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

class ParellelPipe(Pipe):
    # wrap any MonoPipeComponents in ForEach
    def make(self, conf, cli_args=None):
        # Make a tuple of generators that reads from main_inputs
        # iterate over components
        #   give pipeline and appropriate sides to component
        # Round-robin drain pipeline into main_outputs
        pass

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

