from .core.recipe import Rule
from .core.utils import progress, safe_zip


class SplitColumns(Rule):
    def __init__(self, inp, outputs, delimiter='\t', resource_class='short'):
        super().__init__([inp], outputs, resource_class=resource_class)
        self.delimiter = delimiter

    def make(self, conf, cli_args=None):
        stream = self.inputs[0].open(conf, cli_args, mode='r')
        writers = [out.open(conf, cli_args, mode='w')
                   for out in self.outputs]
        stream = progress(stream, self, conf, '(multi)')
        for (i, line) in enumerate(stream):
            tpl = line.split(self.delimiter)
            if not len(tpl) == len(writers):
                raise Exception('line {}: Invalid number of columns '
                    'received {}, expecting {}'.format(
                    i, len(tpl), len(writers)))
            for (val, fobj) in zip(tpl, writers):
                fobj.write(val)
                fobj.write('\n')
        for fobj in [stream] + writers:
            fobj.close()

class PasteColumns(Rule):
    def __init__(self, inputs, output, delimiter='\t', resource_class='short'):
        super().__init__(inputs, output, resource_class=resource_class)
        self.delimiter = delimiter

    def make(self, conf, cli_args=None):
        # Make a tuple of generators that reads from main_inputs
        readers = [inp.open(conf, cli_args, mode='r')
                   for inp in self.inputs]
        # read one line from each and yield it as a tuple
        stream = safe_zip(*readers)

        fobj = self.outputs[0].open(conf, cli_args, mode='w')
        stream = progress(stream, self, conf, '(multi)')
        for (i, tpl) in enumerate(stream):
            line = self.delimiter.join(tpl)
            fobj.write(line)
            fobj.write('\n')
        fobj.close()
