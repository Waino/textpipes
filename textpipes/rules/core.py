from ..recipe import Rule
from ..pipe import DeadEndPipe
from ..components import truecaser
from ..utils import progress

class SplitColumns(Rule):
    def __init__(self, inp, outputs, delimiter='\t', resource_class='short'):
        super().__init__([inp], outputs, resource_class=resource_class)
        self.delimiter = delimiter

    def make(self, conf, cli_args=None):
        stream = self.inputs[0].open(conf, cli_args, mode='rb')
        writers = [out.open(conf, cli_args, mode='wb')
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

class TrainTrueCaserRule(DeadEndPipe):
    """Convenience Rule allowing easy training of truecaser
    from multiple corpora files."""
    def __init__(self, inputs, model_file, sure_thresh=.6, **kwargs):
        component = truecaser.TrainTrueCaser(model_file, sure_thresh)
        super().__init__([component], inputs, **kwargs)
