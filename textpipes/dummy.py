import logging
from .core.recipe import Rule, LoopRecipeFile
from .components.core import MonoPipeComponent, MonoPipe

logger = logging.getLogger('textpipes')

class DummyTrainLoop(Rule):
    def __init__(self, inp, model, loop_indices):
        outputs = LoopRecipeFile.loop_output(
            model[0], model[1], loop_indices)
        super().__init__([inp], outputs)

    def make(self, conf, cli_args):
        print('in DummyTrainLoop')
        in_fobj = self.inputs[0].open(conf, cli_args, mode='r')
        # for debug reasons, only outputs first two
        out_fobjs = [out.open(conf, cli_args, mode='w')
                     for out in self.outputs[:2]]
        for line in in_fobj:
            for (i, out_fobj) in enumerate(out_fobjs):
                out_fobj.write('{} {}\n'.format(i, line))
        in_fobj.close()
        for out_fobj in out_fobjs:
            out_fobj.close()

    def is_atomic(self, output):
        # all loop outputs are atomic
        return isinstance(output, LoopRecipeFile)

    def monitor(self, platform, conf, cli_args=None):
        highest = LoopRecipeFile.highest_written(
            self.outputs, conf, cli_args)
        if highest is None:
            'no output'
        return highest(conf, cli_args)


class DummyParamPrintComponent(MonoPipeComponent):
    def __init__(self, name='Dummy', params=[], extra=[], **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.params = params
        self.extra = extra

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for line in stream:
            yield line
        yield '{}: {}'.format(self.__class__.__name__, self.name)
        for param in self.params:
            sec, key = param.split(':')
            yield '{} param {}: {}'.format(
                self.name, param, config[sec][key])
        for ext in self.extra:
            yield '{} extra: {}'.format(self.name, ext)


class DummyParamPrint(MonoPipe):
    def __init__(self, inp, out, **kwargs):
        super().__init__(
            [DummyParamPrintComponent(**kwargs)],
            [inp], [out])


class Manual(Rule):
    """A hack for including manual processing steps.
    Will throw exception if actually run."""
    def __init__(self, inputs, outputs, message='Manual'):
        super().__init__(inputs, outputs)
        self.message = message
        self.blocks_recursion = True

    def make(self, conf, cli_args):
        raise Exception('Should be run manually: {}'.format(self.message))
