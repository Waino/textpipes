from .core.recipe import Rule, LoopRecipeFile

class DummyTrainLoop(Rule):
    def __init__(self, inp, model, loop_indices):
        outputs = LoopRecipeFile.loop_output(
            model[0], model[1], loop_indices)
        super().__init__([inp], outputs)

    def make(self, conf, cli_args):
        in_fobj = self.inputs[0].open(conf, cli_args, mode='rb')
        # for debug reasons, only outputs first two
        out_fobjs = [out.open(conf, cli_args, mode='wb')
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
