from .core.recipe import Rule
from .core.platform import run

class MakeVocabularies(Rule):
    def __init__(self, *args, argstr='', **kwargs):
        super().__init__(*args, **kwargs)
        self.argstr = argstr

    def make(self, conf, cli_args):
        src_infile = self.inputs[0](conf, cli_args)
        trg_infile = self.inputs[1](conf, cli_args)
        src_outfile = self.outputs[0](conf, cli_args)
        trg_outfile = self.outputs[1](conf, cli_args)
        run('make_vocabularies.py {src_infile} {trg_infile}'
            ' {src_outfile} {trg_outfile} {argstr}'.format(
                src_infile=src_infile,
                trg_infile=trg_infile,
                src_outfile=src_outfile,
                trg_outfile=trg_outfile,
                argstr=self.argstr))

class PrepareData(Rule):
    pass

class Train(Rule):
    pass

class Translate(Rule):
    pass
