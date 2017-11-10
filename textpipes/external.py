import os
import subprocess

from .core.recipe import Rule
from .core.platform import run

# FIXME: use package resources instead
WRAPPER_DIR = os.path.join(
    os.path.dirname(__file__), 'wrappers')

# "transparent" handling of gz for piping scripts
# choose between cat and zcat for input
def maybe_gz_in(infile):
    if infile.endswith('.gz'):
        return 'zcat', infile
    else:
        return 'cat', infile
# choose between tee and gzin for output
# tee used like this is a noop
def maybe_gz_out(outfile):
    if outfile.endswith('.gz'):
        return 'gzip', outfile
    else:
        return 'tee', outfile


class Concatenate(Rule):
    def __init__(self, *args, resource_class='make_immediately', **kwargs):
        super().__init__(*args, resource_class=resource_class, **kwargs)

    def make(self, conf, cli_args):
        infiles = [inp(conf, cli_args) for inp in self.inputs]
        if all(infile.endswith('.gz') for infile in infiles):
            catcmd = 'zcat'
        elif all(not infile.endswith('.gz') for infile in infiles):
            catcmd = 'cat'
        else:
            raise Exception('trying to concatenate gzipped and plain files')
        zipcmd, outfile = maybe_gz_out(self.outputs[0](conf, cli_args))
        run('{catcmd} {infiles} | {zipcmd} > {outfile}'.format(
                catcmd=catcmd,
                infiles=' '.join(infiles),
                zipcmd=zipcmd,
                outfile=outfile)
            )


class LearnBPE(Rule):
    def __init__(self, *args, vocabulary=10000, wordcounts=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.vocabulary = vocabulary
        self.wordcounts = wordcounts

    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        # FIXME: would be much better if this would fail in --check
        assert not infile.endswith('.gz')
        assert not outfile.endswith('.gz')
        run('{prog} --input {infile} --output {outfile}'
            ' --symbols {vocabulary} {wc}'.format(
                prog=os.path.join(WRAPPER_DIR, 'learn_bpe.py'),
                infile=infile,
                outfile=outfile,
                vocabulary=self.vocabulary,
                wc=' --dict-input' if self.wordcounts else ''
                ))

class ApplyBPE(Rule):
    def __init__(self, *args, sep='@@', **kwargs):
        super().__init__(*args, **kwargs)
        self.sep = sep

    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        codes = self.inputs[1](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        # FIXME: would be much better if this would fail in --check
        assert not infile.endswith('.gz')
        assert not codes.endswith('.gz')
        assert not outfile.endswith('.gz')
        run('{prog} --input {infile} --codes {codes} --output {outfile}'
            ' --separator {sep}'.format(
                prog=os.path.join(WRAPPER_DIR, 'apply_bpe.py'),
                infile=infile,
                codes=codes,
                outfile=outfile,
                sep=self.sep))


# FIXME: obsolete
class DummyPipe(Rule):
    def make(self, conf, cli_args):
        inpair = maybe_gz_in(self.inputs[0](conf, cli_args))
        outpair = maybe_gz_out(self.outputs[0](conf, cli_args))
        print('concrete files: {} {}'.format(inpair, outpair))
        # FIXME: use shell=True instead?
        subprocess.check_call(
            (os.path.join(WRAPPER_DIR, 'simple_pipe.sh'),)
            + inpair
            + ('tac',)  # this is the dummy command
            + outpair)
