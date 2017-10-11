import os
import subprocess

from .core.recipe import Rule

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
        subprocess.check_call(
            ['{catcmd} {infiles} | {zipcmd} > {outfile}'.format(
                catcmd=catcmd,
                infiles=' '.join(infiles),
                zipcmd=zipcmd,
                outfile=outfile)
            ], shell=True)


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
