import os
import subprocess

from .step import Step

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


class DummyPipe(Step):
    def make(self, conf, cli_args):
        inpair = maybe_gz_in(self.inputs[0](conf, cli_args))
        outpair = maybe_gz_out(self.outputs[0](conf, cli_args))
        print('concrete files: {} {}'.format(inpair, outpair))
        subprocess.check_call(
            (os.path.join(WRAPPER_DIR, 'simple_pipe.sh'),)
            + inpair
            + ('tac',)  # this is the dummy command
            + outpair)


