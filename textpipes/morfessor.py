import os
import subprocess

from .core.recipe import Rule
from .core.platform import run

class ApplyMorfessor(Rule):
    def __init__(self, *args,
                 sep='@@ ', fmt='{analysis}',
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sep = sep
        self.fmt = fmt

    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        model = self.inputs[1](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        run('{prog} {infile} --load-segmentation {model} --output {outfile}'
            ' --construction-separator "{sep}" --output-format {fmt} --output-newlines'.format(
                prog='morfessor-segment',
                infile=infile,
                model=model,
                outfile=outfile,
                sep=self.sep,
                fmt=self.fmt))
