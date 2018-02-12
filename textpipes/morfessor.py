import os
import subprocess

from .core.recipe import Rule
from .core.platform import run
from .components.core import MonoPipeComponent, MonoPipe

class ApplyMorfessor(Rule):
    def __init__(self, *args,
                 sep='@@ ', fmt='{analysis}',
                 no_space_ok=False,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sep = sep
        self.fmt = fmt
        assert no_space_ok or ' ' in self.sep

    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        model = self.inputs[1](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        run('{prog} {infile} --load-segmentation {model} --output {outfile}'
            ' --output-format-separator "{sep}" --output-format {fmt} --output-newlines'.format(
                prog='morfessor-segment',
                infile=infile,
                model=model,
                outfile=outfile,
                sep=self.sep,
                fmt=self.fmt))


class OverrideSegmentationComponent(MonoPipeComponent):
    """combine overlapping segmentation maps, so that the side input has preference"""
    def __init__(self, override_file, bnd_marker='@@', **kwargs):
        super().__init__(side_inputs=[override_file], **kwargs)
        self.override_file = override_file
        self.bnd_marker = bnd_marker
        self.mapping = {}

    def pre_make(self, side_fobjs):
        for line in side_fobjs[self.override_file]:
            tgt = line.split()
            # bnd_marker not part of actual surface form
            src = ''.join(tgt).replace(self.bnd_marker, '')
            self.mapping[src] = line

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for line in stream:
            tgt = line.split()
            # bnd_marker not part of actual surface form
            src = ''.join(tgt).replace(self.bnd_marker, '')
            if src not in self.mapping:
                self.mapping[src] = line
        for key in sorted(self.mapping.keys()):
            yield self.mapping[key]


class OverrideSegmentation(MonoPipe):
    """combine overlapping segmentation maps, so that the latter input has preference"""
    def __init__(self, main_file, override_file, out, **kwargs):
        super().__init__(
            [OverrideSegmentationComponent(override_file, **kwargs)],
            [main_file], [out])