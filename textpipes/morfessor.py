import os
import subprocess

from .core.recipe import Rule
from .core.platform import run
from .core.utils import FOURDOT, FIVEDOT
from .components.core import MonoPipeComponent, MonoPipe
from .external import simple_external

TrainMorfessor = simple_external(
    'TrainMorfessor', ['infile'], ['model', 'params', 'lexicon'],
    'morfessor-train {infile} --save-segmentation {model}'
    ' --save-parameters {params} --lexicon {lexicon} {argstr}')

TrainMorfessorSimple = simple_external(
    'TrainMorfessor', ['infile'], ['model'],
    'morfessor-train {infile} --save-segmentation {model} {argstr}')

TrainMorfessorSemisup = simple_external(
    'TrainMorfessorSemisup',
    ['infile', 'annots', 'dev'], ['model', 'params', 'lexicon'],
    'morfessor-train {infile} --annotations {annots} --develset {dev}'
    ' --save-segmentation {model}'
    ' --save-parameters {params} --lexicon {lexicon} {argstr}')

TrainFlatcat = simple_external(
    'TrainFlatcat', ['infile'], ['model'],
    'flatcat-train {infile} -s {model} --category-separator ' + FOURDOT + ' {argstr}')

TrainFlatcatSemisup = simple_external(
    'TrainFlatcat', ['infile', 'annots'], ['model'],
    'flatcat-train {infile} --annotations {annots}'
    ' -s {model} --category-separator ' + FOURDOT + ' {argstr}')

class ApplyMorfessor(Rule):
    def __init__(self, *args,
                 sep=FIVEDOT + ' ', fmt='{analysis}',
                 no_space_ok=False,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sep = sep
        self.fmt = fmt
        assert no_space_ok or ' ' in self.sep
        self.add_opt_dep('morfessor-segment', binary=True)

    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        model = self.inputs[1](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        run('{prog} {infile} --load-segmentation {model} --output {outfile}'
            ' --output-format-separator "{sep}" --output-format "{fmt}" --output-newlines'.format(
                prog='morfessor-segment',
                infile=infile,
                model=model,
                outfile=outfile,
                sep=self.sep,
                fmt=self.fmt))


class ApplyFlatcat(Rule):
    def __init__(self, *args,
                 sep=FIVEDOT + ' ', fmt='{analysis}', catsep=FOURDOT, argstr='',
                 no_space_ok=False,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sep = sep
        self.fmt = fmt
        self.catsep = catsep
        self.argstr = argstr
        assert no_space_ok or ' ' in self.sep
        self.add_opt_dep('flatcat-segment', binary=True)

    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        model = self.inputs[1](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        run('{prog} {model} {infile} --output {outfile}'
            ' --output-construction-separator "{sep}" --output-format "{fmt}"'
            ' --output-newlines {argstr} --category-separator {catsep}'.format(
                prog='flatcat-segment',
                infile=infile,
                model=model,
                outfile=outfile,
                sep=self.sep,
                fmt=self.fmt,
                catsep=self.catsep,
                argstr=self.argstr))


class OverrideSegmentationComponent(MonoPipeComponent):
    """combine overlapping segmentation maps, so that the side input has preference"""
    def __init__(self, override_file, bnd_marker=FIVEDOT, **kwargs):
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
