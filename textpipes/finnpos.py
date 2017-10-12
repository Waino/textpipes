import itertools
import subprocess
import re

from .core.recipe import Rule
from .components.core import MonoPipeComponent

# flatten sentences into single-column (surface) tabular representation
class SingleSurfaceColumn(MonoPipeComponent):
    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for line in stream:
            for token in line.split():
                yield token
            # empty line separates sentences
            yield ''

class Finnpos(Rule):
    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        # in > ftb-label > out
        subprocess.check_call(
            ['ftb-label < {infile} > {outfile}'.format(
                infile=infile,
                outfile=outfile)
            ], shell=True)

# deterministic lemma modification
class LemmaModification(SingleCellComponent):
    def __init__(self,
                 lemma_col=2, tag_col=3, sep='\t',
                 number_tag='<NUM>',
                 proper_tag='<PROPER>',
                 hyphen_compounds=True,
                 strip_numbers=True,
                 strip_hyphens=True,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.lemma_col = lemma_col
        self.tag_col = tag_col
        self.sep = sep

        self.number_tag = number_tag
        self.proper_tag = proper_tag
        self.hyphen_compounds = hyphen_compounds
        self.strip_numbers = strip_numbers
        self.strip_hyphens = strip_hyphens

        self.re_proper = re.compile(r'\[PROPER=PROPER\]')
        self.re_num = re.compile(r'[0-9]')
        self.re_punc = re.compile(r'^[,\.-]+$')
        self.re_numpunc = re.compile(r'^[0-9,\.-]+$')

    def single_cell(self, line):
        if len(line) == 0:
            yield line
        cols = line.split(self.sep)
        lemma = cols[self.lemma_col]
        tags = cols[self.tag_col]
        lemma = self._modify(lemma)
        if self.proper_tag and self.re_proper.findall(tags):
            # collapse if tagged as proper name
            lemma = self.proper_tag

        cols[self.lemma_col] = lemma
        yield self.sep.join(cols)

    def _modify(self, lemma):
        ## numbers and punctuation
        # pure punctuation unchanged
        if self.re_punc.match(lemma):
            return lemma
        if self.number_tag and self.re_numpunc.match(lemma):
            # collapse if only numbers and punctuation
            return self.number_tag
        if self.strip_numbers:
            # otherwise remove the numbers
            lemma = self.re_num.sub('', lemma)

        ## hyphens and hyphen compounds
        if self.strip_hyphens:
            # strip leading and trailing hyphens
            lemma = lemma.strip('-')
        if self.hyphen_compounds:
            # internal hyphens: keep last part
            lemma = lemma.split('-')[-1]

# extract and unflatten a single field
class ExtractColumn(MonoPipeComponent):
    def __init__(self, col_i, sep='\t', **kwargs):
        super().__init__(**kwargs)
        self.col_i = col_i
        self.sep = sep

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        result = []
        # in case the file lacks the final sentence break
        stream = itertools.chain(stream, [''])
        for line in stream:
            if len(line) == 0:
                # empty line separates sentences
                if len(result) == 0:
                    # ignore double empty line
                    continue
                yield ' '.join(result)
                result = []
                continue
            cols = line.split(self.sep)
            result.append(cols[self.col_i])

# cluster lemmas (in general: word2vec)
class Word2VecCluster(Rule):
    pass

# apply lemma clusters
class MapColumn(Rule):
    pass

# mangle fields into (src-marked, full-tags, surface)

# apply a segmentation, copy tags to each component
class SegmentColumn(Rule):
