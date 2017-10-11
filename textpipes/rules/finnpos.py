import itertools

from ..recipe import Rule
from ..components.core import MonoPipe, MonoPipeComponent

# flatten sentences into single-column (surface) tabular representation
class SingleSurfaceColumnComponent(MonoPipeComponent):
    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for line in stream:
            line = line.strip()
            for token in line.split():
                yield token
            # empty line separates sentences
            yield ''

class SingleSurfaceColumn(MonoPipe):
    def __init__(self, *args, **kwargs):
        super().__init__(
            [SingleSurfaceColumnComponent()],
            *args, **kwargs)

class Finnpos(Rule):
    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        # in > ftb-label > out

# deterministic lemma modification
class LemmaModification(Rule):
    def __init__(self, *args,
                 number_tag=True,
                 hyphen_compounds=True,
                 strip_numbers=True,
                 strip_hyphens=True,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.number_tag = number_tag
        self.hyphen_compounds = hyphen_compounds
        self.strip_numbers = strip_numbers
        self.strip_hyphens = strip_hyphens

# extract and unflatten a single field
class ExtractColumnComponent(MonoPipeComponent):
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
            line = line.strip()
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

class ExtractColumn(MonoPipe):
    def __init__(self, col_i, sep='\t', *args, **kwargs):
        super().__init__(
            [ExtractColumnComponent(col_i, sep)],
            *args, **kwargs)

# cluster lemmas (in general: word2vec)
class Word2VecCluster(Rule):
    pass

# apply lemma clusters
class MapColumn(Rule):
    pass

# mangle fields into (src-marked, full-tags, surface)

# apply a segmentation, copy tags to each component
