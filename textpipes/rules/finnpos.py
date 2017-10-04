# flatten sentences into single-column (surface) tabular representation
class SingleSurfaceColumn(Rule):
    # FIXME: component
    for line in sys.stdin:
        line = line.strip()
        for token in line.split():
            yield token
        # empty line separates sentences
        yield ''

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
class ExtractField(Rule):
    pass

# cluster lemmas (in general: word2vec)
class Word2VecCluster(Rule):
    pass

# apply lemma clusters
class MapColumn(Rule):
    pass

# mangle fields into (src-marked, full-tags, surface)
