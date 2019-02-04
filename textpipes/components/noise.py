import random
import re

from .core import SingleCellComponent
from ..core.utils import FIVEDOT

# Noise model (loosely) based on
# Lample, Denoyer and Ranzato 2017
# unsupervised machine translation using monolingual corpora only

class DropTokens(SingleCellComponent):
    def __init__(self, drop_prob=0.1, skip_line_prob=0, **kwargs):
        super().__init__(**kwargs)
        self.drop_prob = drop_prob
        self.skip_line_prob = skip_line_prob

    def _draw(self):
        return random.random() < self.drop_prob

    def single_cell(self, line):
        if random.random() < self.skip_line_prob:
            return line
        tokens = line.strip().split()
        drop = [self._draw() for _ in tokens]
        if sum(drop) == len(tokens):
            # never drop all tokens
            return line
        tokens = [token for (token, d) in zip(tokens, drop)
                  if not d]
        return ' '.join(tokens)


class PeturbOrder(SingleCellComponent):
    def __init__(self, max_dist=3, **kwargs):
        super().__init__(**kwargs)
        self.max_dist = max_dist

    def _split(self, line):
        return line.split()

    def single_cell(self, line):
        tokens = self._split(line.strip())
        indices = [(i + random.uniform(-self.max_dist, self.max_dist), token)
                   for (i, token) in enumerate(tokens)]
        indices.sort()
        return ' '.join(token for (i, token) in indices)


class SegmentationAwarePeturbOrder(PeturbOrder):
    def __init__(self, bnd_marker=FIVEDOT, **kwargs):
        super().__init__(**kwargs)
        self.bnd_marker = bnd_marker
        self.re_wordbnd = re.compile('(?<!' + bnd_marker + ') (?!' + bnd_marker + ')')

    def _split(self, line):
        return self.re_wordbnd.split(line)
