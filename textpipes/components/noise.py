import random

from .core import SingleCellComponent

class DropTokens(SingleCellComponent):
    def __init__(self, drop_prob=0.1, **kwargs):
        super().__init__(**kwargs)
        self.drop_prob = drop_prob

    def _draw(self):
        return random.random() < self.drop_prob

    def single_cell(self, line):
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

    def single_cell(self, line):
        tokens = line.strip().split()
        indices = [(i + random.uniform(-self.max_dist, self.max_dist), token)
                   for (i, token) in enumerate(tokens)]
        indices.sort()
        return ' '.join(token for (i, token) in indices)
