import collections
import logging

from ..recipe import Rule
from .core import SingleCellComponent, DeadEndPipe

logger = logging.getLogger(__name__)


class CountTokensComponent(SingleCellComponent):
    def __init__(self, output, **kwargs):
        # must disable multiprocessing 
        super().__init__(side_outputs=[output], mp=False, **kwargs)
        self.count_file = output
        self.counts = collections.Counter()

    def single_cell(self, sentence):
        for token in sentence.split():
            self.counts[token] += 1

    def post_make(self, side_fobjs):
        fobj = side_fobjs[self.count_file]
        for (wtype, count) in self.counts.most_common():
            fobj.write('{}\t{}\n'.format(count, wtype))
        del self.counts


class CountTokens(DeadEndPipe):
    def __init__(self, inputs, output, **kwargs):
        component = CountTokensComponent(output)
        super().__init__([component], inputs, **kwargs)

