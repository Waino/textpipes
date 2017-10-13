import collections
import logging

from .core.recipe import Rule
from .components.core import SingleCellComponent, DeadEndPipe
from .components.filtering import Filter

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
    def __init__(self, inp, output, **kwargs):
        component = CountTokensComponent(output)
        super().__init__([component], [inp], **kwargs)


# concatenate countfiles before using this (has single input)
class CombineCounts(SingleCellComponent):
    def __init__(self, output, reverse=False, **kwargs):
        # must disable multiprocessing
        super().__init__(side_outputs=[output], mp=False, **kwargs)
        self.count_file = output
        self.counts = collections.Counter()
        # BPE wants word first, followed by count
        self.reverse = reverse

    def single_cell(self, line):
        count, wtype = line.strip().split()
        self.counts[token] += int(count)

    def post_make(self, side_fobjs):
        fobj = side_fobjs[self.count_file]
        for (wtype, count) in self.counts.most_common():
            pair = (wtype, count) if self.reverse else (count, wtype)
            fobj.write('{}\t{}\n'.format(*pair))
        del self.counts

class CombineCounts(DeadEndPipe):
    def __init__(self, inputs, output, reverse=False, **kwargs):
        component = CombineCountsComponent(output, reverse=reverse)
        super().__init__([component], inputs, **kwargs)


class FilterCounts(Filter):
    """Apply a filter to counts based on just the token"""
    def __init__(self, filtr):
        super().__init__()
        self.filtr = filtr

    def __call__(self, line, side_fobjs=None):
        """Returns True if the line should be filtered out"""
        count, wtype = line.strip().split()
        return self.filtr(wtype)
