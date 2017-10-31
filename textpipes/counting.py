import collections
import logging

from .core.recipe import Rule
from .components.core import SingleCellComponent, DeadEndPipe
from .components.filtering import Filter

logger = logging.getLogger('textpipes')


class CountTokensComponent(SingleCellComponent):
    def __init__(self, output, words_only=None, **kwargs):
        side_outputs = [output]
        if words_only:
            side_outputs.append(words_only)
        # must disable multiprocessing
        super().__init__(side_outputs=side_outputs, mp=False, **kwargs)
        self.count_file = output
        self.words_file = words_only
        self.counts = collections.Counter()

    def single_cell(self, sentence):
        for token in sentence.split():
            self.counts[token] += 1

    def post_make(self, side_fobjs):
        fobj = side_fobjs[self.count_file]
        if self.words_file:
            wo_fobj = side_fobjs[self.words_file]
        for (wtype, count) in self.counts.most_common():
            fobj.write('{}\t{}\n'.format(count, wtype))
            if self.words_file:
                wo_fobj.write('{}\n'.format(wtype))
        del self.counts


class CountTokens(DeadEndPipe):
    def __init__(self, inp, output, words_only=None, **kwargs):
        component = CountTokensComponent(output, words_only=words_only)
        super().__init__([component], [inp], **kwargs)


# concatenate countfiles before using this (has single input)
# FIXME: DRY with CountTokensComponent
class CombineCountsComponent(SingleCellComponent):
    def __init__(self, output, reverse=False, words_only=None, **kwargs):
        side_outputs = [output]
        if words_only:
            side_outputs.append(words_only)
        # must disable multiprocessing
        super().__init__(side_outputs=side_outputs, mp=False, **kwargs)
        self.count_file = output
        self.words_file = words_only
        self.counts = collections.Counter()
        # BPE wants word first, followed by count
        self.reverse = reverse

    def single_cell(self, line):
        count, wtype = line.strip().split()
        self.counts[wtype] += int(count)

    def post_make(self, side_fobjs):
        fobj = side_fobjs[self.count_file]
        if self.words_file:
            wo_fobj = side_fobjs[self.words_file]
        for (wtype, count) in self.counts.most_common():
            pair = (wtype, count) if self.reverse else (count, wtype)
            fobj.write('{}\t{}\n'.format(*pair))
            if self.words_file:
                wo_fobj.write('{}\n'.format(wtype))
        del self.counts

class CombineCounts(DeadEndPipe):
    def __init__(self, inp, output, reverse=False, words_only=None, **kwargs):
        component = CombineCountsComponent(
            output, reverse=reverse, words_only=words_only)
        super().__init__([component], [inp], **kwargs)


class FilterCounts(Filter):
    """Apply a filter to counts based on just the token"""
    def __init__(self, filtr):
        super().__init__()
        self.filtr = filtr

    def __call__(self, line, side_fobjs=None):
        """Returns True if the line should be filtered out"""
        count, wtype = line.strip().split()
        return self.filtr(wtype)


class RemoveCounts(SingleCellComponent):
    def single_cell(self, line):
        count, wtype = line.strip().split()
        return wtype
