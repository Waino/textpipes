import collections
import logging
import math

from .core.recipe import Rule
from .components.core import SingleCellComponent, DeadEndPipe, \
    MonoPipe, MonoPipeComponent, apply_component
from .components.filtering import Filter

logger = logging.getLogger('textpipes')

def sort_counts(counts):
    return sorted(counts, key=lambda pair: (-pair[1], pair[0]))

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
        for (wtype, count) in sort_counts(self.counts.most_common()):
            fobj.write('{}\t{}\n'.format(count, wtype))
            if self.words_file:
                wo_fobj.write('{}\n'.format(wtype))
        del self.counts


class CountTokens(DeadEndPipe):
    def __init__(self, inp, output, words_only=None, **kwargs):
        component = CountTokensComponent(output, words_only=words_only)
        super().__init__([component], [inp], **kwargs)


class CountCharsComponent(CountTokensComponent):
    def single_cell(self, sentence):
        for char in sentence:
            self.counts[char] += 1

class CountChars(DeadEndPipe):
    def __init__(self, inp, output, **kwargs):
        component = CountCharsComponent(output, words_only=None)
        super().__init__([component], [inp], **kwargs)


class ScaleCountsComponent(SingleCellComponent):
    def __init__(self, scale, **kwargs):
        super().__init__(mp=False, **kwargs)
        self.scale = scale

    def single_cell(self, line):
        count, wtype = line.strip().split()
        count = math.ceil(float(count) * self.scale)
        return '{}\t{}'.format(count, wtype)


class ScaleCounts(MonoPipe):
    def __init__(self, inp, output, scale, **kwargs):
        component = ScaleCountsComponent(scale)
        super().__init__([component], [inp], [output], **kwargs)


class CombineCounts(MonoPipe):
    def __init__(self, inputs, output, reverse=False, words_only=None, balance=False, threshold=0, **kwargs):
        extra_side_outputs = (words_only,) if words_only else ()
        super().__init__([], inputs, [output], extra_side_outputs=extra_side_outputs, **kwargs)
        self.output = output
        self.count_file = output
        self.words_file = words_only
        # BPE wants word first, followed by count
        self.reverse = reverse
        self.reversed_input = False
        # scale counts to balance contribution of different inputs
        self.balance = balance
        # minimum unscaled count to include in output
        self.threshold = threshold

    def make(self, conf, cli_args=None):
        combined_counts = collections.Counter()
        unscaled_counts = []
        sums = []
        # counting
        for inp in self.main_inputs:
            counts = collections.Counter()
            count_sum = 0
            in_fobj = inp.open(conf, cli_args, mode='r')
            for line in in_fobj:
                if self.reversed_input:
                    wtype, count = line.split('\t')
                else:
                    count, wtype = line.split('\t')
                try:
                    count = int(count)
                except ValueError:
                    self.reversed_input = True
                    wtype, count = line.split('\t')
                    count = int(count)
                count_sum += count
                if count >= self.threshold:
                    counts[wtype] += count
            in_fobj.close()
            unscaled_counts.append(counts)
            sums.append(count_sum)
        # balancing
        max_sum = max(sums)
        scales = [max_sum / x for x in sums]
        for counts, scale in zip(unscaled_counts, scales):
            if not self.balance or scale == 1:
                combined_counts.update(counts)
            else:
                for wtype, count in counts.items():
                    combined_counts[wtype] += int(count * scale)
        # writing
        out_fobj = self.count_file.open(conf, cli_args, mode='w')
        if self.words_file:
            wo_fobj = self.words_file.open(conf, cli_args, mode='w')
        for (wtype, count) in sort_counts(combined_counts.most_common()):
            pair = (wtype, count) if self.reverse else (count, wtype)
            out_fobj.write('{}\t{}\n'.format(*pair))
            if self.words_file:
                wo_fobj.write('{}\n'.format(wtype))
        out_fobj.close()
        if self.words_file:
            wo_fobj.close()

class CombineWordlistsComponent(MonoPipeComponent):
    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        words = set()
        for word in stream:
            words.add(word)
        for word in sorted(words):
            yield word

CombineWordlists = apply_component(CombineWordlistsComponent(), auto_concat=True)


class FilterCounts(Filter):
    """Apply a filter to counts based on just the token"""
    def __init__(self, filtr):
        super().__init__()
        self.filtr = filtr

    def __call__(self, line, side_fobjs=None):
        """Returns True if the line should be filtered out"""
        count, wtype = line.strip().split()
        return self.filtr(wtype)


class RemoveCountsComponent(SingleCellComponent):
    def single_cell(self, line):
        count, wtype = line.strip().split()
        return wtype

RemoveCounts = apply_component(RemoveCountsComponent())


class SegmentCountsFile(SingleCellComponent):
    def __init__(self, segmenters, output, words_only=None, reverse=False, **kwargs):
        assert all(isinstance(segmenter, SingleCellComponent) for segmenter in segmenters)
        self.segmenters = segmenters
        side_outputs = [output]
        if words_only:
            side_outputs.append(words_only)
        # must disable multiprocessing
        super().__init__(side_outputs=side_outputs, mp=False, **kwargs)
        self.count_file = output
        self.words_file = words_only
        self.counts = collections.Counter()
        self.rev = reverse

    def single_cell(self, line):
        count, word = line.split(None, 1)
        if self.rev:
            count, word = word, count
        count = int(count)
        modified = word
        for segmenter in self.segmenters:
            modified = segmenter.single_cell(modified)
        parts = modified.split()
        for part in parts:
            self.counts[part] += count

    def post_make(self, side_fobjs):
        fobj = side_fobjs[self.count_file]
        if self.words_file:
            wo_fobj = side_fobjs[self.words_file]
        for (wtype, count) in sort_counts(self.counts.most_common()):
            if self.rev:
                count, wtype = wtype, count
            fobj.write('{}\t{}\n'.format(count, wtype))
            if self.words_file:
                wo_fobj.write('{}\n'.format(wtype))
        del self.counts


class TfIdfComponent(SingleCellComponent):
    def __init__(self, output, parse_func, **kwargs):
        side_outputs = [output]
        # must disable multiprocessing
        super().__init__(side_outputs=side_outputs, mp=False, **kwargs)
        self.count_file = output
        self.parse_func = parse_func
        self.tf_counts = collections.Counter()
        self.df_counts = collections.Counter()
        self.total_docs = 0
        self.docid = None
        self.seen = set()

    def single_cell(self, line):
        docid, sentence = self.parse_func(line)
        if docid != self.docid:
            # start of new document
            self.seen = set()
            self.docid = docid
            self.total_docs += 1
        for token in sentence.split():
            self.tf_counts[token] += 1
            if token not in self.seen:
                self.df_counts[token] += 1
            self.seen.add(token)

    def post_make(self, side_fobjs):
        fobj = side_fobjs[self.count_file]
        tf_idf = collections.Counter()
        for wtype, tf in self.tf_counts.items():
            df = self.df_counts[wtype]
            tf_idf[wtype] = math.log(1 + tf) * math.log(self.total_docs / (1 + df))
        for (wtype, weight) in sort_counts(tf_idf.most_common()):
            tf = self.tf_counts[wtype]
            df = self.df_counts[wtype]
            fobj.write('{}\t{}\t{}\t{}\n'.format(weight, wtype, tf, df))
        del self.tf_counts
        del self.df_counts


class TfIdf(DeadEndPipe):
    def __init__(self, inp, output, parse_func, **kwargs):
        component = TfIdfComponent(output, parse_func=parse_func)
        super().__init__([component], [inp], **kwargs)
