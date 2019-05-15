import collections
import re
import math

try:
    import Levenshtein as lev
except ImportError:
    # install python-Levenshtein
    # warnings emitted by check in cli
    pass

from .components.core import SingleCellComponent, apply_component
from .components.filtering import FILTER_ALPHA, Filter
from .components.preprocessing import TruncateWords
from .tabular import PasteColumns
from .core.platform import run
from .core.recipe import Rule
from .core.utils import safe_zip, progress, FIVEDOT

# Rule, not Component (input pairs not synchronous)
class TriangulateParallel(Rule):
    def __init__(self, pivot_a, inp_a, pivot_b, inp_b, out_a, out_b, out_pivot=None):
        self.pivot_a = pivot_a
        self.inp_a = inp_a
        self.pivot_b = pivot_b
        self.inp_b = inp_b
        self.out_a = out_a
        self.out_b = out_b
        self.out_pivot = out_pivot
        outputs = [out_a, out_b]
        if out_pivot is not None:
            outputs.append(out_pivot)
        super().__init__(
            [pivot_a, inp_a, pivot_b, inp_b],
            outputs)

    def make(self, conf, cli_args=None):
        pivot_a = self.pivot_a.open(conf, cli_args, mode='r')
        inp_a = self.inp_a.open(conf, cli_args, mode='r')
        pivot_b = self.pivot_b.open(conf, cli_args, mode='r')
        inp_b = self.inp_b.open(conf, cli_args, mode='r')

        out_a = self.out_a.open(conf, cli_args, mode='w')
        out_b = self.out_b.open(conf, cli_args, mode='w')
        if self.out_pivot is not None:
            out_pivot = self.out_pivot.open(conf, cli_args, mode='w')

        map_a = {}
        for (pivot, a) in safe_zip(pivot_a, inp_a):
            pivot_lc = ''.join([x for x in pivot.lower() if x in FILTER_ALPHA])
            map_a[pivot_lc] = a

        for (pivot, b) in safe_zip(pivot_b, inp_b):
            pivot_lc = ''.join([x for x in pivot.lower() if x in FILTER_ALPHA])
            a = map_a.get(pivot_lc, None)
            if a is None:
                # no match
                continue
            out_a.write(a)
            out_a.write('\n')
            out_b.write(b)
            out_b.write('\n')
            if self.out_pivot is not None:
                out_pivot.write(pivot)
                out_pivot.write('\n')

        for fobj in (pivot_a, inp_a, pivot_b, inp_b, out_a, out_b):
            fobj.close()
        if self.out_pivot is not None:
            out_pivot.close()


class FastAlign(Rule):
    def __init__(self, inp, out, base_argstr='-v -d -o ', argstr='', **kwargs):
        super().__init__([inp], [out], **kwargs)
        self.argstr = base_argstr + argstr
        self.add_opt_dep('fast_align', binary=True)

    def make(self, conf, cli_args):
        corpus_file = self.inputs[0](conf, cli_args)
        align_file = self.outputs[0](conf, cli_args)
        run('fast_align -i {corpus_file} {argstr}'
            ' > {align_file}'.format(
                corpus_file=corpus_file,
                align_file=align_file,
                argstr=self.argstr))


class Symmetrize(Rule):
    def __init__(self, fwd, rev, out, command='grow-diag-final-and', **kwargs):
        super().__init__([fwd, rev], [out], **kwargs)
        self.command = command
        self.add_opt_dep('atools', binary=True)

    def make(self, conf, cli_args):
        fwd_file = self.inputs[0](conf, cli_args)
        rev_file = self.inputs[1](conf, cli_args)
        sym_file = self.outputs[0](conf, cli_args)
        run('atools -c {command} -i {fwd} -j {rev}'
            ' > {sym}'.format(
                command=self.command,
                fwd=fwd_file,
                rev=rev_file,
                sym=sym_file))


class WordPairs(Rule):
    def __init__(self, alignment, src, trg, out, min_freq=2, bnd_marker=None, **kwargs):
        super().__init__([alignment, src, trg], [out], **kwargs)
        self.min_freq = min_freq
        self.bnd_marker = bnd_marker

    def make(self, conf, cli_args):
        # Make a tuple of generators that reads from main_inputs
        readers = [inp.open(conf, cli_args, mode='r')
                   for inp in self.inputs]
        # read one line from each and yield it as a tuple
        stream = safe_zip(*readers)

        counts = collections.Counter()
        for (i, tpl) in enumerate(stream):
            aligns, src, trg = tpl
            aligns = aligns.split()
            aligns = [x.split('-') for x in aligns]
            aligns = [(int(x) for x in pair) for pair in aligns]
            if self.bnd_marker is not None:
                src = src.replace(self.bnd_marker, '')
                trg = trg.replace(self.bnd_marker, '')
            src = src.split()
            trg = trg.split()
            for src_i, trg_i in aligns:
                counts[(src[src_i], trg[trg_i])] += 1

        fobj = self.outputs[0].open(conf, cli_args, mode='w')
        stream = progress(stream, self, conf, '(multi)')
        for (pair, count) in counts.most_common():
            if count < self.min_freq:
                break
            fobj.write('{}\t{}\n'.format(*pair))
        fobj.close()


class Levenshtein(SingleCellComponent):
    def __init__(self, separator='\t', **kwargs):
        super().__init__(**kwargs)
        self.separator = separator
        self.add_opt_dep('Levenshtein', binary=False)

    def single_cell(self, line):
        left, right = line.split(self.separator)
        dist = lev.distance(left, right)
        return '{dist}{sep}{left}{sep}{right}'.format(
            dist=dist, sep=self.separator, right=right, left=left)


class FilterLevenshtein(Filter):
    def __init__(self, min_len=4, ratio=1/3, separator='\t', **kwargs):
        super().__init__(**kwargs)
        self.min_len = min_len
        self.ratio = ratio
        self.separator = separator

    def __call__(self, line, side_fobjs=None):
        dist, left, right = line.split(self.separator)
        lleft = len(left)
        lright = len(right)
        if lleft < self.min_len or lright < self.min_len:
            # must match exactly
            return left != right
        mean = (lleft + lright) / 2
        return int(dist) > math.ceil(mean * self.ratio)


class FilterLevenshteinLongEdits(Filter):
    def __init__(self, max_len=3, separator='\t', **kwargs):
        super().__init__(**kwargs)
        self.max_len = max_len
        self.separator = separator
        self.add_opt_dep('Levenshtein', binary=False)

    def __call__(self, line, side_fobjs=None):
        dist, left, right = line.split(self.separator)
        if len(left) == 0 or len(right) == 0:
            return False
        longest = max(self.edit_lengths(left, right))
        if longest > self.max_len:
            return True
        return False

    def remove_irrelevant(self, edits):
        for edit in edits:
            if edit[0] in ('replace', 'equal'):
                continue
            yield edit

    def edit_lengths(self, left, right):
        edits = lev.opcodes(left, right)
        edits = self.remove_irrelevant(edits)
        for op, ib, ie, jb, je in edits:
            yield max(ie - ib, je - jb)
        yield 0


def train_lexical_match(recipe,
                        inp_src, inp_trg,
                        tmp_trunc_src, tmp_trunc_trg,
                        tmp_pasted, tmp_fwd, tmp_rev, tmp_sym,
                        out_pairs):
    truncator = TruncateWords()
    truncate_words = apply_component(truncator)
    recipe.add_rule(truncate_words(inp_src, tmp_trunc_src))
    recipe.add_rule(truncate_words(inp_trg, tmp_trunc_trg))
    recipe.add_rule(PasteColumns(
        [tmp_trunc_src, tmp_trunc_trg], tmp_pasted, delimiter=' ||| '))

    recipe.add_rule(FastAlign(
        tmp_pasted, tmp_fwd))
    recipe.add_rule(FastAlign(
        tmp_pasted, tmp_rev, argstr='-r'))
    recipe.add_rule(Symmetrize(
        tmp_fwd, tmp_rev, tmp_sym, command='grow-diag-final-and'))
    recipe.add_rule(WordPairs(
        tmp_sym, tmp_trunc_src, tmp_trunc_trg, out_pairs, bnd_marker=FIVEDOT))
    return truncator


class LexicalMatchScore(Rule):
    def __init__(self, inp_src, inp_trg, pairs, scores, truncator, **kwargs):
        super().__init__([inp_src, inp_trg, pairs], [scores], **kwargs)
        self.inp_src = inp_src
        self.inp_trg = inp_trg
        self.pairs = pairs
        self.truncator = truncator

    def make(self, conf, cli_args):
        # read in pairs
        fwd_map = collections.defaultdict(set)
        rev_map = collections.defaultdict(set)
        reader = self.pairs.open(conf, cli_args, mode='r')
        for line in reader:
            src, trg = line.strip().split('\t')
            fwd_map[src].add(trg)
            rev_map[trg].add(src)
        reader.close()

        # Make a tuple of generators that reads from main_inputs
        readers = [self.inp_src.open(conf, cli_args, mode='r'),
                   self.inp_trg.open(conf, cli_args, mode='r')]
        # writer for scores
        fobj = self.outputs[0].open(conf, cli_args, mode='w')
        # read one line from each and yield it as a tuple
        stream = safe_zip(*readers)

        #stream = progress(stream, self, conf, '(multi)')
        for (i, tpl) in enumerate(stream):
            src, trg = tpl
            src_tokens = self.truncator.single_cell(src).split()
            trg_tokens = self.truncator.single_cell(trg).split()
            hits = 0
            total = 0
            for src_token in src_tokens:
                if src_token in trg_tokens:
                    # accept prefix-cognates
                    hits += 1
                elif any(trg_token in fwd_map[src_token]
                       for trg_token in trg_tokens):
                    # accept translations from map
                    hits += 1
                total += 1
            for trg_token in trg_tokens:
                if trg_token in src_tokens:
                    hits += 1
                elif any(src_token in rev_map[trg_token]
                       for src_token in src_tokens):
                    hits += 1
                total += 1
            # negated for compatibility with
            # components.filtering.FilterUsingLmScore
            score = -hits / total
            fobj.write('{}\n'.format(score))
        for reader in readers:
            reader.close()
        fobj.close()
