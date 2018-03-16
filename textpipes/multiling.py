import collections
import re

from .components.filtering import FILTER_ALPHA
from .core.platform import run
from .core.recipe import Rule
from .core.utils import safe_zip, progress

# Rule, not Component (input pairs not synchronous)
class TriangulateParallel(Rule):
    def __init__(self, pivot_a, inp_a, pivot_b, inp_b, out_a, out_b):
        self.pivot_a = pivot_a
        self.inp_a = inp_a
        self.pivot_b = pivot_b
        self.inp_b = inp_b
        self.out_a = out_a
        self.out_b = out_b
        super().__init__(
            [pivot_a, inp_a, pivot_b, inp_b],
            [out_a, out_b])

    def make(self, conf, cli_args=None):
        pivot_a = self.pivot_a.open(conf, cli_args, mode='rb')
        inp_a = self.inp_a.open(conf, cli_args, mode='rb')
        pivot_b = self.pivot_b.open(conf, cli_args, mode='rb')
        inp_b = self.inp_b.open(conf, cli_args, mode='rb')

        out_a = self.out_a.open(conf, cli_args, mode='wb')
        out_b = self.out_b.open(conf, cli_args, mode='wb')

        map_a = {}
        for (pivot, a) in safe_zip(pivot_a, inp_a):
            pivot = ''.join([x for x in pivot.lower() if x in FILTER_ALPHA])
            map_a[pivot] = a

        for (pivot, b) in safe_zip(pivot_b, inp_b):
            pivot = ''.join([x for x in pivot.lower() if x in FILTER_ALPHA])
            a = map_a.get(pivot, None)
            if a is None:
                # no match
                continue
            out_a.write(a)
            out_a.write('\n')
            out_b.write(b)
            out_b.write('\n')

        for fobj in (pivot_a, inp_a, pivot_b, inp_b, out_a, out_b):
            fobj.close()


class FastAlign(Rule):
    def __init__(self, inp, out, base_argstr='-v -d -o ', argstr='', **kwargs):
        super().__init__([inp], [out], **kwargs)
        self.argstr = base_argstr + argstr

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
    def __init__(self, alignment, src, trg, out, min_freq=2, **kwargs):
        super().__init__([alignment, src, trg], [out], **kwargs)
        self.min_freq = min_freq

    def make(self, conf, cli_args):
        # Make a tuple of generators that reads from main_inputs
        readers = [inp.open(conf, cli_args, mode='rb')
                   for inp in self.inputs]
        # read one line from each and yield it as a tuple
        stream = safe_zip(*readers)

        counts = collections.Counter()
        for (i, tpl) in enumerate(stream):
            aligns, src, trg = tpl
            aligns = aligns.split()
            aligns = [x.split('-') for x in aligns]
            aligns = [(int(x) for x in pair) for pair in aligns]
            src = src.split()
            trg = trg.split()
            for src_i, trg_i in aligns:
                counts[(src[src_i], trg[trg_i])] += 1

        fobj = self.outputs[0].open(conf, cli_args, mode='wb')
        stream = progress(stream, self, conf, '(multi)')
        for (pair, count) in counts.most_common():
            if count < self.min_freq:
                break
            fobj.write('{}\t{}\n'.format(*pair))
        fobj.close()

