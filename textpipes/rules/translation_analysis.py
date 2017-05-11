import collections
import logging
import re

logger = logging.getLogger(__name__)
try:
    import chrF
except ImportError:
    logger.warning('Unable to load chrF.')
    logger.warning('You will not be able to use AnalyzeChrF.')

from .wmt_sgm import read_sgm, read_bleu
from ..pipe import ParallelPipe
from ..components import ParallelPipeComponent, PerColumn, IdentityComponent
from ..components.tokenizer import Tokenize


class AnalyzeTranslations(ParallelPipe):
    def __init__(self, components, main_inputs, main_outputs, n_refs=1):
        """
        6 inputs:
          4 sgm files: source, bl, sys, (multi)ref
          2 mteval bleu outputs: bl, sys
        Components must take 5 or more columns
        where 4 first are (key, src, bl, sys) and rest are refs.
        Note that BLEUs are not passed through components.
        """
        super().__init__(components, main_inputs, main_outputs)
        self.n_refs = n_refs
        self.key_order = []

    def make(self, conf, cli_args=None):
        # Make a tuple of generators that reads from main_inputs
        readers = [inp.open(conf, cli_args, mode='rb')
                   for inp in self.main_inputs]

        # FIXME: fully read in each input into dicts
        bl_segs = {(seg.docid, seg.segid): seg.text
                   for seg in read_sgm(readers[1])}
        sys_segs = {(seg.docid, seg.segid): seg.text
                    for seg in read_sgm(readers[2])}
        ref_segs = collections.defaultdict(list)
        for seg in read_sgm(readers[3]):
            ref_segs[(seg.docid, seg.segid)].append(seg.text)
        bl_bleus = {(seg.docid, seg.segid): seg.bleu
                    for seg in read_bleu(readers[4])}
        sys_bleus = {(seg.docid, seg.segid): seg.bleu
                    for seg in read_bleu(readers[5])}
        # when reading in source (for order)
        #   yield into stream a tuple with one or more columns from each
        def zip_streams():
            for seg in read_sgm(readers[0]):
                self.key_order.append((seg.docid, seg.segid))
                yield (
                    (seg.docid, seg.segid),
                    seg.text,
                    bl_segs[(seg.docid, seg.segid)],
                    sys_segs[(seg.docid, seg.segid)]) + \
                    tuple(ref_segs[(seg.docid, seg.segid)])
        stream = zip_streams()

        stream, side_fobjs = self._make_helper(stream, conf, cli_args)

        # Drain pipeline, throwing the output away
        for tpl in stream:
            pass
        # post_make must be done after draining
        self._post_make(side_fobjs)

        # write for each key fields of all columns into a tsv file
        components_with_fields = []
        all_fields = []
        for component in self.components:
            try:
                all_fields.extend(component.fields)
                components_with_fields.append(component)
            except AttributeError:
                pass
        extra_fields, extra_fields_func = self._extra_fields(all_fields)
        all_fields.extend(extra_fields)

        # FIXME: do the sorted translations also
        with self.main_outputs[0].open(conf, cli_args, mode='wb') as outfobj:
            outfobj.write('\t'.join(all_fields))
            outfobj.write('\n')
            for key in self.key_order:
                columns = []
                for component in components_with_fields:
                    columns.extend(component[key])
                    columns.extend(extra_fields_func(columns))
                outfobj.write('\t'.join(str(x) for x in columns))
                outfobj.write('\n')

        # close side file objects
        for fobj in readers + list(side_fobjs.values()):
            fobj.close()

    def _extra_fields(self, all_fields):
        # FIXME: calculate deltas etc for sorting
        return [], lambda x: []
        



class AnalyzeChrF(ParallelPipeComponent):
    """Perform sentence level character-F bl-refs and sys-refs.
    This should be applied before retokenizing."""
    def __init__(self, side_inputs=None, side_outputs=None):
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)
        # (docid, segid) -> tpl
        self.scores = {}
        # FIXME: use levenshtein for soft repeats
        self.fields = (
            'bl_chrF1', 'bl_chrF2',
            'sys_chrF1', 'sys_chrF2',
        )

    def _chrf_helper(self, hypothesis, references):
        max_n = 6
        nw = [1/float(max_n) for _ in range(max_n)]
        stats = chrF.measure.evaluate_single(
            hypothesis, references, max_n, factor=None)
        pres, recs, fs = stats.ngram_prf(1.0) # squaring does nothing
        _, _, score1 = chrF.measure.apply_ngram_weights(
            pres, recs, fs, nw)
        pres, recs, fs = stats.ngram_prf(2.0 ** 2)
        _, _, score2 = chrF.measure.apply_ngram_weights(
            pres, recs, fs, nw)
        return score1, score2

    def __call__(self, stream, side_fobjs=None):
        for tpl in stream:
            key = tpl[0]
            bl = tpl[2]
            sys = tpl[3]
            refs = tpl[4:]
            self.scores[key] = self._chrf_helper(bl, refs) + \
                               self._chrf_helper(sys, refs)
            yield tpl

    def __getitem__(self, key):
        return self.scores[key]


class ReTokenize(PerColumn):
    """Inputs are postprocessed translations.
    Analysis that prefers tokenized input should be
    preceded by this component."""
    def __init__(self, src_lang, trg_lang, n_refs=1):
        src_tokenizer = Tokenize(src_lang)
        trg_tokenizer = Tokenize(trg_lang)
        components = [IdentityComponent(),  # key
                      src_tokenizer,        # src
                      trg_tokenizer,        # bl
                      trg_tokenizer,        # sys
                     ]
        for _ in range(n_refs):
            # all refs tokenized by target tokenizer
            components.append(trg_tokenizer)
        super().__init__(components)


class AnalyzeLetteredNames(ParallelPipeComponent):
    """Counts how many tokens in the source are eligible for lettering
    by components.LetterizeNames.
    This should be applied after retokenizing."""
    def __init__(self, side_inputs=None, side_outputs=None):
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)
        # (docid, segid) -> (count,)
        self.counts = {}
        self.fields = ('lnames',)

    def __call__(self, stream, side_fobjs=None):
        for tpl in stream:
            key = tpl[0]
            src = tpl[1]
            count = sum(len(token) > 1 and (token[0].isupper() or token[0].isdigit())
                        for token in src.split())
            self.counts[key] = (count,)
            yield tpl

    def __getitem__(self, key):
        return self.counts[key]


class AnalyzeRepetitions(ParallelPipeComponent):
    """Counts repeated tokens in the references and translations
    This should be applied after retokenizing."""
    def __init__(self, side_inputs=None, side_outputs=None):
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)
        # (docid, segid) -> tpl
        self.counts = {}
        # FIXME: use levenshtein for soft repeats
        self.fields = (
            'ref_reps_anywhere', 'ref_reps_conseq',
            'bl_reps_anywhere',  'bl_reps_conseq',
            'sys_reps_anywhere', 'sys_reps_conseq',
        )

    def _repeats(self, line):
        prev = None
        seen = set()
        anywhere = 0
        conseq = 0
        for token in line.split():
            if token == prev:
                conseq += 1
            if token in seen:
                anywhere += 1
            prev = token
            seen.add(token)
        return anywhere, conseq

    def __call__(self, stream, side_fobjs=None):
        for tpl in stream:
            key = tpl[0]
            bl = tpl[2]
            sys = tpl[3]
            refs = tpl[4:]
            ref_reps_anywhere, ref_reps_conseq = zip(
                *(self._repeats(ref) for ref in refs))
            ref_reps_anywhere = max(ref_reps_anywhere)
            ref_reps_conseq = max(ref_reps_conseq)
            self.counts[key] = (ref_reps_anywhere, ref_reps_conseq) + \
                self._repeats(bl) + self._repeats(sys)
            yield tpl

    def __getitem__(self, key):
        return self.counts[key]


class AnalyzeLength(ParallelPipeComponent):
    """Counts length in tokens and chars
    This should be applied after retokenizing."""
    def __init__(self, side_inputs=None, side_outputs=None):
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)
        # (docid, segid) -> tpl
        self.lengths = {}
        # ref len is average
        self.fields = (
            'src_len_chars', 'src_len_words',
            'bl_len_chars',  'bl_len_words',
            'sys_len_chars', 'sys_len_words',
            'ref_len_chars', 'ref_len_words',
        )

    def __call__(self, stream, side_fobjs=None):
        for tpl in stream:
            key = tpl[0]
            src = tpl[1]
            bl = tpl[2]
            sys = tpl[3]
            refs = tpl[4:]
            self.lengths[key] = (
                len(src), len(src.split(' ')),
                len(bl),  len(bl.split(' ')),
                len(sys), len(sys.split(' ')),
                max(len(ref) for ref in refs),
                max(len(ref.split(' ')) for ref in refs))
            yield tpl

    def __getitem__(self, key):
        return self.lengths[key]