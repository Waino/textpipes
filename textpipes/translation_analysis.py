import collections
import logging
import re

logger = logging.getLogger(__name__)
try:
    import pandas as pd
except ImportError:
    # warnings emitted by check in cli
    pass
try:
    import chrF
except ImportError:
    # warnings emitted by check in cli
    pass

from .wmt_sgm import read_sgm, read_bleu
from .core.recipe import Rule, RecipeFile
from .core.utils import progress
from .components.core import ParallelPipe, ParallelPipeComponent, PerColumn, IdentityComponent
from .components.tokenizer import Tokenize
from .truecaser import TrueCase

RE_ALNUM = re.compile(r'[a-z0-9]')

class EvaluateChrF(Rule):
    def __init__(self, hyp, refs, outputs,
                 betas=(2.0,), resource_class='short', **kwargs):
        if isinstance(refs, RecipeFile):
            refs = [refs]
        super().__init__([hyp] + refs, outputs,
                         resource_class=resource_class, **kwargs)
        self.hyp = hyp
        self.refs = refs
        if len(outputs) != len(betas):
            raise Exception('EvaluateChrF got {} output files '
                            'but {} betas'.format(len(outputs), len(betas)))
        self.betas = betas

    def make(self, conf, cli_args=None):
        hyp = self.hyp.open(conf, cli_args, mode='rb')
        refs = [ref.open(conf, cli_args, mode='rb')
                for ref in self.refs]
        ref_tuples = list(zip(*refs))
        max_n = 6
        ngram_weights = [1/float(max_n) for _ in range(max_n)]
        hyp = progress(hyp, self, conf,
                       self.hyp(conf, cli_args))
        stats = chrF.measure.evaluate(
            hyp, ref_tuples, max_n=max_n,
            sentence_level=False, summary=False)
        hyp.close()
        for ref in refs:
            ref.close()
        for (out, beta) in zip(self.outputs, self.betas):
            out = out.open(conf, cli_args, mode='wb')
            tot_pre, tot_rec, tot_f = stats.ngram_prf(beta ** 2)
            pre, rec, f = chrF.measure.apply_ngram_weights(
                tot_pre, tot_rec, tot_f, ngram_weights)
            out.write('chr{}-{}\t{:.4f}\n'.format(
                'F', beta, f))
            out.write('chr{}\t{:.4f}\n'.format(
                'Prec', pre))
            out.write('chr{}\t{:.4f}\n'.format(
                'Rec', rec))
            out.close()


class AnalyzeTranslations(ParallelPipe):
    def __init__(self,
                 components,
                 source_sgm,
                 bl_sgm,
                 sys_sgm,
                 ref_sgm,
                 bl_bleu,
                 sys_bleu,
                 main_output,
                 by_chrF1_output=None,
                 by_chrF2_output=None,
                 by_bleu_output=None,
                 by_delta_chrF1_output=None,
                 by_delta_chrF2_output=None,
                 by_delta_bleu_output=None,
                 by_delta_prod_output=None,
                 n_refs=1):
        """
        6 inputs:
          4 sgm files: source, bl, sys, (multi)ref
          2 mteval bleu outputs: bl, sys
        Components must take 5 or more columns
        where 4 first are (key, src, bl, sys) and rest are refs.
        Note that BLEUs are not passed through components.
        """
        main_inputs = [source_sgm, bl_sgm, sys_sgm, ref_sgm, bl_bleu, sys_bleu]
        main_outputs = [main_output, by_chrF1_output, by_chrF2_output, by_bleu_output,
                 by_delta_chrF1_output, by_delta_chrF2_output, by_delta_bleu_output, by_delta_prod_output]
        main_outputs = [x for x in main_outputs if not x is None]
        super().__init__(components, main_inputs, main_outputs)
        self.n_refs = n_refs
        self.key_order = []

        self.sorted_outputs = [
            ('sys_chrF1', by_chrF1_output),
            ('sys_chrF2', by_chrF2_output),
            ('sys_bleu', by_bleu_output),
            ('delta_chrF1', by_delta_chrF1_output),
            ('delta_chrF2', by_delta_chrF2_output),
            ('delta_bleu', by_delta_bleu_output),
            ('delta_prod', by_delta_prod_output)]
        self.keep_output = any(x is not None for x in
            (by_chrF1_output, by_chrF2_output, by_bleu_output,
             by_delta_chrF1_output, by_delta_chrF2_output, by_delta_bleu_output,
             by_delta_prod_output,))

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
        self.bl_bleus = {(seg.docid, seg.segid): seg.bleu
                         for seg in read_bleu(readers[4])}
        self.sys_bleus = {(seg.docid, seg.segid): seg.bleu
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

        lines_by_key = {}

        # Drain pipeline,
        for tpl in stream:
            # throw the output away, unless needed for sorted outputs
            if self.keep_output:
                lines_by_key[tpl[0]] = tpl

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

        def stats_tuples():
            for key in self.key_order:
                columns = []
                for component in components_with_fields:
                    columns.extend(component[key])
                columns.extend(extra_fields_func(key, columns))
                yield columns

        df = pd.DataFrame.from_records(stats_tuples(), columns=all_fields)
        # extra columns referencing other columns are easiest to build here
        if 'delta_chrF1' in all_fields:
            df['delta_prod'] = df['delta_chrF1'] * df['delta_bleu']

        df.to_csv(self.main_outputs[0](conf, cli_args), sep='\t', index=False)

        for (field, output) in self.sorted_outputs:
            if output is None:
                continue
            with output.open(conf, cli_args, mode='wb') as outfobj:
                sorted_df = df.sort_values(field)
                for tpl in sorted_df[['docid', 'segid', field]].itertuples():
                    key = (tpl.docid, tpl.segid)
                    val = tpl[-1]
                    lines = lines_by_key[key]
                    outfobj.write('DOC: "{}" SEG: "{}" {}: {}\n'.format(
                        key[0], key[1], field, val))
                    outfobj.write('SRC : {}\n'.format(lines[1]))
                    for (i, ref) in enumerate(lines[4:]):
                        outfobj.write('REF{}: {}\n'.format(i, ref))
                    outfobj.write('BL  : {}\n'.format(lines[2]))
                    outfobj.write('SYS : {}\n'.format(lines[3]))
                    outfobj.write('-' * 80)
                    outfobj.write('\n')

        # close side file objects
        for fobj in readers + list(side_fobjs.values()):
            fobj.close()

    def _extra_fields(self, all_fields):
        extra_fields = ['bl_bleu', 'sys_bleu', 'delta_bleu',
                        'docid', 'segid']

        def extra_fields_func(key, columns):
            bl_bleu = self.bl_bleus[key]
            sys_bleu = self.sys_bleus[key]
            delta_bleu = sys_bleu - bl_bleu
            result = [bl_bleu, sys_bleu, delta_bleu,
                      key[0], key[1]]
            return result
        return extra_fields, extra_fields_func



class AnalyzeChrF(ParallelPipeComponent):
    """Perform sentence level character-F bl-refs and sys-refs.
    This should be applied before retokenizing."""
    def __init__(self, side_inputs=None, side_outputs=None):
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)
        # (docid, segid) -> tpl
        self.scores = {}
        self.fields = (
            'bl_chrF1', 'bl_chrF2',
            'sys_chrF1', 'sys_chrF2',
            'delta_chrF1', 'delta_chrF2',
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

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for tpl in stream:
            key = tpl[0]
            bl = tpl[2]
            sys = tpl[3]
            refs = tpl[4:]
            bl_score1, bl_score2 = self._chrf_helper(bl, refs)
            sys_score1, sys_score2 = self._chrf_helper(sys, refs)
            self.scores[key] = (bl_score1, bl_score2,
                                sys_score1, sys_score2,
                                sys_score1 - bl_score1,
                                sys_score2 - bl_score2)
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
            if not RE_ALNUM.findall(token):
                # don't count punctuation
                continue
            if token == prev:
                conseq += 1
            if token in seen:
                anywhere += 1
            prev = token
            seen.add(token)
        return anywhere, conseq

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
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

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
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


class ReTrueCase(PerColumn):
    """Inputs are retokenized translations.
    Analysis that prefers truecased input should be
    preceded by this component."""
    def __init__(self, src_model, trg_model, n_refs=1):
        if src_model is not None:
            src_truecaser = TrueCase(src_model)
        else:
            src_truecaser = IdentityComponent()
        if trg_model is not None:
            trg_truecaser = TrueCase(trg_model)
        else:
            trg_truecaser = IdentityComponent()
        components = [IdentityComponent(),  # key
                      src_truecaser,        # src
                      trg_truecaser,        # bl
                      trg_truecaser,        # sys
                     ]
        for _ in range(n_refs):
            # all refs by target truecaser
            components.append(trg_truecaser)
        super().__init__(components)


class AnalyzeLetteredNames(ParallelPipeComponent):
    """Counts how many tokens in the source are eligible for lettering
    by components.LetterizeNames.
    This should be applied after retokenizing and retruecasing."""
    def __init__(self, side_inputs=None, side_outputs=None):
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)
        # (docid, segid) -> (count,)
        self.counts = {}
        self.fields = ('lnames',)

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for tpl in stream:
            key = tpl[0]
            src = tpl[1]
            count = sum(len(token) > 1 and (token[0].isupper() or token[0].isdigit())
                        for token in src.split())
            self.counts[key] = (count,)
            yield tpl

    def __getitem__(self, key):
        return self.counts[key]


