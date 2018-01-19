import itertools
import subprocess
import re

from .core.recipe import Rule
from .core.platform import run
from .components.core import MonoPipeComponent, SingleCellComponent

# flatten sentences into single-column (surface) tabular representation
class SingleSurfaceColumn(MonoPipeComponent):
    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for line in stream:
            for token in line.split():
                yield token
            # empty line separates sentences
            yield ''

class Finnpos(Rule):
    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        # FIXME: would be much better if this would fail in --check
        assert not infile.endswith('.gz')
        assert not outfile.endswith('.gz')
        subprocess.check_call(
            ['ftb-label < {infile} > {outfile}'.format(
                infile=infile,
                outfile=outfile)
            ], shell=True)

# deterministic lemma modification
class ModifyLemmas(SingleCellComponent):
    def __init__(self,
                 lemma_col=2, tags_col=3, sep='\t',
                 number_tag='<NUM>',
                 proper_tag='<PROPER>',
                 hyphen_compounds=True,
                 strip_numbers=True,
                 strip_hyphens=True,
                 strip_junk=True,
                 collapse_repeats=True,
                 **kwargs):
        super().__init__(**kwargs)
        self.lemma_col = lemma_col
        self.tags_col = tags_col
        self.sep = sep

        self.number_tag = number_tag
        self.proper_tag = proper_tag
        self.hyphen_compounds = hyphen_compounds
        self.strip_numbers = strip_numbers
        self.strip_hyphens = strip_hyphens
        self.strip_junk = strip_junk
        self.collapse_repeats = collapse_repeats

        self.re_proper = re.compile(r'\[PROPER=PROPER\]')
        self.re_num = re.compile(r'[0-9]')
        self.re_punc = re.compile(r'^[,\.-]+$')
        self.re_numpunc = re.compile(r'^[0-9,\.-]+$')
        self.re_repeats = re.compile(r'(.)\1\1*')
        self.junk = ".,[]<>()@'#*-"

    def single_cell(self, line):
        if len(line) == 0:
            return line
        cols = line.split(self.sep)
        lemma = cols[self.lemma_col]
        tags = cols[self.tags_col]
        lemma = self._modify(lemma)
        if self.proper_tag and self.re_proper.findall(tags):
            # collapse if tagged as proper name
            lemma = self.proper_tag

        cols[self.lemma_col] = lemma
        return self.sep.join(cols)

    def _modify(self, lemma):
        if lemma in (self.number_tag, self.proper_tag)
            return lemma
        ## numbers and punctuation
        # pure punctuation unchanged
        if self.re_punc.match(lemma):
            return lemma
        if self.number_tag and self.re_numpunc.match(lemma):
            # collapse if only numbers and punctuation
            return self.number_tag
        if self.strip_numbers:
            # otherwise remove the numbers
            lemma = self.re_num.sub('', lemma)

        ## hyphens and hyphen compounds
        if self.strip_hyphens:
            # strip leading and trailing hyphens
            lemma = lemma.strip('-')
        if self.hyphen_compounds:
            # internal hyphens: keep last part
            lemma = lemma.split('-')[-1]
        if self.strip_junk:
            lemma = lemma.strip(self.junk)
        if self.collapse_repeats:
            lemma = self.re_repeats.sub(r'\1\1', lemma)
        return lemma

class SplitLemmas(MonoPipeComponent):
    def __init__(self,
                 lang,
                 min_len=5,
                 seed_prefixes='finnpos_mislemma_prefix',
                 strip_suffixes='finnpos_mislemma_suffix',
                 **kwargs):
        super().__init__(**kwargs)
        self.min_len = min_len
        if seed_prefixes is not None:
            self.seed_prefixes = utils.read_lang_file(seed_prefixes, lang)
        else:
            self.seed_prefixes = []
        if strip_suffixes is not None:
            self.strip_suffixes = utils.read_lang_file(strip_suffixes, lang)
        else:
            self.strip_suffixes = []

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        seen = set(self.seed_prefixes)
        for line in stream:
            line = line.strip()
            if len(line) == 0:
                continue
            _, lemma = line.split()

            parts = self.split(lemma, seen)
            if parts is None:
                parts = [lemma]
            for part in parts:
                if len(part) >= self.min_len:
                    seen.add(part)
            yield '{}\t{}'.format(lemma, ' '.join(parts))

    def split(self, lemma, seen):
        if lemma in seen:
            return [lemma]
        for trunc in self.simplify(lemma):
            if trunc in seen:
                return [trunc]
        for i in range(self.min_len, len(lemma) - self.min_len + 1):
            pre = lemma[:i]
            if pre not in seen:
                # first part must be a single seen part
                continue
            suf = split(lemma[i:], seen)
            if suf is None:
                continue
            return [pre] + suf
        # no valid split found
        return None

    def simplify(self, lemma):
        for suffix in self.strip_suffixes:
            if lemma.endswith(suffix):
                return lemma[:-len(suffix)]

# remove unwanted tag categories
class FilterTags(SingleCellComponent):
    def __init__(self,
                 tags_col=3, sep='\t',
                 keep=('POS', 'NUM', 'CASE', 'PERS', 'MOOD', 'TENSE',),
                 #'PROPER',
                 mangle_fun=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.tags_col = tags_col
        self.sep = sep
        self.keep = keep
        self.mangle_fun = mangle_fun

        self.re_tag = re.compile(r'\[([A-Z]*)=.*')

    def single_cell(self, line):
        if len(line) == 0:
            return line
        cols = line.split(self.sep)
        tags = cols[self.tags_col]
        tags = self._modify(tags)

        cols[self.tags_col] = tags
        return self.sep.join(cols)

    def _modify(self, tags):
        tags = tags.split('|')
        tags = {self._tag_cat(x): x for x in tags}
        if self.mangle_fun is not None:
            tags = self.mangle_fun(tags)
        result = [tags[key] for key in self.keep if key in tags]
        return '|'.join(result)

    def _tag_cat(self, tag):
        m = self.re_tag.match(tag)
        if m:
            return m.group(1)
        return tag

# extract and unflatten a single field
class ExtractColumn(MonoPipeComponent):
    def __init__(self, col_i, sep='\t', **kwargs):
        super().__init__(**kwargs)
        self.col_i = col_i
        self.sep = sep

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        result = []
        # in case the file lacks the final sentence break
        stream = itertools.chain(stream, [''])
        for line in stream:
            if len(line) == 0:
                # empty line separates sentences
                if len(result) == 0:
                    # ignore double empty line
                    continue
                yield ' '.join(result)
                result = []
                continue
            cols = line.split(self.sep)
            result.append(cols[self.col_i])

# cluster e.g. lemmas
class Word2VecCluster(Rule):
    def __init__(self, *args, dims=300, clusters=10000, **kwargs):
        super().__init__(*args, **kwargs)
        self.dims = dims
        self.clusters = clusters

    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        # FIXME: would be much better if this would fail in --check
        assert not infile.endswith('.gz')
        assert not outfile.endswith('.gz')
        run('word2vec -train {infile} -output {outfile}'
            ' -size {dims} -classes {clusters}'.format(
                infile=infile,
                outfile=outfile,
                dims=self.dims,
                clusters=self.clusters))

class MapColumn(SingleCellComponent):
    """Applies a mapping table to values in a column."""
    def __init__(self, map_file, col_i, sep='\t', unk=False, **kwargs):
        super().__init__(side_inputs=[map_file], **kwargs)
        self.map_file = map_file
        self.col_i = col_i
        self.sep = sep
        self.unk = unk
        self.mapping = {}

    def pre_make(self, side_fobjs):
        for line in side_fobjs[self.map_file]:
            src, tgt = line.strip().split()
            self.mapping[src] = tgt

    def single_cell(self, line):
        if len(line) == 0:
            return line
        cols = line.split(self.sep)
        val = cols[self.col_i]
        default = self.unk if self.unk else val
        val = self.mapping.get(val, default)

        cols[self.col_i] = val
        return self.sep.join(cols)

class ApplyClusteringToColumn(MapColumn):
    """Applies a clustering to values in a column.
    The cluster file contains an arbitrary cluster label,
    but in the output the label is replaced by
    the first seen example from the cluster."""
    def __init__(self, map_file, col_i, sep='\t', unk=False, **kwargs):
        super().__init__(map_file, col_i, sep=sep, unk=unk, **kwargs)
        self.cluster_labels = {}

    def pre_make(self, side_fobjs):
        for line in side_fobjs[self.map_file]:
            src, cluster_idx = line.strip().split()
            if cluster_idx not in self.cluster_labels:
                self.cluster_labels[cluster_idx] = src
            example = self.cluster_labels[cluster_idx]
            self.mapping[src] = example


# mangle fields into (src-marked, full-tags, surface)
# https://github.com/franckbrl/bilingual_morph_normalizer
#class BurlotYvon
# many steps required:
# 1) fastalign
# 2) reformat: sentence per line, tab between words, space between fields (split tags into individual fields)
# 3) train
# 4) apply
# 5) reformat back to tabular

# learn a BPE segmentation
# and apply it to just the words, to get a map_file for SegmentColumn

# apply a segmentation, copy tags to each component
class SegmentColumn(MonoPipeComponent):
    def __init__(self, map_file, col_i, col_sep='\t',
                 bnd_marker='@@', bies=True, replace=None, **kwargs):
        super().__init__(side_inputs=[map_file], **kwargs)
        self.map_file = map_file
        self.col_i = col_i
        self.col_sep = col_sep
        self.bnd_marker = bnd_marker
        self.bies = bies
        self.mapping = {}
        self.replace = replace

    def pre_make(self, side_fobjs):
        for line in side_fobjs[self.map_file]:
            tgt = line.split()
            # bnd_marker not part of actual surface form
            src = ''.join(tgt).replace(self.bnd_marker, '')
            self.mapping[src] = tgt

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        result = []
        for line in stream:
            if len(line) == 0:
                yield line
                continue
            cols = line.split(self.col_sep)
            val = cols[self.col_i]
            val = self.mapping.get(val, [val])
            if len(val) == 1:
                bies_tags = 'S'
            else:
                bies_tags = 'B' + ('I' * (len(val) - 2)) + 'E'
            if self.bies:
                cols.append('')
            for (subword, bies_tag) in zip(val, bies_tags):
                tmp = list(cols)
                tmp[self.col_i] = subword
                if self.bies:
                    tmp[-1] = bies_tag
                if self.replace is not None and bies_tag not in 'ES':
                    # don't replace last
                    col, repl = self.replace
                    tmp[col] = repl
                yield self.col_sep.join(tmp)

# apply a segmentation, interleave tags and flatten
class Interleave(MonoPipeComponent):
    def __init__(self, map_file, col_i, col_sep='\t',
                 bnd_marker='@@', aux_marker='%%', keep=(2,3,0), **kwargs):
        super().__init__(side_inputs=[map_file], **kwargs)
        self.map_file = map_file
        self.col_i = col_i
        self.col_sep = col_sep
        self.bnd_marker = bnd_marker
        self.aux_marker = aux_marker
        self.keep = keep
        self.mapping = {}

    def pre_make(self, side_fobjs):
        for line in side_fobjs[self.map_file]:
            tgt = line.split()
            # bnd_marker not part of actual surface form
            src = ''.join(tgt).replace(self.bnd_marker, '')
            self.mapping[src] = tgt

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        result = []
        for line in stream:
            line = line.strip()
            if len(line) == 0:
                yield ' '.join(result)
                result = []
                continue
            cols = line.split(self.col_sep)
            val = cols[self.col_i]
            val = self.mapping.get(val, [val])
            for i in self.keep:
                if i == self.col_i:
                    result.extend(val)
                else:
                    result.append(self.aux_marker + cols[i])


class OnlyFirstSubword(MonoPipeComponent):
    def __init__(self, col_i, col_sep='\t', bnd_marker='@@', **kwargs):
        super().__init__(**kwargs)
        self.col_i = col_i
        self.col_sep = col_sep
        self.bnd_marker = bnd_marker

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        suppress = False
        for line in stream:
            line = line.strip()
            if len(line) == 0:
                yield line
                suppress = False
                continue
            if not suppress:
                yield line
            cols = line.split(self.col_sep)
            val = cols[self.col_i]
            suppress = self.bnd_marker in val
