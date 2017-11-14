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

        self.re_proper = re.compile(r'\[PROPER=PROPER\]')
        self.re_num = re.compile(r'[0-9]')
        self.re_punc = re.compile(r'^[,\.-]+$')
        self.re_numpunc = re.compile(r'^[0-9,\.-]+$')

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
        return lemma

# remove unwanted tag categories
class FilterTags(SingleCellComponent):
    def __init__(self,
                 tags_col=3, sep='\t',
                 keep=('POS', 'NUM', 'CASE', 'PERS', 'MOOD', 'TENSE',),
                 #'PROPER',
                 **kwargs):
        super().__init__(**kwargs)
        self.tags_col = tags_col
        self.sep = sep
        self.keep = keep

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
                 bnd_marker='@@', bies=True, **kwargs):
        super().__init__(side_inputs=[map_file], **kwargs)
        self.map_file = map_file
        self.col_i = col_i
        self.col_sep = col_sep
        self.bnd_marker = bnd_marker
        self.bies = bies
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
                cols[self.col_i] = subword
                if self.bies:
                    cols[-1] = bies_tag
                yield self.col_sep.join(cols)

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
