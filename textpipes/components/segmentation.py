from .core import RegexSubstitution, ApplyMapping, MonoPipeComponent, SingleCellComponent
from ..core.utils import FIVEDOT, FOURDOT

class SplitHyphens(RegexSubstitution):
    def __init__(self, before=' ', after=' ', **kwargs):
        """Splits hyphens by inserting a boundary string before and
        after the hyphen. The strings can be empty, or more than one character,
        so e.g. before='', after=' @' results in
        'foo-bar' -> 'foo- @bar'.
        """
        repl = '{}-{}'.format(before, after)
        # not preceded by: space or other hyphen
        # not followed by: space, number or other hyphen
        super().__init__([(r'(?<![-\s])-(?![-\d\s])', repl)], **kwargs)


class StrictSplitHyphens(RegexSubstitution):
    def __init__(self, before=' ', after=' ', **kwargs):
        """Splits hyphens by inserting a boundary string before and
        after the hyphen. The strings can be empty, or more than one character,
        so e.g. before='', after=' @' results in
        'foo-bar' -> 'foo- @bar'.
        """
        repl = '{}-{}'.format(before, after)
        # preceded and followed by: at least 4 alphabetic chars
        super().__init__([(r'(?<=[a-z]{4})-(?=[a-z]{4})', repl)], ignore_case=True, **kwargs)


class SplitAllHyphens(RegexSubstitution):
    def __init__(self, before=' ', after=' ', **kwargs):
        """Splits hyphens by inserting a boundary string before and
        after the hyphen. The strings can be empty, or more than one character,
        so e.g. before='', after=' @' results in
        'foo-bar' -> 'foo- @bar'.
        """
        repl = '{}-{}'.format(before, after)
        # not preceded by: space
        # not followed by: space
        super().__init__([(r'(?<![\s])-(?![\s])', repl)], **kwargs)


class SplitDecimal(RegexSubstitution):
    def __init__(self, before=' ', after=' ', **kwargs):
        """Splits hyphens by inserting a boundary string before and
        after the hyphen. The strings can be empty, or more than one character,
        so e.g. before='', after=' @' results in
        'foo-bar' -> 'foo- @bar'.
        """
        repl = r'\1{}\2{}\3'.format(before, after)
        super().__init__([(r'([0-9])([,\.])([0-9])', repl)], **kwargs)


class ApplySegmentation(ApplyMapping):
    """Substitutes words with a segmented form.

    The map_file should contain a single column with the segmented form.
    Optionally the segmentation can be pre-marked with boundary markers.
    If this is the case, specify the pre_marked flag.
    The corpus can also contain pre-existing boundary markers,
    which are stripped before performing the lookup, and readded after.
    """
    def __init__(self, map_file, bnd_marker=FIVEDOT+' ',
                 pre_marked=False, no_space_ok=False, **kwargs):
        super().__init__(map_file, **kwargs)
        self.bnd_marker = bnd_marker
        self.nonspace_marker = self.bnd_marker.replace(' ', '')
        self.pre_marked = pre_marked
        assert no_space_ok or ' ' in self.bnd_marker

    def pre_make(self, side_fobjs):
        for line in side_fobjs[self.map_file]:
            # don't create empty parts
            line = line.strip(' ')
            # only the segmented form is given
            parts = line.split()
            if self.pre_marked:
                # bnd_marker contains chars not part of actual surface form
                src = ''.join(parts).replace(self.nonspace_marker, '')
            else:
                src = ''.join(parts)
                line = self.bnd_marker.join(parts)
            self.mapping[src] = line

    def lookup_mapping(self, src):
        leading = src.startswith(self.nonspace_marker)
        trailing = src.endswith(self.nonspace_marker)
        stripped = src.strip(self.nonspace_marker)
        trg = super().lookup_mapping(stripped)
        return '{}{}{}'.format(
            self.nonspace_marker if leading else '',
            trg,
            self.nonspace_marker if trailing else '')


# main input is word file, output is just missing words
class MissingSegmentations(MonoPipeComponent):
    def __init__(self, map_file, **kwargs):
        super().__init__(side_inputs=[map_file], **kwargs)
        self.map_file = map_file
        self.in_map = set()
        self.missing = set()

    def pre_make(self, side_fobjs):
        for line in side_fobjs[self.map_file]:
            # mapping contains only target form
            morphs = line.split()
            src = ''.join(morphs)
            self.in_map.add(src)

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for word in stream:
           if word not in self.in_map: 
                if not word in self.missing:
                    # only yield once
                    yield word
                self.missing.add(word)


# only space as bnd_marker
class MappingToSegmentation(SingleCellComponent):
    require_match = set('abcdefghijklmnopqrstuvxyzåäö')

    def __init__(self, normalize, non_concatenative, postp=None, **kwargs):
        self.normalize = normalize
        self.non_concatenative = non_concatenative
        self._nconc_tmp = []
        self.postp = postp
        super().__init__(side_outputs=[non_concatenative], **kwargs)

    def single_cell(self, line):
        src, tgt = line.rstrip('\n').split('\t', 1)
        tgt_norm = self.normalize(src, tgt)
        tgt_conc = tgt_norm.replace(' ', '')
        if tgt_conc != src:
            self._nconc_tmp.append((src, tgt, tgt_norm))
            # src atleast concatenates to src
            return src
        if self.postp is not None:
            tgt_norm = self.postp(tgt_norm)
        return tgt_norm

    def post_make(self, side_fobjs):
        non_concatenative = side_fobjs[self.non_concatenative]
        for tpl in self._nconc_tmp:
            non_concatenative.write('\t'.join(tpl))
            non_concatenative.write('\n')

    @classmethod
    def omorfi_normalize(cls, src, tgt):
        out = []
        i = 0
        j = 0
        src_lower = src.lower()
        tgt = tgt.lower()
        while i < len(src):
            if j >= len(tgt):
                # trailing removed
                out.append(src[i])
                i += 1
                continue
            if src_lower[i] == tgt[j]:
                # happy path: matching char
                out.append(src[i])
                i += 1
                j += 1
                continue
            if tgt[j] == ' ':
                # happy path: boundary
                out.append(' ')
                j += 1
                continue
            src_req = src_lower[i] in cls.require_match
            tgt_req = tgt[j] in cls.require_match
            if src_req and tgt_req:
                # failure to normalize
                return tgt
            if src_req:
                # skip over added noise
                j += 1
                continue
            if tgt_req:
                # re-add removed stuff
                out.append(src[i])
                i += 1
                continue
            # assume mapped char
            out.append(src[i])
            i += 1
            j += 1
        return ''.join(out)

class CharSegmentation(SingleCellComponent):
    def __init__(self, bnd_marker=None, space_marker=FOURDOT, **kwargs):
        self.bnd_marker = bnd_marker
        self.space_marker = space_marker
        super().__init__(**kwargs)

    def single_cell(self, line):
        if self.bnd_marker is not None:
            # detokenize
            line = line.replace(' ' + self.bnd_marker + ' ', '')
            line = line.replace(self.bnd_marker + ' ', '')
            line = line.replace(' ' + self.bnd_marker, '')
        line = line.replace(' ', self.space_marker)
        return ' '.join(list(line))
