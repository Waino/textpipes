from .core import RegexSubstitution, ApplyMapping, MonoPipeComponent
from ..core.utils import FIVEDOT

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
