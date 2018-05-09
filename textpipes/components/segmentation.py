from .core import RegexSubstitution, ApplyLexicon, MonoPipeComponent

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


# FIXME: there are two different apply components (the BETTER other in preprocessing.py)
class ApplySegmentationLexicon(ApplyLexicon):
    """Substitutes words with a segmentation defined in a lexicon of the format
    WORD_COUNT <tab> MORPH_1 <space> MORPH_2 <space> ... MORPH_N

    This component can only add spaces: the surface form is the concatenation 
    of the segments.
    """
    def pre_make(self, side_fobjs):
        fobj = side_fobjs[self.lexicon_file]
        for line in fobj:
            # throw away word count
            _, seg = line.strip().split('\t')
            # surface form is concatenation of segments
            word = seg.replace(' ', '')
            self.lexicon[word] = seg


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
