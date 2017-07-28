from .core import RegexSubstitution, ApplyLexicon

class SplitHyphens(RegexSubstitution):
    def __init__(self, before=' ', after=' '):
        """Splits hyphens by inserting a boundary string before and
        after the hyphen. The strings can be empty, or more than one character,
        so e.g. before='', after=' @' results in 
        'foo-bar' -> 'foo- @bar'.
        """
        repl = '{}-{}'.format(before, after)
        # not preceded by: space or other hyphen
        # not followed by: space, number or other hyphen
        super().__init__([(r'(?<![-\s])-(?![-\d\s])', repl)])


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
