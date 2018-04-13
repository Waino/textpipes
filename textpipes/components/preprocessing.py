import logging
import unicodedata
import re

logger = logging.getLogger('textpipes')

FIVEDOT = '\u2059' # 5-dot punctuation
LETTERING_BEG = '\u2e2b' # v 3-dot
LETTERING_MID = '\u2e2c' # ^ 3-dot
LETTERING_END = '\u2e2d' # + 4-dot

MULTISPACE_RE = re.compile(r' +')

try:
    import ftfy
except ImportError:
    # warnings emitted by check in cli
    pass

from ..core.utils import read_lang_file
from .core import SingleCellComponent, RegexSubstitution, MonoPipeComponent

class Clean(SingleCellComponent):
    """Uses ftfy to perform a number of normalizations """
    def __init__(self, maintain_alignment=True, **kwargs):
        super().__init__(mp=False)  # not paralellizable
        self.params = {
            'fix_entities': True,     # ftfy default is 'auto'
            'remove_terminal_escapes': True,
            'fix_encoding': True,
            'fix_latin_ligatures': True,
            'fix_character_width': True,
            'uncurl_quotes': True,
            'fix_line_breaks': True,
            'fix_surrogates': True,
            'remove_control_chars': True,
            'remove_bom': True,
            'normalization': 'NFKC',   # ftfy default is 'NFC'
            'max_decode_length': 1000000}
        self.params.update(kwargs)
        self.maintain_alignment = maintain_alignment

    def single_cell(self, line):
        line = ftfy.fix_text(line, **self.params)
        if self.maintain_alignment:
            # remove intoduced extra newlines
            line = line.replace('\n', ' ')
        return line


class NormalizePunctuation(RegexSubstitution):
    def __init__(self, **kwargs):
        expressions = [
            ('[\u002d\u058a\u05be\u2011\u2012\u2013\u2014\u2015\u2e3a\u2e3b'
              '\u2212\ufe58\ufe63\uff0d\xad]', '-'),
            ('(?<=\w)[\u2018\u2019](?=\w)', "'"),   # clitics
            ('[`´]', "'"),
            ('[\u201e\u201c\u201d\u2018\u201a\u2019\u2039\u203a\u02ee'
              '\xab\xbb\u27ea\u27eb\u300a\u300b]', '"'),
            ("''", '"'),    # quote using apostrophes
            ('\u2044', '/'),
            ('\uff3f', '_'),
            ('\u066A', '%'),
            ('\u0609', '\u2030'),   # promille
            ]
        super().__init__(expressions, **kwargs)


# FIXME: use of fancy quotes is really inconsistent between corpora
class DeNormalizePunctuation(RegexSubstitution):
    def __init__(self, **kwargs):
        expressions = [
            # fancy quotes FIXME: some langs have assymmetric
            (r'"', '\u201d'),
            # fancy apostrophe
            (r"'", '\u2019'),
            ]
        super().__init__(expressions, **kwargs)


# FIXME: separate components to (de)normalize order of punctuation, if desired
        ## quotpunc normalizes english "foo." into internal standard "foo".
        ## but tries not to when comma comes, "before quoted phrase"
        ## use language-specific DeNormalizePunctuation to postprocess
        #if quotpunc:
        #    expressions.append((r'("[^"]+)([,\.])( ?)"', r'\1"\3\2'))
        ## FIXME: thousand seps?

        ## FIXME en-dash \u2013 (under what conditions?)
        ## FIXME: thousand seps?
        #if self.lang == 'en':
        #    self.expressions.extend((
        #        # quotpunc FIXME: make configurable?
        #        (r'"( ?)([,\.])', r'\2\1"'),
        #        ))

class MapChars(SingleCellComponent):
    """Character mangling based on unicode character category,
    with individual overrides.
    """
    def __init__(self, policies={}, overrides={}, maintain_alignment=True):
        super().__init__(mp=False)  # not paralellizable
        self._cache = dict()
        self.policies = {
            'Cc': 'drop',   # Other, Control
            'Cf': 'drop',   # Other, Format (note: arabic markers)
            'Cn': 'drop',   # Other, Not Assigned
            'Co': 'drop',   # Other, Private Use
            'Cs': 'drop',   # Other, Surrogate (normalize first)
            'LC': 'accept', # Letter, Cased
            'Ll': 'accept', # Letter, Lowercase
            'Lm': 'drop',   # Letter, Modifier (normalize aggressively?)
            'Lo': 'accept', # Letter, Other (you may want whitelist instead)
            'Lt': 'accept', # Letter, Titlecase
            'Lu': 'accept', # Letter, Uppercase
            'Mc': 'drop',   # Mark, Spacing Combining
            'Me': 'drop',   # Mark, Enclosing
            'Mn': 'drop',   # Mark, Nonspacing (normalize aggressively?)
            'Nd': 'accept', # Number, Decimal Digit (whitelist instead?)
            'Nl': 'drop',   # Number, Letter (note: roman numerals)
            'No': 'drop',   # Number, Other (note: fractions, super/sub)
            'Pc': 'drop',   # Punctuation, Connector
            'Pd': 'drop',   # Punctuation, Dash (normalize aggressively?)
            'Pe': 'drop',   # Punctuation, Close (normalize aggressively?)
            'Pf': 'drop',   # Punctuation, Final quote (normalize aggressively)
            'Pi': 'drop',   # Punctuation, Initial quote (normalize aggressively)
            'Po': 'drop',   # Punctuation, Other (normalize aggressively)
            'Ps': 'drop',   # Punctuation, Open (normalize aggressively?)
            'Sc': 'accept', # Symbol, Currency
            'Sk': 'drop',   # Symbol, Modifier (normalize first)
            'Sm': 'accept', # Symbol, Math (whitelist instead?)
            'So': 'drop',   # Symbol, Other
            'Zl': 'drop',   # Separator, Line
            'Zp': 'drop',   # Separator, Paragraph
            'Zs': ' ',      # Separator, Space
            }
        self.policies.update(policies)
        self.overrides = {
            '\t': '\t',     # Cc
            '-': '-',       # Pd
            ')': ')',       # Pe
            '}': '}',       # Pe
            ']': ']',       # Pe
            '(': '(',       # Ps
            '{': '{',       # Ps
            '[': '[',       # Ps
            '!': '!',       # Po
            '"': '"',       # Po
            '#': '#',       # Po
            '%': '%',       # Po
            '&': '&',       # Po
            "'": "'",       # Po
            '*': '*',       # Po
            ',': ',',       # Po
            '.': '.',       # Po
            '/': '/',       # Po
            ':': ':',       # Po
            ';': ';',       # Po
            '?': '?',       # Po
            '@': '@',       # Po
            '\\': '\\',     # Po
            '§': '§',       # Po
            '\u2030': '\u2030',     # Po, promille
            '\xa9': '\xa9',         # So, (c)
            '\xae': '\xae',         # So, (r)
            '\xb0': '\xb0',         # So, degrees
            '\u2122': '\u2122',     # So, tm
            ' ': ' ',       # Zs
        }
        self.overrides.update(overrides)
        self.maintain_alignment = maintain_alignment

    def single_cell(self, line):
        result = []
        for char in line:
            if char not in self._cache:
                self._cache[char] = self._decide(char)
            result.append(self._cache[char])
        result = ''.join(result)
        if self.maintain_alignment:
            # remove intoduced extra newlines
            result = result.replace('\n', ' ')
        return result

    def _decide(self, char):
        if char in self.overrides:
            return self.overrides[char]
        # implicit else: use default policy for category
        category = unicodedata.category(char)
        if category not in self.policies:
            # nothing specified: default to drop
            return ''
        policy = self.policies[category]
        if policy == 'drop':
            return ''
        elif policy == 'accept':
            return char
        else:
            return policy


class StripRareChars(SingleCellComponent):
    def __init__(self, char_counts, min_count=10, **kwargs):
        super().__init__(side_inputs=[char_counts], **kwargs)
        self.char_counts = char_counts
        self.min_count = min_count
        self.keep = set()

    def pre_make(self, side_fobjs):
        for line in side_fobjs[self.char_counts]:
            count, char = line.lstrip().rstrip('\n').split('\t')
            count = int(count)
            if count >= self.min_count:
                self.keep.add(char)

    def single_cell(self, line):
        result = []
        for char in line:
            if char not in self.keep:
                continue
            result.append(char)
        return ''.join(result)


class LetterizeNames(SingleCellComponent):
    """Segment tokens starting with capital or digit into chars"""
    def single_cell(self, line):
        out = []
        for token in line.split():
            if FIVEDOT in token:
                out.append(token)
                continue
            if len(token) > 1 and (token[0].isupper() or token[0].isdigit()):
                chars = [char for char in token]

                firstmarked = LETTERING_BEG + chars.pop(0)
                lastmarked = LETTERING_END + chars.pop(-1)
                midmarked = [LETTERING_MID + char for char in chars]
                marked = [firstmarked] + midmarked + [lastmarked]
                chars = ' '.join(marked)

                out.append(chars)
                continue
            out.append(token)
        return ' '.join(out)


class StripXml(MonoPipeComponent):
    def __init__(self, filter_blanks=False):
        super().__init__()
        self.filter_blanks = filter_blanks

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        depth = 0
        for line in stream:
            result = []
            for char in line:
                if char == '<':
                    depth += 1
                elif char == '>':
                    depth = max(0, depth - 1)
                elif depth == 0:
                    result.append(char)
                # else throw away
            result = MULTISPACE_RE.sub(' ', ''.join(result))
            if not self.filter_blanks or len(result) > 0:
                yield result


class NormalizeContractions(RegexSubstitution):
    """replace contractions with normalized form"""

    def __init__(self, lang, **kwargs):
        contractions = [line.split('\t')
                        for line in read_lang_file('contractions', lang)]
        expressions = ((r'\b{}\b'.format(cont.replace("'", " ?' ?")), repl)
                       for (cont, repl) in contractions)
        super().__init__(expressions, ignore_case=True, **kwargs)


class ApplyMapping(MonoPipeComponent):
    def __init__(self, map_file, **kwargs):
        super().__init__(side_inputs=[map_file], **kwargs)
        self.map_file = map_file
        self.mapping = {}

    def pre_make(self, side_fobjs):
        for line in side_fobjs[self.map_file]:
            # mapping contains both source and target form
            # separated by tab
            src, tgt = line.split('\t')
            self.mapping[src] = tgt

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for line in stream:
            result = []
            for token in line.split():
                token = self.mapping.get(token, token)
                result.append(token)
            yield ' '.join(result)


# apply a segmentation
# FIXME: there are two different apply components (the other in segmentation.py)
class ApplySegmentation(ApplyMapping):
    def __init__(self, map_file, bnd_marker='@@ ', pre_marked=False, no_space_ok=False, **kwargs):
        super().__init__(map_file, **kwargs)
        self.bnd_marker = bnd_marker
        self.pre_marked = pre_marked
        assert no_space_ok or ' ' in self.bnd_marker

    def pre_make(self, side_fobjs):
        nonspace_marker = self.bnd_marker.replace(' ', '')
        for line in side_fobjs[self.map_file]:
            # only the segmented form is given
            parts = line.split()
            if self.pre_marked:
                # bnd_marker contains chars not part of actual surface form
                src = ''.join(parts).replace(nonspace_marker, '')
            else:
                src = ''.join(parts)
                line = self.bnd_marker.join(parts)
            self.mapping[src] = line


class SplitNumbers(RegexSubstitution):
    """ split number-punctuation sequences """
    # FIXME: also split long numbers into shorter chunks?
    def __init__(self, **kwargs):
        super().__init__([(r'(\d)([.,/-])(\d)', r'\1@@ \2@@ \3')], **kwargs)


class Prefix(SingleCellComponent):
    def __init__(self, prefix='', suffix='', **kwargs):
        super().__init__(**kwargs)
        self.prefix = prefix
        self.suffix = suffix

    def single_cell(self, line):
        return ''.join((self.prefix, line, self.suffix))
