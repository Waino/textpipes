import logging
import unicodedata

logger = logging.getLogger(__name__)

try:
    import ftfy
except ImportError:
    logger.warning('Unable to load ftfy.')
    logger.warning('You will not be able to use Clean.')

from .core import SingleCellComponent, RegexSubstitution

class Clean(SingleCellComponent):
    """Uses ftfy to perform a number of normalizations """
    def __init__(self, **kwargs):
        super().__init__()
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

    def single_cell(self, line, side_fobjs=None):
        return ftfy.fix_text(line, **self.params)


class NormalizePunctuation(RegexSubstitution):
    def __init__(self):
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
        super().__init__(expressions)


# FIXME: use of fancy quotes is really inconsistent between corpora
class DeNormalizePunctuation(RegexSubstitution):
    def __init__(self, node_in):
        self.expressions = [
            # fancy quotes FIXME: some langs have assymmetric
            (r'"', '\u201d'),
            # fancy apostrophe
            (r"'", '\u2019'),
            ]
        super().__init__(expressions)


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
    def __init__(self, policies={}, overrides={}):
        super().__init__()
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

    def single_cell(self, line, side_fobjs=None):
        result = []
        for char in line:
            if char not in self._cache:
                self._cache[char] = self._decide(char)
            result.append(self._cache[char])
        return ''.join(result)

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
