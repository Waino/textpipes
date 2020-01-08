import codecs
import re
import os
import logging

logger = logging.getLogger('textpipes')

from ..core.utils import read_lang_file, FIVEDOT
from .core import SingleCellComponent, RegexSubstitution

# ### pyonmttok tokenizer
class OnmtTokenize(SingleCellComponent):
    """Tokenizer from pyonmttok.
    Similar to SimpleTokenize.
    Supports a large number of alphabets."""
    def __init__(self,
                 bnd_marker=FIVEDOT,
                 aggressive=True,
                 joiner_annotate=True,
                 spacer_annotate=False,
                 segment_alphabet_change=True,
                 **kwargs):
        super().__init__(**kwargs)
        self.bnd_marker = bnd_marker
        self.aggressive = aggressive
        self.joiner_annotate = joiner_annotate
        self.spacer_annotate = spacer_annotate
        self.segment_alphabet_change = segment_alphabet_change
        self.add_opt_dep('pyonmttok', binary=False)
        self.tok = None

    def pre_make(self, side_fobjs):
        import pyonmttok
        self.tok = pyonmttok.Tokenizer(
            'aggressive' if self.aggressive else 'conservative',
            joiner=self.bnd_marker,
            joiner_annotate=self.joiner_annotate,
            spacer_annotate=self.spacer_annotate,
            segment_alphabet_change=self.segment_alphabet_change)

    def single_cell(self, sentence):
        tokens, feats = self.tok.tokenize(sentence)
        # case feats currently not supported
        return ' '.join(tokens)


class OnmtDeTokenize(SingleCellComponent):
    def __init__(self, tokenizer, **kwargs):
        super().__init__(**kwargs)
        self.tokenizer = tokenizer
        self.add_opt_dep('pyonmttok', binary=False)

    def pre_make(self, side_fobjs):
        if self.tokenizer.tok is None:
            self.tokenizer.pre_make(side_fobjs)

    def single_cell(self, sentence):
        return self.tokenizer.tok.detokenize(sentence.split(' '))


# ### Simple tokenizer
class SimpleTokenize(SingleCellComponent):
    """Simple tokenizer relying on there being a subword segmentation
    step later on in the preprocessing, and detokenizing by joining
    these subwords.

    The reasons for applying this tokenizer
    before the actual segmentation are:
    1) Truecasing and other token-based preprocessing steps
    2) Reduce the noise burden of the subword segmentation
    """
    def __init__(self,
                 punc='-.,!?:;/\\@%()\'"+£$€¥',
                 bnd_marker=FIVEDOT,
                 **kwargs):
        super().__init__(**kwargs)
        self.punc = set(punc)
        self.bnd_marker = bnd_marker
        assert ' ' not in self.bnd_marker

    def single_cell(self, sentence):
        result = []
        chars = [None] + list(sentence) + [None]
        for prev, char, nxt in zip(chars, chars[1:], chars[2:]):
            result.append(self._char(prev, char, nxt))
        return ''.join(result)

    def _char(self, prev, char, nxt):
        if char not in self.punc:
            # if it isn't punctuation, don't do anything
            return char
        # IMPLICIT: current char is punctuation
        if char == prev:
            # don't split a repeating punc sequence
            return char
        if prev is None or prev == ' ':
            prefix = '' 
        else:
            prefix = ' ' + self.bnd_marker
        if nxt is None or nxt == ' ' or nxt in self.punc:
            suffix = '' 
        else:
            suffix = self.bnd_marker + ' '
        return prefix + char + suffix


class SimpleDeTokenize(RegexSubstitution):
    def __init__(self, bnd_marker=FIVEDOT):
        super().__init__([(bnd_marker + ' ', ''),
                          (' ' + bnd_marker, ''),
                          (bnd_marker, '')])


# ### Complicated tokenizer

MULTISPACE_RE = re.compile(r' +')
END_PERIOD_RE = re.compile(r'\.\s*$')

# punctuation that should be tokenized separately
TOK_PUNC_RE = re.compile(r'([\.,!?:;/@%\(\)\'"+£\$€])')


# Tokenization must be mostly reversible for use on target lang:
# not good to split hyphens here
# r'- \d',           # negative numbers (protected)
class Tokenize(SingleCellComponent):
    """
    Complicated tokenizer that tries to leave certain patterns whole.
    Might be useful for systems that benefit from not splitting too much:
    e.g. SMT and copy-mechanisms.
    """
    def __init__(self, lang, extra=None, escape_extra=True, **kwargs):
        side_inputs = []
        if extra:
            side_inputs.append(extra)
        super().__init__(side_inputs=side_inputs, **kwargs)
        self.lang = lang
        # FIXME: customizable punctuation?
        self.punctuation_re = TOK_PUNC_RE
        self.protected_str = read_lang_file('nonbreaking_prefix', self.lang)
        # these are specified in the over-tokenized form
        # all contained spaces are removed
        self.protected_re = [
            # decimal and thousand separators, IPs (note: non-grouping)
            r'\d (?:[\.,] \d)+',
            #r'\d %',           # percentages
            r'\.(?: \.)+',      # multiple dots (note: non-grouping)
            # urls. stops at first original space (note: not rawstring)
            'https? : / / [^\u001F]*',
            # certain domains even if not urls
            r'(?:www \. )?[A-Za-z]* \. (?:com|org|cz|de|fi|ee|ru|tr)',
            # emails. stops at first original space
            r'[a-z\. ]* @ [a-z\. ]*',
            ]
        self.map_re = [
            # Abbreviations protected if followed by a number.
            (r'(No|Art|pp) \.(\s+\d)', r"\1.\2"),
            # name clitics: O' D' L' (don't split at all)
            (r' ([ODL])\s+\' (?=\w)', r" \1'"),
            # decimals without leading zero: .38 caliber
            # must originally have a leading space
            ('(\u001F) ?\\. (\\d)', r'\1 .\2'),
            ]
        if self.lang == 'en':
            self.map_re.extend(
                ((r'(?<=\w) \'\s+([a-z]) ', r" '\1 "),    # english suffix clitics. FIXME: fr and others
                 (r'(?<=\w) \'\s+(ll|re|ve) ', r" '\1 "), # longer clitics: 'll 're 've
                 (r'(?<=\d) \'\s+s ', r" 's "),           # special case: 1990's
                ))
        elif self.lang in ('fi', 'et'):
            self.protected_re.extend(
                (r'\d \. ',  # ordinals
                ))
            # not trying to correct if split in input
            self.map_re.extend(
                ((r'(?<=\w) : ([a-zåäöõü]{1,3}) ', r" :\1 "),  # finnish abbrevation suffixes
                 (r'(?<=\w) : (nneksi|ista) ', r" :\1 "),      # longer suffixes
                ))
        if self.lang not in ('en',):
            # languages without specific clitic rules
            self.map_re.extend(
                ((r'([A-Za-z][a-z][a-z]) \' ([a-z]) ', r"\1'\2 "), # join obvious clitics
                ))
        self.extra = extra
        self.escape_extra = escape_extra

    def pre_make(self, side_fobjs):
        if self.extra:
            for line in side_fobjs[self.extra]:
                if self.escape_extra:
                    line = re.escape(line)
                self.protected_re.append(line)

        # process and compile expressions
        self.protected_str = self._compile_str(self.protected_str)
        self.protected_re = self._compile_re(self.protected_re)
        self.map_re = self._compile_map(self.map_re)

    def single_cell(self, sentence):
        out = sentence
        # mark original spaces
        out = out.replace(' ', ' \u001F ')
        out = ' ' + out + ' '
        out = self._split(out)
        # collapse multiple spaces
        out = MULTISPACE_RE.sub(' ', out)
        # recombine protected
        for (src, tgt) in self.protected_str:
            out = out.replace(src, tgt)
        for pattern in self.protected_re:
            for src in self._unique(pattern.findall(out)):
                tgt = src.replace(' ', '')
                out = out.replace(src, tgt)
        # do mappings
        for (src, tgt) in self.map_re:
            out = src.sub(tgt, out)
        # always split final period, even if abbrev.
        #out = END_PERIOD_RE.sub(' .', out)
        # remove original space markers
        out = out.replace('\u001F', '')
        # collapse multiple spaces
        out = MULTISPACE_RE.sub(' ', out)
        out = out.strip()
        return out

    def _split(self, string):
        return self.punctuation_re.sub(r' \1 ', string)

    def _compile_str(self, patterns):
        result = []
        for pattern in patterns:
            # final period is not included in file
            pattern += '.'
            # rstrip leaves leading space: not enough that end of token matches pattern
            result.append((' ' + self._split(pattern).rstrip(), ' ' + pattern))
        return result

    def _compile_re(self, patterns):
        return [re.compile(pattern, flags=re.UNICODE) for pattern in patterns]

    def _compile_map(self, patterns):
        return [(re.compile(pattern, flags=re.UNICODE), tgt)
                for (pattern, tgt) in patterns]

    def _unique(self, matches):
        """ Removes duplicate matches, and sorts from longer to shorter,
        to avoid partially replacing longer matches.
        """
        matches = set(matches)
        return sorted(matches, key=len, reverse=True)


class DeTokenize(SingleCellComponent):
    def __init__(self, lang, **kwargs):
        super().__init__(**kwargs)
        self.lang = lang
        expressions = [
            # collapse multispace
            (r' +', ' '),
        ]
        if self.lang == 'en':
            expressions.extend([
                # english suffix clitics
                (r' (\'[a-z]) ', r'\1 '),
                (r' (\'(ll|re|ve)) ', r'\1 '),
                ])
        elif self.lang in ('fi', 'et'):
            # finnish abbrevation suffixes
            expressions.extend([
                (r' (:[a-zåäöõü]{1,3}) ', r"\1 "),
                (r' (:(nneksi|ista)) ', r"\1 "),
                (r' \' (an|in|ista|hun|lla|lle|sin|sta|ssa|ta) ', r"'\1 "),
                (r'([Tt]ark) \' ', r"\1'"),
                ])
            # in finnish, would joining apos from both sides make sense?
        # must come after clitics
        expressions.extend([
            # plural apostrophe for s-ending 
            # (common in names also on non-en side)
            (r'(s) (\') ', r'\1\2 '),
            # colon joined from both sides if between numbers
            (r'(\d) : (\d)', r'\1:\2'),
            # join left
            (r' ([\.,!?:;\]\)])(?!\w)', r'\1'),
            # join right
            (r'([@\[\(]) ', r'\1'),
            # join both sides
            #(r' ([/]) ', r'\1'),
            # join currency symbol towards number
            (r'([\$£€]) ([-\.,]*\d)', r'\1\2'),
            (r'(\d[\.,]?) ([\$£€])', r'\1\2'),
            # join paired quotes inward
            (r'(") ([^"]+) (")', r'\1\2\3'),
            (r" (') ([^']+) (') ", r' \1\2\3 '),
            # unpaired starting/ending quote
            (r'^ (") ', r' \1'),
            (r' (") $', r'\1 '),
            # unpaired quote followed by punc joined right
            (r' ("[\.,]) ', r'\1 '),
            ])
        self.expressions = [(re.compile(exp, flags=re.UNICODE), repl)
                            for (exp, repl) in expressions]

    def single_cell(self, val):
        val = ' ' + val + ' '
        for (exp, repl) in self.expressions:
            val = exp.sub(repl, val)
        return val.strip()


# allow leading UNDER
NUMONLY_RE = re.compile(r'^▁?[0-9][0-9]*$')

class ForceTokenizeLongNumbers(SingleCellComponent):
    def __init__(self, min_len=4, tok_len=3, bnd_marker=FIVEDOT, **kwargs):
        super().__init__(**kwargs)
        self.min_len = min_len
        self.tok_len = tok_len
        self.bnd_marker = bnd_marker

    def single_cell(self, sentence):
        return ' '.join(self._token(token) for token in sentence.split())

    def _token(self, token):
        m = NUMONLY_RE.match(token)
        if not m:
            return token
        else:
            if len(token.replace('▁', '')) <= 4:
                return token
            return self.fseg(token)

    def fseg(self, token):
        token = token[::-1]
        out = []
        while len(token) > 3:
            out.append(token[:3] + self.bnd_marker)
            token = token[3:]
        out.append(token)
        return ' '.join(out)[::-1]
