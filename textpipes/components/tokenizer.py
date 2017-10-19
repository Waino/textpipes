import codecs
import re
import os
import logging

logger = logging.getLogger('textpipes')

from ..core.utils import read_lang_file
from .core import SingleCellComponent

MULTISPACE_RE = re.compile(r' +')
END_PERIOD_RE = re.compile(r'\.\s*$')
# punctuation that should be tokenized separately
TOK_PUNC_RE = re.compile(r'([\.,!?:;/@%\(\)\'"+£\$€])')

# Tokenization must be mostly reversible for use on target lang:
# not good to split hyphens here
# r'- \d',           # negative numbers (protected)
class Tokenize(SingleCellComponent):
    def __init__(self, lang, **kwargs):
        super().__init__(**kwargs)
        self.lang = lang
        # FIXME: customizable punctuation?
        self.punctuation_re = TOK_PUNC_RE
        protected_str = read_lang_file('nonbreaking_prefix', self.lang)
        # these are specified in the over-tokenized form
        # all contained spaces are removed
        protected_re = [
            # decimal and thousand separators, IPs (note: non-grouping)
            r'\d (?:[\.,] \d)+',
            #r'\d %',           # percentages
            r'\.(?: \.)+',      # multiple dots (note: non-grouping)
            # urls. stops at first original space (note: not rawstring)
            'https? : / / [^\u001F]*',
            # certain domains even if not urls
            r'(?:www \. )?[A-Za-z]* \. (?:com|cz|de|fi|org|ru|tr)',
            # emails. stops at first original space
            r'[a-z\. ]* @ [a-z\. ]*',
            ]
        map_re = [
            # Abbreviations protected if followed by a number.
            (r'(No|Art|pp) \.(\s+\d)', r"\1.\2"),
            # name clitics: O' D' L' (don't split at all)
            (r' ([ODL])\s+\' (?=\w)', r" \1'"),
            # decimals without leading zero: .38 caliber
            # must originally have a leading space
            ('(\u001F) ?\\. (\\d)', r'\1 .\2'),
            ]
        if self.lang == 'en':
            map_re.extend(
                ((r'(?<=\w) \'\s+([a-z]) ', r" '\1 "),    # english suffix clitics. FIXME: fr and others
                 (r'(?<=\w) \'\s+(ll|re|ve) ', r" '\1 "), # longer clitics: 'll 're 've
                 (r'(?<=\d) \'\s+s ', r" 's "),           # special case: 1990's
                ))
        elif self.lang == 'fi':
            protected_re.extend(
                (r'\d \. ',  # ordinals
                ))
            # not trying to correct if split in input
            map_re.extend(
                ((r'(?<=\w) : ([a-zåäö]{1,3}) ', r" :\1 "),    # finnish abbrevation suffixes
                 (r'(?<=\w) : (nneksi|ista) ', r" :\1 "),      # longer suffixes
                ))
        if self.lang not in ('en',):
            # languages without specific clitic rules
            map_re.extend(
                ((r'([A-Za-z][a-z][a-z]) \' ([a-z]) ', r"\1'\2 "), # join obvious clitics
                ))

        # process and compile expressions
        self.protected_str = self._compile_str(protected_str)
        self.protected_re = self._compile_re(protected_re)
        self.map_re = self._compile_map(map_re)

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
        elif self.lang == 'fi':
            # finnish abbrevation suffixes
            expressions.extend([
                (r' (:[a-zåäö]{1,3}) ', r"\1 "),
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
