import codecs
import re
import os
import logging

logger = logging.getLogger(__name__)

from .core import SingleCellComponent

ALNUM_RE = re.compile('\w', flags=re.UNICODE)
# punctuation that is followed by uppercase
# note that this is a smaller set than tokenizer punctuation
# FIXME: excl and quest not if followed by closing quote or paren
# (hard to implement, as matching is done by token
CASE_PUNC_RE = re.compile(r'^([\.!?:])$')   # FIXME: semicolon?

# FIXME: use package resources instead
LANG_DIR = os.path.join(
    os.path.dirname(__file__), 'langs')

class TrainTrueCaser(SingleCellComponent):
    def __init__(self, model_file, sure_thresh=.6)
        # sure_thresh: truecase also within sentence if common enough
        super().__init__(side_outputs=[model_file])
        self.model_file = model_file
        self.sure_thresh = sure_thresh
        self.counts = collections.defaultdict(collections.Counter)

    def single_cell(self, sentence):
        seen_first = False
        for token in sentence.split():
            if not seen_first:
                # skip fully nonalnum tokens in beginning
                if ALNUM_RE.search(token):
                    seen_first = True
                continue
            elif self.punctuation_re.match(token):
                seen_first = False
            lower = token.lower()
            self.counts[lower][token] += 1
            # FIXME: also count prefixes with reduced weight?

    def save(self):
        self.words = dict()
        for (word, counts) in self.counts.items():
            # FIXME: use prefix counts
            total = sum(counts.values())
            best, bestcount = counts.most_common(1)[0]
            sure = (float(bestcount) / total) > self.sure_thresh
            self.words[word] = (best, sure)
        # yield model serialized into rows
        # FIXME: write into file? requires handing down conf, cli_args
        for (word, (best, sure)) in self.words.items():
            yield (word, best, str(sure))


class TrueCase(SingleCellComponent):
    def __init__(self, model_file,
                 titlecase_thresh=.5, lc_unseen_first=False):
        # sure_thresh: truecase also within sentence if common enough
        super().__init__(side_inputs=[model_file])
        self.model_file = model_file
        self.titlecase_thresh = titlecase_thresh
        self.lc_unseen_first = lc_unseen_first
        self.words = None
        # punctuation that resets seen_first
        # note that this is a smaller set than tokenizer punctuation
        self.punctuation_re = CASE_PUNC_RE

    def load(self):
        self.counts = None
        self.words = dict()
        for (i, tpl) in enumerate(self.persist_file.read()):
            try:
                (word, best, sure) = tpl
                sure = (sure == 'True')
            except ValueError:
                raise Exception(
                    'Unable to load truecaser line {} "{}"'.format(i, tpl))
            self.words[word] = (best, sure)
        return None

    def single_cell(self, sentence):
        result = []
        seen_first = False
        tokens = sentence.split()
        lowered = [token.lower() for token in tokens]
        n_cased = sum(token != lower
                      for (token, lower)
                      in zip(tokens, lowered))
        # FIXME: titlecase: don't count lang-specific always-lower words
        # FIXME: ignore very short sentences
        n_maybetitle = sum(1 for token in tokens
                           if ALNUM_RE.search(token))
        aggressive = (
            n_cased == 0    # irc-case
            or float(n_cased) / n_maybetitle > self.titlecase_thresh)
        for (token, lower) in zip(tokens, lowered):
            if lower in self.words:
                best, sure = self.words[lower]
                if sure and (aggressive or not seen_first):
                    # truecase the first token
                    # truecase also within sentence if abnormal case
                    truecased = best
                else:
                    # if not sure, don't change the case
                    truecased = token
            else:
                if (not seen_first) and self.lc_unseen_first:
                    # lowercase unseen first words
                    truecased = lower
                else:
                    # if not sure, don't change the case
                    truecased = token
            result.append(truecased)
            if not seen_first and ALNUM_RE.search(token):
                seen_first = True
            elif self.punctuation_re.match(token):
                seen_first = False
        return ' '.join(result)


# FIXME: detruecase: MWE:s
# FIXME: using LM, alignment to source, ...
class DeTrueCase(SingleCellOperation):
    def __init__(self):
        super().__init__()
        #self.lang = node_in.column_tags.get_joined('lang')
        # FIXME: customizable punctuation?
        self.punctuation_re = CASE_PUNC_RE

    def single_cell(self, sentence, tags):
        result = []
        seen_first = False
        tokens = sentence.split()
        for token in tokens:
            if not seen_first:
                if ALNUM_RE.search(token):
                    token = self._uc_first(token)
                    seen_first = True
            elif self.punctuation_re.match(token):
                seen_first = False
            result.append(token)
        return ' '.join(result)

    def _uc_first(self, token):
        # can't use capitalize(), don't want to lower rest
        return token[0].upper() + token[1:]
