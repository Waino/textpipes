import collections
import re

from .core import MonoPipeComponent, ParallelPipeComponent, PipeComponent, apply_component
from .preprocessing import Clean
from ..core.utils import safe_zip
from ..core.recipe import Rule, OptionalDep

# used for removal of nonalphabetic content for rough comparisons
# space and punc intentionally not included: tokenization invariant
FILTER_ALPHA = set('abcdefghijklmnopqrstuvwxyz')

RE_NUMPUNC = re.compile(r'^[0-9,\.-]+$')

def apply_filter(filtr, para=False, logfile=None, **kwargs):
    if para:
        component = ParallelFilter(filtr, logfile=logfile)
    else:
        component = MonoFilter(filtr, logfile=logfile)
    return apply_component(component, para=para, **kwargs)


class MonoFilter(MonoPipeComponent):
    def __init__(self, filtr, logfile=None):
        side_inputs = filtr.side_inputs
        side_outputs = [logfile] + filtr.side_outputs
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)
        self.filtr = filtr
        self.logfile = logfile

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        logfile = side_fobjs.get(self.logfile, None)
        for line in stream:
            if self.filtr(line, side_fobjs=side_fobjs):
                # filter out this line
                if logfile is not None:
                    # FIXME: not possible to log the reason
                    logfile.write(line)
                    logfile.write('\n')
            else:
                # keep this line
                yield line

    @property
    def opt_deps(self):
        return self.filtr.opt_deps


class ParallelFilter(ParallelPipeComponent):
    def __init__(self, filters, logfile=None):
        side_inputs = []
        side_outputs = [logfile]
        if isinstance(filters, Filter):
            side_inputs.extend(filters.side_inputs)
            side_outputs.extend(filters.side_outputs)
        else:
            for filtr in filters:
                side_inputs.extend(filtr.side_inputs)
                side_outputs.extend(filtr.side_outputs)
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)
        self.filters = filters
        self.logfile = logfile

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        filters = self.filters
        logfile = side_fobjs.get(self.logfile, None)
        for tpl in stream:
            if isinstance(filters, Filter):
                # use same filter for all streams
                filters = [filters] * len(tpl)
            if any(filtr(line, side_fobjs=side_fobjs)
                   for (filtr, line)
                   in zip(filters, tpl)):
                # filter out this line
                if logfile is not None:
                    logfile.write(' ||| '.join(tpl))
                    logfile.write('\n')
            else:
                # keep this line
                yield tpl

    @property
    def opt_deps(self):
        all_deps = set()
        filters = self.filters
        if isinstance(filters, Filter):
            filters = [filters]
        for filtr in filters:
            all_deps.update(filtr.opt_deps)
        return all_deps


class Filter(object):
    """Base class for filter implementations"""
    def __init__(self, side_inputs=None, side_outputs=None):
        self.side_inputs = side_inputs if side_inputs is not None else []
        self.side_outputs = side_outputs if side_outputs is not None else []
        self.opt_deps = set()

    def __call__(self, line, side_fobjs=None):
        """Returns True if the line should be filtered out"""
        raise NotImplementedError()

    def add_opt_dep(self, name, binary=False):
        self.opt_deps.add(OptionalDep(name, binary, self.__class__.__name__))

class FilterOovs(Filter):
    def __init__(self, vocabulary):
        super().__init__(side_inputs=[vocabulary])
        self.vocabulary = vocabulary
        self._vocab = None

    def _read_vocab(self, side_fobjs):
        self._vocab = set()
        for line in side_fobjs[self.vocabulary]:
            self._vocab.add(line.strip())

    def __call__(self, line, side_fobjs):
        if self._vocab is None:
            self._read_vocab(side_fobjs)
        return any(line.strip().split() not in self._vocab)


class ComparisonFilter(Filter):
    """Takes a tuple of lines instead of a single line"""
    pass


class NoFilter(Filter):
    """Does not filter out anything.
    Useful in ParallelFilter to only apply to one side."""
    def __call__(self, line, side_fobjs=None):
        return False


class InverseFilter(Filter):
    def __init__(self, filtr):
        super().__init__()
        self.filtr = filtr

    """Inverts a Filter,
    leaving only lines that the original would have removed"""
    def __call__(self, line, side_fobjs=None):
        return not self.filtr(line, side_fobjs=side_fobjs)


class FilterRegex(Filter):
    """Filters out any lines matching the expressions"""
    def __init__(self, expressions, ignore_case=False):
        super().__init__()
        flags = re.UNICODE
        if ignore_case:
            flags += re.IGNORECASE
        self.expressions = [re.compile(exp, flags=flags)
                            for exp in expressions]

    def __call__(self, line, side_fobjs=None):
        return any(exp.search(line) for exp in self.expressions)


class FilterUnclean(Filter):
    """Filters out any lines changed by the cleaning op"""
    def __init__(self, operation=None):
        super().__init__()
        self.operation = operation if operation is not None else Clean()
        try:
            self.operation.single_cell
        except AttributeError:
            raise TypeError('Wrapped operation "{}" in {} '
                'does not support single-cell operation'.format(
                    self.operation,
                    self.__class__.__name__))

    def __call__(self, line, side_fobjs=None):
        cleaned = self.operation.single_cell(line)
        return cleaned != line


class FilterByLength(Filter):
    def __init__(self,
                 min_tokens=None,
                 max_tokens=None,
                 min_chars=None,
                 max_chars=None,
                 max_chars_per_token=None):
        super().__init__()
        self.min_tokens = min_tokens
        self.max_tokens = max_tokens
        self.min_chars = min_chars
        self.max_chars = max_chars
        self.max_chars_per_token = max_chars_per_token

    def __call__(self, line, side_fobjs=None):
        if self.min_chars and len(line.strip()) < self.min_chars:
            return True
        if self.max_chars and len(line) > self.max_chars:
            return True
        tokens = line.split()
        if self.min_tokens and len(tokens) < self.min_tokens:
            return True
        if self.max_tokens and len(tokens) > self.max_tokens:
            return True
        if (self.max_chars_per_token and
                any(len(tok) > self.max_chars_per_token for tok in tokens)):
            return True
        return False


class ComparisonFilterByLengthRatio(ComparisonFilter):
    def __init__(self, min_ratio, max_ratio=None, threshold=10,
                 only_alpha=False, tokens=False):
        super().__init__()
        self.min_ratio = min_ratio
        self.max_ratio = max_ratio if max_ratio is not None \
            else 1. / min_ratio
        self.threshold = threshold
        self.only_alpha = only_alpha
        self.tokens = tokens
        assert not (tokens and only_alpha)

    def __call__(self, tpl, side_fobjs=None):
        left, right = tpl
        if self.only_alpha:
            left = ''.join([x for x in left.lower() if x in FILTER_ALPHA])
            right = ''.join([x for x in right.lower() if x in FILTER_ALPHA])
        if self.tokens:
            left = left.split()
            right = right.split()
        llen = float(len(left))
        rlen = float(len(right))
        if llen < self.threshold and rlen < self.threshold:
            # don't filter very short lines
            return False
        if llen == 0 or rlen == 0:
            # infinite ratio
            return True
        ratio = llen / rlen
        if ratio < self.min_ratio or ratio > self.max_ratio:
            # too extreme ratio, filter out this line
            return True
        # implicit else
        # keep this line
        return False


# FIXME: obsoleted by ComparisonFilterByLengthRatio
class FilterByLengthRatio(ParallelPipeComponent):
    """A Component that filters parallel streams
    by the ratio of their lenghts (in characters).
    """
    def __init__(self, min_ratio, max_ratio=None, threshold=10,
                 only_alpha=False, logfile=None):
        super().__init__(side_outputs=[logfile])
        self.logfile = logfile
        self.min_ratio = min_ratio
        self.max_ratio = max_ratio if max_ratio is not None \
            else 1. / min_ratio
        self.threshold = threshold
        self.only_alpha = only_alpha

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        logfile = side_fobjs.get(self.logfile, None)
        for tpl in stream:
            left, right = tpl
            if self.only_alpha:
                left = ''.join([x for x in left.lower() if x in FILTER_ALPHA])
                right = ''.join([x for x in right.lower() if x in FILTER_ALPHA])
            llen = float(len(left))
            rlen = float(len(right))
            if llen < self.threshold and rlen < self.threshold:
                # don't filter very short lines
                yield tpl
            if llen == 0 or rlen == 0:
                # infinite ratio
                continue
            ratio = llen / rlen
            if ratio < self.min_ratio or ratio > self.max_ratio:
                # too extreme ratio, filter out this line
                if logfile is not None:
                    logfile.write('{} ||| {} ||| {}\n'.format(ratio, tpl[0], tpl[1]))
                continue
            # implicit else
            # keep this line
            yield tpl



# a Component, not a Filter! needs to compare the streams.
class FilterLongUntranslated(ParallelPipeComponent):
    """A Component that filters parallel streams
    to remove untranslated content.
    """
    def __init__(self, threshold=20, logfile=None):
        super().__init__(side_outputs=[logfile])
        self.logfile = logfile
        self.threshold = threshold

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        logfile = side_fobjs.get(self.logfile, None)
        for tpl in stream:
            left, right = tpl
            left = ''.join([x for x in left.lower() if x in FILTER_ALPHA])
            right = ''.join([x for x in right.lower() if x in FILTER_ALPHA])
            llen = float(len(left))
            rlen = float(len(right))
            if llen < self.threshold or rlen < self.threshold:
                # don't filter short lines
                yield tpl
                continue
            if left == right:
                # same content on both sides
                if logfile is not None:
                    logfile.write('{} ||| {}\n'.format(*tpl))
                continue
            # implicit else
            # keep this line
            yield tpl


# a Component, not a Filter! uses a synchronous side input used multiple times
class FilterUsingLmScore(PipeComponent):
    """Filters out lines or tuples
    (can be used as both Mono and Parallel PipeComponent)
    if the score (read from a separate file) is above a threshold.
    Alternatively a fixed number of best entries can be kept
    (dynamically sets the threshold to achieve this).
    Can also be used with other scores than Language Model.
    """
    def __init__(self, scores, threshold=None, keep=None, logfile=None):
        super().__init__(side_inputs=[scores], side_outputs=[logfile])
        assert sum(x is None for x in (threshold, keep)) == 1
        self.scores_file = scores
        self.logfile = logfile
        self.keep = keep
        self.threshold = threshold
        self.scores = None
        # does not care if the data is mono or parallel
        self._is_mono_pipe_component = True
        self._is_parallel_pipe_component = True

    def pre_make(self, side_fobjs):
        if self.keep is None and self.threshold is not None:
            self.scores = (float(x) for x in side_fobjs[self.scores_file])
        elif self.threshold is None and self.keep is not None:
            # need to cache the whole list of scores
            self.scores = [float(x) for x in side_fobjs[self.scores_file]]
            sorted_scores = sorted(self.scores)
            self.threshold = sorted_scores[self.keep]

    def __call__(self, stream, side_fobjs=None, 
                 config=None, cli_args=None):
        logfile = side_fobjs.get(self.logfile, None)
        kept = 0
        for (line, score) in safe_zip(stream, self.scores):
            if score > self.threshold:
                if logfile is not None:
                    logfile.write('{}\t{}\n'.format(score, line))
                continue
            kept += 1
            yield line
            if self.keep is not None and kept == self.keep:
                break


class OnlyNames(Filter):
    """Only keep tokens that would be segmented by LetterizeNames"""
    def __call__(self, token, side_fobjs=None):
        #if FIVEDOT in token:
        #    return True
        if len(token) > 1 and (token[0].isupper() or token[0].isdigit()):
            return False    # these trigger the lettering operation
        return True


class FilterBureaucratic(Filter):
    """Filters out sentences that don't flow naturally due
    to a large number of parenthesized references etc.
    Use after tokenization."""
    def __init__(self,
                 threshold=4,
                 numeric=(4, 6, 10),
                 chars={'(': (2, 4, 6),
                        ')': (2, 4, 6),
                        '[': (2, 4, 6),
                        ']': (2, 4, 6),
                        ';': (2, 4, 6),
                        '/': (3, 4, 6),
                        ':': (3, 4, 6),
                        '-': (4, 6, 10),
                        ',': (4, 6, 10),}):
        super().__init__()
        self.threshold = threshold
        self.numeric = numeric
        self.chars = chars

    def __call__(self, line, side_fobjs=None):
        total = 0
        n_numeric = sum(1 for token in line.split()
                        if RE_NUMPUNC.match(token))
        for limit in self.numeric:
            if n_numeric >= limit:
                total += 1
        for char, limits in self.chars.items():
            n_char = sum(1 for c in line
                         if c == char)
            for limit in limits:
                if n_char >= limit:
                    total += 1
        # more strikes than the threshold: filter out
        return total >= self.threshold


class FilterRepetitions(Filter):
    """Filters out sentences with very repetitive content"""
    def __init__(self,
                 min_anywhere=15,
                 min_consequent=5):
        super().__init__()
        self.min_anywhere = min_anywhere
        self.min_consequent = min_consequent

    def __call__(self, line, side_fobjs=None):
        tokens = line.split()
        counts = collections.Counter(tokens)
        for word, count in counts.most_common():
            if count >= self.min_anywhere:
                return True
            if count < self.min_consequent:
                # can't be any streaks left anymore
                return False
            consequent = 0
            for token in tokens:
                if token == word:
                    consequent += 1
                    if consequent >= self.min_consequent:
                        return True
                else:
                    consequent = 0
        return False


class FilterSingleUrl(FilterRegex):
    """Filters out lines with only a single url"""
    # note that if you use SimpleTokenize, this should precede it
    def __init__(self):
        super().__init__((r'^https?://[^ ]*$',), ignore_case=True)


class FilterAllUrls(FilterRegex):
    """Filters out all lines with an url"""
    # note that if you use SimpleTokenize, this should precede it
    def __init__(self):
        super().__init__((r'https?://[^ ][^ ]*',), ignore_case=True)


class FilterEllipsis(FilterRegex):
    """Filters out lines with ..."""
    def __init__(self):
        super().__init__((r'\.\.\.',))


### Delayed filtering using mask

class MonoFilterMask(MonoPipeComponent):
    def __init__(self, filtr):
        side_inputs = filtr.side_inputs
        side_outputs = filtr.side_outputs
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)
        self.filtr = filtr

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        logfile = side_fobjs.get(self.logfile, None)
        for line in stream:
            if self.filtr(line, side_fobjs=side_fobjs):
                # filter out this line
                yield "1"
            else:
                # keep this line
                yield "0"

# FIXME clunky: produces two identical outputs
class ParallelFilterMask(ParallelPipeComponent):
    def __init__(self, filters):
        side_inputs = []
        side_outputs = []
        if isinstance(filters, Filter):
            side_inputs.extend(filters.side_inputs)
            side_outputs.extend(filters.side_outputs)
        else:
            for filtr in filters:
                assert isinstance(filtr, Filter)
                side_inputs.extend(filtr.side_inputs)
                side_outputs.extend(filtr.side_outputs)
        super().__init__(side_inputs=side_inputs, side_outputs=side_outputs)
        self.filters = filters

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        filters = self.filters
        for tpl in stream:
            if isinstance(filters, Filter) and not isinstance(filters, ComparisonFilter):
                # use same filter for all streams
                filters = [filters] * len(tpl)
            if isinstance(filters, ComparisonFilter):
                remove = filters(tpl, side_fobjs=side_fobjs)
            else:
                remove = any(filtr(line, side_fobjs=side_fobjs)
                             for (filtr, line)
                             in zip(filters, tpl))
            if remove:
                # filter out this line
                yield tuple("1" for _ in range(len(tpl)))
            else:
                # keep this line
                yield tuple("0" for _ in range(len(tpl)))


class CombineFilterMasks(Rule):
    def __init__(self, inputs, output, func=None, **kwargs):
        super().__init__(inputs, [output], **kwargs)
        self.func = func if func is not None else lambda x: x == "1"
        # does not care if the data is mono or parallel
        self._is_mono_pipe_component = True
        self._is_parallel_pipe_component = True

    def make(self, conf, cli_args=None):
        inputs = [mask.open(conf, cli_args, mode='r')
                  for mask in self.inputs]
        output = self.outputs[0].open(conf, cli_args, mode='w')
        kept = 0
        for masks in safe_zip(*inputs):
            if any(self.func(mask) for mask in masks):
                # filter out this line
                output.write("1\n")
            else:
                # keep this line
                output.write("0\n")
        for fobj in inputs:
            fobj.close()
        output.close()


# a Component, not a Filter!
# can be used as both Mono and Parallel PipeComponent
class FilterUsingMask(PipeComponent):
    def __init__(self, masks, func=None, logfile=None):
        super().__init__(side_inputs=[masks], side_outputs=[logfile])
        self.masks = masks
        self.func = func if func is not None else lambda x: x == "1"
        self.logfile = logfile
        # does not care if the data is mono or parallel
        self._is_mono_pipe_component = True
        self._is_parallel_pipe_component = True

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        masks = side_fobjs[self.masks]
        logfile = side_fobjs.get(self.logfile, None)
        for (line, mask) in safe_zip(stream, masks):
            if self.func(mask):
                # filter out this line
                if logfile is not None:
                    logfile.write('{}\n'.format(line))
            else:
                yield line
