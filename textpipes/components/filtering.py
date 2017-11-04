import re

from .core import MonoPipeComponent, ParallelPipeComponent
from .preprocessing import Clean

class MonoFilter(MonoPipeComponent):
    def __init__(self, filtr, logfile=None):
        super().__init__(side_outputs=[logfile])
        self.filtr = filtr
        self.logfile = logfile

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for line in stream:
            if self.filtr(line):
                # filter out this line
                if self.logfile is not None:
                    # FIXME: write into logfile
                    pass
            else:
                # keep this line
                yield line

class ParallelFilter(ParallelPipeComponent):
    def __init__(self, filters, logfile=None):
        super().__init__(side_outputs=[logfile])
        self.filters = filters
        self.logfile = logfile

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        filters = self.filters
        for tpl in stream:
            if isinstance(filters, Filter):
                # use same filter for all streams
                filters = [filters] * len(tpl)
            if any(filtr(line) for (filtr, line)
                   in zip(filters, tpl)):
                # filter out this line
                if self.logfile is not None:
                    # FIXME: write into logfile
                    pass
            else:
                # keep this line
                yield tpl


class Filter(object):
    """Base class for filter implementations"""
    def __call__(self, line, side_fobjs=None):
        """Returns True if the line should be filtered out"""
        raise NotImplementedError()


class NoFilter(Filter):
    """Does not filter out anything.
    Useful in ParallelFilter to only apply to one side."""
    def __call__(self, line, side_fobjs=None):
        return False


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
                 max_chars=None,
                 max_chars_per_token=None):
        super().__init__()
        self.min_tokens = min_tokens
        self.max_tokens = max_tokens
        self.max_chars = max_chars
        self.max_chars_per_token = max_chars_per_token

    def __call__(self, line, side_fobjs=None):
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


# a Component, not a Filter! needs to compare the streams.
class FilterByLengthRatio(ParallelPipeComponent):
    """A Component that filters parallel streams
    by the ratio of their lenghts (in characters).
    """
    def __init__(self, min_ratio, max_ratio=None, treshold=10, logfile=None):
        super().__init__(side_outputs=[logfile])
        self.logfile = logfile
        self.min_ratio = min_ratio
        self.max_ratio = max_ratio if max_ratio is not None \
            else 1. / min_ratio
        self.treshold = treshold

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        logfile = side_fobjs[self.logfile]
        for tpl in stream:
            left, right = tpl
            llen = float(len(left))
            rlen = float(len(right))
            if llen < self.treshold and rlen < self.treshold:
                # don't filter very short lines
                yield tpl
            if llen == 0 or rlen == 0:
                # infinite ratio
                continue
            ratio = llen / rlen
            if ratio < self.min_ratio or ratio > self.max_ratio:
                # too extreme ratio, filter out this line
                if logfile is not None:
                    logfile.write('{} ||| {} ||| {}\n'.format(ratio, left, right))
                continue
            # implicit else
            # keep this line
            yield tpl


class OnlyNames(Filter):
    """Only keep tokens that would be segmented by LetterizeNames"""
    def __call__(self, token, side_fobjs=None):
        #if FIVEDOT in token:
        #    return True
        if len(token) > 1 and (token[0].isupper() or token[0].isdigit()):
            return False    # these trigger the lettering operation
        return True
