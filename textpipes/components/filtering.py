import re

from .core import MonoPipeComponent, ParallelPipeComponent
from .preprocessing import Clean

class MonoFilter(MonoPipeComponent):
    def __init__(self, filtr, logfile=None):
        self.filtr = filtr
        self.logfile = logfile

    def __call__(self, stream):
        for line in stream:
            if self.filtr(line):
                # filter out this line
                if self.logfile is not None:
                    # FIXME: write into logfile
                    # FIXME: requires handing down conf, cli_args
                    pass
            else:
                # keep this line
                yield line

class ParallelFilter(ParallelPipeComponent):
    def __init__(self, filters, logfile=None):
        self.filters = filters
        self.logfile = logfile

    def __call__(self, stream):
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
                    # FIXME: requires handing down conf, cli_args
                    pass
            else:
                # keep this line
                yield tpl


class Filter(object):
    """Base class for filter implementations"""
    def __call__(self, line):
        """Returns True if the line should be filtered out"""
        raise NotImplementedError()


class FilterUnclean(Filter):
    """Filters out any lines changed by the cleaning op"""
    def __init__(self, operation=None):
        self.operation = operation if operation is not None else Clean()
        try:
            self.operation.single_cell
        except AttributeError:
            raise TypeError('Wrapped operation "{}" in {} '
                'does not support single-cell operation'.format(
                    self.operation,
                    self.__class__.__name__))

    def __call__(self, line):
        cleaned = self.operation.single_cell(line)
        return cleaned != line


class FilterByLength(Filter):
    def __init__(self,
                 min_tokens=None,
                 max_tokens=None,
                 max_chars=None,
                 max_chars_per_token=None):
        self.min_tokens = min_tokens
        self.max_tokens = max_tokens
        self.max_chars = max_chars
        self.max_chars_per_token = max_chars_per_token

    def __call__(self, line):
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


#class FilterByLengthRatio(Filter):
#    def __init__(self, min_ratio, max_ratio=None):
#        self.min_ratio = min_ratio
#        self.max_ratio = max_ratio if max_ratio is not None \
#            else 1. / min_ratio
#
#    def __call__(self, tpl):
#        try:
#            left, right = tpl
#        except ValueError:
#            raise Exception('FilterByLengthRatio got {} columns, '
#                'expecting 2'.format(len(tpl)))
#        try:
#            ratio = len(left.split()) / len(right.split())
#        except ZeroDivisionError:
#            return True     # ratio is infinite
#        return ratio < self.min_ratio or ratio > self.max_ratio
