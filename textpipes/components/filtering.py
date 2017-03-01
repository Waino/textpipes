import re

from .preprocessing import Clean


class FilterUnclean(Filter):
    """Filters out any lines changed by the cleaning op"""
    def __init__(self, operation=None):
        self.wrapped_operation = operation if operation is not None \
            else Clean()
        try:
            self.wrapped_operation.single_cell
        except AttributeError:
            raise TypeError('Wrapped operation "{}" in {} '
                'does not support single-cell operation'.format(
                    self.wrapped_operation,
                    self.__class__.__name__))
        super().__init__(
            criterion=filter_by_column([self._filter],
                                       broadcast_columns=True))

    def _filter(self, line):
        cleaned = self.wrapped_operation.single_cell(line, tags=None)
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
        super().__init__(
            criterion=filter_by_column([self._filter],
                                       broadcast_columns=True))

    def _filter(self, line):
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


class FilterByLengthRatio(Filter):
    def __init__(self, min_ratio, max_ratio=None):
        self.min_ratio = min_ratio
        self.max_ratio = max_ratio if max_ratio is not None \
            else 1. / min_ratio
        super().__init__(criterion=self._filter)

    def _filter(self, tpl):
        try:
            left, right = tpl
        except ValueError:
            raise Exception('FilterByLengthRatio got {} columns, '
                'expecting 2'.format(len(tpl)))
        try:
            ratio = len(left.split()) / len(right.split())
        except ZeroDivisionError:
            return True     # ratio is infinite
        return ratio < self.min_ratio or ratio > self.max_ratio
