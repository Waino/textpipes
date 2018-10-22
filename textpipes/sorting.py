import collections
import logging
import math

from .core.utils import FIVEDOT
from .components.core import MonoPipe, MonoPipeComponent

logger = logging.getLogger('textpipes')

class SortComponent(MonoPipeComponent):
    def __init__(self, key, reverse=False):
        super().__init__()
        self.key = key
        self.reverse = reverse

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        stream = sorted(stream, key=self.key, reverse=self.reverse)
        yield from stream


class Sort(MonoPipe):
    def __init__(self, inp, out, key, reverse=False):
        super().__init__([SortComponent(key, reverse=reverse)], [inp], [out])


class SortByLength(Sort):
    def __init__(self, inp, out, reverse=False):
        super().__init__(inp, out, len, reverse=reverse)
