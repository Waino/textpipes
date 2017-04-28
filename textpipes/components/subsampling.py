from .core import MonoPipeComponent

class Head(MonoPipeComponent):
    def __init__(self, limit):
        super().__init__()
        self.limit = int(limit)

    def __call__(self, stream, side_fobjs=None):
        for (i, line) in enumerate(stream):
            if i >= self.limit:
                break
            yield line
