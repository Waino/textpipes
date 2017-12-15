from .core import MonoPipeComponent

class Head(MonoPipeComponent):
    def __init__(self, limit):
        super().__init__()
        self.limit = int(limit)

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for (i, line) in enumerate(stream):
            if i >= self.limit:
                break
            yield line

class RoundRobin(MonoPipeComponent):
    def __init__(self, shard_idx, num_shards):
        super().__init__()
        self.shard_idx = int(shard_idx)
        self.num_shards = int(num_shards)

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for (i, line) in enumerate(stream):
            if i % self.num_shards == self.shard_idx:
                yield line
