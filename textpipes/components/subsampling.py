import random

from .core import PipeComponent
from ..core.recipe import Rule

class Head(PipeComponent):
    def __init__(self, limit):
        super().__init__()
        self.limit = int(limit)
        # does not care if the data is mono or parallel
        self._is_mono_pipe_component = True
        self._is_parallel_pipe_component = True

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for (i, line) in enumerate(stream):
            if i >= self.limit:
                break
            yield line

class Tail(PipeComponent):
    def __init__(self, skip):
        super().__init__()
        self.skip = int(skip)
        # does not care if the data is mono or parallel
        self._is_mono_pipe_component = True
        self._is_parallel_pipe_component = True

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for (i, line) in enumerate(stream):
            if i < self.skip:
                continue
            yield line

# for backwards compatibility
ParaTail = Tail

class RoundRobin(PipeComponent):
    def __init__(self, shard_idx, num_shards):
        super().__init__()
        self.shard_idx = int(shard_idx)
        self.num_shards = int(num_shards)
        # does not care if the data is mono or parallel
        self._is_mono_pipe_component = True
        self._is_parallel_pipe_component = True

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for (i, line) in enumerate(stream):
            if i % self.num_shards == self.shard_idx:
                yield line


class DeRoundRobin(Rule):
    def __init__(self, inputs, output):
        super().__init__(inputs, [output])

    def make(self, conf, cli_args=None):
        # Make a tuple of generators that reads from main_inputs
        readers = [inp.open(conf, cli_args, mode='r')
                   for inp in self.main_inputs]

        # Round-robin read from each, and drain pipeline into main_output
        writer = self.main_outputs[0].open(conf, cli_args, mode='w')
        while True:
            for reader in readers:
                try:
                    writer.write(next(reader))
                    writer.write('\n')
                except StopIteration:
                    pass
        # close all file objects
        for fobj in readers + [writer]:
            fobj.close()


class Shuffle(PipeComponent):
    """Full uniform shuffle.
     Reads the whole data into memory.
    """
    def __init__(self):
        super().__init__()
        # does not care if the data is mono or parallel
        self._is_mono_pipe_component = True
        self._is_parallel_pipe_component = True

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        stream = list(stream)
        random.shuffle(stream)
        for line in stream:
            yield line
