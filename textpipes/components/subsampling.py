import collections
import random

from .core import PipeComponent, MonoPipeComponent, DeadEndPipe, \
                  apply_component
from ..core.recipe import Rule

def split_dataset(inputs, train_file,
                  dev_file=None, test_file=None,
                  dev_size=0, test_size=0, train_size=None,
                  resource_class='make_immediately',
                  **kwargs):
    components = [Shuffle()]
    used_lines = 0
    if dev_size:
        assert dev_file is not None
        components.append(HeadTee(dev_size, dev_file))
        used_lines += dev_size
    if test_size:
        assert test_file is not None
        components.append(SliceTee(
            used_lines, used_lines + test_size, test_file))
        used_lines += test_size
    if train_size:
        components.append(SliceTee(
            used_lines, used_lines + train_size, train_file))
        used_lines += test_size
    else:
        # use all remaining lines for train
        components.append(TailTee(
            used_lines, train_file))
    return DeadEndPipe(components, inputs,
                       name='SplitDataset',
                       resource_class=resource_class,
                       **kwargs)

def split_dataset_para(recipe,
                       inputs, tmp_files, train_files, dev_files,
                       dev_size=0,
                       **kwargs):
    recipe.add_rule(apply_component(Shuffle(), para=True, **kwargs)(inputs, tmp_files))
    recipe.add_rule(apply_component(Head(dev_size), para=True, **kwargs)(tmp_files, dev_files))
    recipe.add_rule(apply_component(Tail(dev_size), para=True, **kwargs)(tmp_files, train_files))


class Head(PipeComponent):
    """Removes from the stream everything except for
    the specified number of lines/tuples from the begining"""
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
    """Skips the specified number of lines/tuples from the
    beginning, then outputs the rest."""
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

class RealTail(PipeComponent):
    """Removes from the stream everything except for
    the specified number of lines/tuples from the end"""
    def __init__(self, keep):
        super().__init__()
        self.keep = int(keep)
        self.deque = collections.deque(maxlen=self.keep)
        # does not care if the data is mono or parallel
        self._is_mono_pipe_component = True
        self._is_parallel_pipe_component = True

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for line in stream:
            self.deque.append(line)
        for line in self.deque:
            yield line
        del self.deque

class HeadTee(MonoPipeComponent):
    """Passes the stream unchanged, while copying
    the specified number of lines from the begining
    to sub_file"""
    def __init__(self, limit, sub_file):
        super().__init__(side_outputs=[sub_file])
        self.sub_file = sub_file
        self.limit = int(limit)

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        writer = side_fobjs[self.sub_file]
        for (i, line) in enumerate(stream):
            if i < self.limit:
                writer.write(line)
                writer.write('\n')
            yield line

class SliceTee(MonoPipeComponent):
    """Passes the stream unchanged, while copying
    the specified number of lines from the middle
    to sub_file"""
    def __init__(self, start, end, sub_file):
        super().__init__(side_outputs=[sub_file])
        self.sub_file = sub_file
        self.start = int(start)
        self.end = int(end)

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        writer = side_fobjs[self.sub_file]
        for (i, line) in enumerate(stream):
            if self.start <= i < self.end:
                writer.write(line)
                writer.write('\n')
            yield line

class TailTee(MonoPipeComponent):
    """Passes the stream unchanged, while copying
    the specified number of lines from the end
    to sub_file"""
    def __init__(self, skip, sub_file):
        super().__init__(side_outputs=[sub_file])
        self.sub_file = sub_file
        self.skip = int(skip)

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        writer = side_fobjs[self.sub_file]
        for (i, line) in enumerate(stream):
            if i >= self.skip:
                writer.write(line)
                writer.write('\n')
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
    def __init__(self, inputs, output, multipliers=None):
        super().__init__(inputs, [output])
        self.multipliers = multipliers
        if multipliers is not None:
            assert len(multipliers) == len(inputs)

    def make(self, conf, cli_args=None):
        # Make a tuple of generators that reads from inputs
        readers = [inp.open(conf, cli_args, mode='r')
                   for inp in self.inputs]

        if self.multipliers is not None:
            # multipliers allow reading several lines 
            # at a time from particular files,
            # if line counts are unbalanced
            repeated = []
            for mult, reader in zip(self.multipliers, readers):
                repeated.extend(mult * [reader])
            readers = repeated

        # Round-robin read from each, and drain pipeline into output
        writer = self.outputs[0].open(conf, cli_args, mode='w')
        while len(readers) > 0:
            for (i, reader) in enumerate(readers):
                try:
                    writer.write(next(reader))
                    writer.write('\n')
                except StopIteration:
                    reader.close()
                    readers = readers[:i] + readers[i+1:]
        writer.close()


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
        try:
            random.seed(config['exp']['seed'])
        except KeyError:
            pass
        random.shuffle(stream)
        for line in stream:
            yield line


class Upsample(PipeComponent):
    """Repeats each line in the data n times.
    """
    def __init__(self, n):
        super().__init__()
        self.n = n
        # does not care if the data is mono or parallel
        self._is_mono_pipe_component = True
        self._is_parallel_pipe_component = True

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        for line in stream:
            for _ in range(self.n):
                yield line


class ChunkSplit(Rule):
    def __init__(self, inp, outputs, lines_per_chunk):
        super().__init__([inp], outputs)
        self.lines_per_chunk = lines_per_chunk
        # does not care if the data is mono or parallel
        self._is_mono_pipe_component = True
        self._is_parallel_pipe_component = True

    def make(self, conf, cli_args=None):
        outputs = list(self.outputs)
        writer = None
        stream = self.inputs[0].open(conf, cli_args, mode='r')
        for (i, line) in enumerate(stream):
            if i % self.lines_per_chunk == 0:
                if writer is not None:
                    writer.close()
                writer = outputs.pop(0).open(conf, cli_args, mode='w')
            writer.write(line)
            writer.write('\n')
        if writer is not None:
            writer.close()
        stream.close()
