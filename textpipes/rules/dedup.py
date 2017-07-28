import logging

from ..recipe import Rule
from ..components.core import ParallelPipe
from ..components.filtering import Filter, ParallelFilter

logger = logging.getLogger(__name__)

try:
    from pybloom import BloomFilter
except ImportError:
    # To fix this, install a python3 compatible pybloom.
    # warnings emitted by check in cli
    pass


class Deduplicate(Rule):
    def __init__(self,
                 inputs, outputs,
                 dup_proportion=.1,
                 truncate=30,
                 **kwargs):
        super().__init__(inputs, outputs, **kwargs)
        # capacity ratio of bloom filters
        self.dup_proportion = dup_proportion
        # if this many initial chars match, consider it a dup
        self.truncate = truncate

    def make(self, conf, cli_args):
        estimated_lines = 1.2 * conf.conf['exp'].getint('n_lines', 500000000)
        # first pass: collect collisions
        pipes = [inp.open(conf, cli_args, mode='rb')
                 for inp in self.inputs]
        filters = [DedupFilter(pipe,
                               estimated_lines=estimated_lines,
                               dup_proportion=self.dup_proportion,
                               truncate=self.truncate)
                   for pipe in pipes]
        # second pass: filter
        pp = ParallelPipe(
            [ParallelFilter(filters)],
            self.inputs, self.outputs)
        pp.make(conf, cli_args)


class DedupFilter(Filter):
    def __init__(self, lines, estimated_lines, dup_proportion, truncate):
        estimated_dups = estimated_lines * dup_proportion
        self.truncate = truncate
        self.potential = BloomFilter(capacity=estimated_dups,
                                     error_rate=0.001)
        self.seen = set()
        self._find_collisions(lines, estimated_lines)
        
    def _find_collisions(self, lines, estimated_lines):
        bloom_all = BloomFilter(capacity=estimated_lines,
                                error_rate=0.005)
        for line in lines:
            if line in bloom_all:
                self.potential.add(line)
            bloom_all.add(line)

    def __call__(self, line):
        if line in self.potential:
            line_trunc = line[:self.truncate]
            if line_trunc in self.seen:
                return True
            self.seen.add(line_trunc)
        return False
