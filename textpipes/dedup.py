import logging

from .recipe import Rule

logger = logging.getLogger(__name__)

try:
    from pybloom import BloomFilter
except ImportError:
    logger.warning('Unable to load BloomFilter from pybloom.')
    logger.warning('You will not be able to use Deduplicate.')
    logger.warning('To fix this, install a python3 compatible pybloom.')


class Deduplicate(Rule):    # FIXME: rewrite as Rule
    def __init__(self,
                 estimated_lines=500000000,
                 dup_proportion=.1,
                 truncate=30):
        # capacity of big bloom filter
        self.estimated_lines = estimated_lines
        # capacity of small bloom filter
        self.estimated_dups = estimated_lines * dup_proportion
        # if this many initial chars match, consider it a dup
        self.truncate = truncate

        self.potential = BloomFilter(capacity=self.estimated_dups,
                                     error_rate=0.001)
        super().__init__([self._collisions, self._dups])

    def _collisions(self, incoming_pipes, column_tags=None):
        bloom_all = BloomFilter(capacity=self.estimated_lines,
                                error_rate=0.005)
        for tpl in unwrap_single(incoming_pipes, assert_single=True):
            line, = tpl     # extract only column
            if line in bloom_all:
                self.potential.add(line)
            bloom_all.add(line)

    def _dups(self, incoming_pipes, column_tags=None):
        seen = set()
        for (i, tpl) in enumerate(unwrap_single(incoming_pipes,
                                                assert_single=True)):
            line, = tpl     # extract only column
            if line in self.potential:
                line_trunc = line[:self.truncate]
                if line_trunc in seen:
                    self.filter_indices.add(i)
                    continue
                seen.add(line_trunc)

    def make(self, conf, cli_args):
        # 2-pass: collect indices, use them to filter
        pass
