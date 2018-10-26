import bz2
import codecs
import gzip
import itertools
import logging
import lzma
import os
import re
import subprocess
from multiprocessing import Pool

logger = logging.getLogger('textpipes')

UNICODE_UNIT_SEP = '\u001F'
THREEDOT = '\u2056' # 3-dot punctuation
FOURDOT  = '\u2058' # 4-dot punctuation
FIVEDOT  = '\u2059' # 5-dot punctuation. Default subword-boundary marker.

def safe_zip(*iterables):
    iters = [iter(x) for x in iterables]
    sentinel = object()
    for (j, tpl) in enumerate(itertools.zip_longest(*iterables, fillvalue=sentinel)):
        for (i, val) in enumerate(tpl):
            if val is sentinel:
                raise ValueError('Column {} was too short. '
                    'Row {} (and later) missing.'.format(i, j))
        yield tpl


# opening a gzip as binary and then using a codecs reader on it
# can result in nasty newline bugs
def open_text_file(file_path, mode='r', encoding='utf-8'):
    """Open a file for i/o with the appropriate decompression/decoding
    """
    mode = mode.replace('b', '')    # FIXME: hack
    if file_path.endswith('.gz'):
        if 't' not in mode:
            mode += 't'
        file_obj = gzip.open(file_path, mode)
    elif file_path.endswith('.bz2'):
        if 't' not in mode:
            mode += 't'
        file_obj = bz2.open(file_path, mode)
    elif file_path.endswith('.xz'):
        if 't' not in mode:
            mode += 't'
        file_obj = lzma.open(file_path, mode)
    else:
        file_obj = open(file_path, mode)
    if encoding != 'utf-8':
        raise Exception('Re-encode your data')
    return file_obj


def external_linecount(file_path):
    if file_path.endswith('.gz'):
        ext_lc = subprocess.check_output(
            ['zcat {} | wc -l'.format(file_path)], shell=True).split()[0]
    else:
        ext_lc = subprocess.check_output(['wc', '-l', file_path]).split()[0]
    ext_lc = int(ext_lc.decode('utf-8'))
    return ext_lc


def table_print(tpls, line_before=False, line_after=False):
    transposed = tuple(zip(*tpls))
    col_widths = [max(len(str(val)) for val in column)
                  for column in transposed]
    total_width = sum(col_widths) + (2 * (len(transposed) - 1))
    fmt = ['{!s:' + str(width) + '}' for width in col_widths]
    fmt = '  '.join(fmt)
    if line_before:
        print(line_before * total_width)
    for tpl in tpls:
        print(fmt.format(*tpl))
    if line_after:
        print(line_after * total_width)


def progress(iterable, rule, conf, out_file, total='conf'):
    try:
        from tqdm import tqdm
    except ImportError:
        return iterable
    if total == 'conf':
        total = conf.conf['exp'].getint('n_lines', None)
    rule_name = rule.name[:15]
    out_file = out_file[-20:]
    description = '{}:{}'.format(rule_name, out_file)
    return tqdm(iterable, desc=description, total=total, unit_scale=True)


class LazyPool(object):
    def __init__(self, processes, chunksize=1000):
        self.processes = processes
        self.chunksize = chunksize
        self.pool = None

    def imap(self, func, iterable, chunksize=None):
        chunksize = chunksize if chunksize is not None else self.chunksize
        if self.pool is None:
            if self.processes is not None:
                logger.info('Using pool of {} processes'.format(self.processes))
            else:
                logger.info('Using maximal pool.')
            self.pool = Pool(processes=self.processes)
        for item in self.pool.imap(func, iterable, chunksize):
            yield item

    def close(self):
        if self.pool is None:
            self.pool.close()


class NoPool(object):
    def __init__(self, *args, **kwargs):
        pass

    def imap(self, func, iterable, chunksize=None):
        return map(func, iterable)

    def close(self):
        pass


# FIXME: use package resources instead
LANG_DIR = os.path.join(
    os.path.dirname(__file__), 'langs')

def read_lang_file(fname, lang):
    result = []
    path = os.path.join(LANG_DIR, '{}.{}'.format(fname, lang))
    for line in codecs.open(path, encoding='utf-8'):
        line = line.strip()
        if len(line) == 0 or line[0] == '#':
            continue
        result.append(line)
    return result


def find_highest_file(path_template, wildcard):
    directory, filename_base = os.path.split(path_template)
    filename_template = re.escape(filename_base) + wildcard
    re_filename_template = re.compile(filename_template)
    matches = []
    for candidate in os.listdir(directory):
        m = re_filename_template.match(candidate)
        if not m:
            continue
        idx = int(m.group(1))
        matches.append((idx, candidate))
    if len(matches) == 0:
        return None, None
    idx, highest = max(matches)
    return idx, os.path.join(directory, highest)


def dir_is_empty(path):
    return all(f.startswith('.') for f in os.listdir(path))
