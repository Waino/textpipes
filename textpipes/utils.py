import bz2
import codecs
import gzip
import itertools
import subprocess

UNICODE_UNIT_SEP = '\u001F'
FIVEDOT = '\u2059' # 5-dot punctuation

def safe_zip(*iterables):
    iters = [iter(x) for x in iterables]
    sentinel = object()
    for (j, tpl) in enumerate(itertools.zip_longest(*iterables, fillvalue=sentinel)):
        for (i, val) in enumerate(tpl):
            if val is sentinel:
                raise ValueError('Column {} was too short. '
                    'Row {} (and later) missing.'.format(i, j))
        yield tpl


def open_text_file(file_path, mode='rb', encoding='utf-8'):
    """Open a file for i/o with the appropriate decompression/decoding
    """
    if 'b' not in mode:
        mode += 'b'
    if file_path.endswith('.gz'):
        file_obj = gzip.open(file_path, mode)
    elif file_path.endswith('.bz2'):
        file_obj = bz2.BZ2File(file_path, mode)
    else:
        file_obj = open(file_path, mode)
    if 'w' in mode or 'a' in mode:
        return codecs.getwriter(encoding)(file_obj)
    else:
        return codecs.getreader(encoding)(file_obj)


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
    fmt = ['{:' + str(width) + '}' for width in col_widths]
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
