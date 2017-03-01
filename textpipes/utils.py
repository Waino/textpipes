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
    if 'w' in mode:
        return codecs.getwriter(encoding)(file_obj)
    else:
        return codecs.getreader(encoding)(file_obj)
