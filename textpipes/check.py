from abc import ABCMeta, abstractmethod
import collections
import os
import re
import subprocess

from .core.utils import UNICODE_UNIT_SEP, FIVEDOT, open_text_file

RE_ANY_WHITE = re.compile(r'\s', flags=re.UNICODE)

def summarize_num(num):
    if num < 10:
        return str(num)
    if num < 100:
        return 'd'
    if num < 1000:
        return 'h'
    if num < 10000:
        return 'k'
    return '!'

def summarize_counter(counter,
                      show_label=True, show_count=True, show_prop=True):
    sorted_counts = counter.most_common()
    if len(sorted_counts) == 0:
        return '-'
    total = sum(c for (l, c) in sorted_counts)
    label, count = sorted_counts[0]
    if total == 0:
        return '-'

    result = []
    if show_label:
        result.append(label)
    if show_count:
        result.append(summarize_num(count))
    if show_prop:
        prop = int(100 * (float(count) / total))
        if prop == 100:
            result.append('all')
        else:
            result.append('{:2}%'.format(prop))
    return ' '.join(result)


class SummaryColumn(metaclass=ABCMeta):
    def __init__(self, heading, width):
        self.formatstr = '{:' + str(width)  + '}'
        self.heading = self.formatstr.format(heading)
        self.width = width
        self.counter = None

    def format_summary(self):
        return self.formatstr.format(self.summary)

    def new_file(self, file_name):
        self.counter = collections.Counter()

    def line_count(self, i):
        pass
        
    @abstractmethod
    def process_line(self, i, line):
        pass

    @property
    @abstractmethod
    def summary(self):
        pass


class PatternColumn(SummaryColumn):
    def __init__(self, heading, width, patterns):
        self.patterns = [(re.compile(expr, flags=re.UNICODE), label)
                         for (expr, label) in patterns]
        super().__init__(heading, width)

    def process_line(self, i, line):
        for (expr, label) in self.patterns:
            self.counter[label] += len(expr.findall(line))

    @property
    def summary(self):
        return summarize_counter(self.counter)


# ## concrete columns

class FileName(SummaryColumn):
    def __init__(self):
        super().__init__('File name', 40)

    def new_file(self, file_name):
        self.file_name = file_name

    def process_line(self, i, line):
        pass

    @property
    def summary(self):
        # end is more informative
        return self.file_name[-self.width:]
    

class LineCount(SummaryColumn):
    def __init__(self):
        super().__init__('Linecount', 10)

    def process_line(self, i, line):
        pass

    def line_count(self, i):
            self.counter = i

    @property
    def summary(self):
        return self.counter

# most frequent fully alpha token?  -> lang

# max len in tokens, chars
class MaxLen(SummaryColumn):
    def __init__(self):
        super().__init__('#Tok/Chr', 3 + 1 + 4)

    def new_file(self, file_name):
        self.max_chr = 0
        self.max_tok = 0

    def process_line(self, i, line):
        self.max_chr = max(self.max_chr, len(line))
        self.max_tok = max(self.max_tok, len(line.split()))

    @property
    def summary(self):
        return '{:2}/{}'.format(self.max_tok, self.max_chr)
    
# case of first token
class FirstCase(SummaryColumn):
    def __init__(self):
        super().__init__('Case', 5)

    def process_line(self, i, line):
        if line[0].isupper():
            self.counter['U'] += 1
        elif line[0].islower():
            self.counter['L'] += 1
        elif line[0].isdigit():
            self.counter['N'] += 1
        else:
            self.counter['?'] += 1

    @property
    def summary(self):
        return summarize_counter(self.counter, show_count=False)

# hyphens: most common char before and after
RE_HYP_TRIPLE = re.compile(r'.-.', flags=re.UNICODE)
class Hyphens(SummaryColumn):
    def __init__(self):
        super().__init__('Hyphen', 2+6)

    def process_line(self, i, line):
        for triple in RE_HYP_TRIPLE.findall(line):
            key = triple[0] + triple[2]
            key = RE_ANY_WHITE.sub(' ', key)
            self.counter[key] += 1

    @property
    def summary(self):
        return summarize_counter(self.counter)

# periods: most common char before
RE_PERIOD_PAIR = re.compile(r'.\.', flags=re.UNICODE)
class Periods(SummaryColumn):
    def __init__(self):
        super().__init__('Dots', 1+6)

    def process_line(self, i, line):
        for pair in RE_PERIOD_PAIR.findall(line):
            key = pair[0]
            key = RE_ANY_WHITE.sub(' ', key)
            self.counter[key] += 1

    @property
    def summary(self):
        return summarize_counter(self.counter)

## multicol: tabs, unicode-seps
class MultiCol(SummaryColumn):
    def __init__(self):
        super().__init__('Cols', 2+6)

    def process_line(self, i, line):
        n_col = len(line.split(UNICODE_UNIT_SEP))
        if n_col > 1:
            self.counter['{}u'.format(n_col)] += 1
            return
        n_col = len(line.split('\t'))
        if n_col > 1:
            self.counter['{}t'.format(n_col)] += 1
            return
        n_col = len(line.split('|||'))
        if n_col > 1:
            self.counter['{}p'.format(n_col)] += 1
            return
        self.counter['no'] += 1

    @property
    def summary(self):
        return summarize_counter(self.counter)

# patterns: <number> vs [0-9]+
class Numbers(PatternColumn):
    def __init__(self):
        patterns = ((r'[<&]number[>;]', 'tag'),
                    (r'(?<=\s)[0-9]+(?=\s)', 'raw'))
        super().__init__('Numbers', 3+6, patterns)

# boundary markers r'(?<=\S)@ ' r' \+(?=\S)' {[^}]+}
class BoundaryMarkers(PatternColumn):
    def __init__(self):
        patterns = ((r'(?<=\S)@ ', 'comp'),
                    (r' \+(?=\S)', 'plus'),
                    (r'\+ (?=\S)', 'plus'),
                    (r' ' + FIVEDOT + r'(?=\S)', '5dot'),
                    (FIVEDOT + r' (?=\S)', '5dot'),
                    (r'{[^}]+}', 'omor'),
                    (r'\w \w', 'none'))
        super().__init__('Boundary', 4+6, patterns)

# patterns: 's vs &apos;s vs ' s vs &apos; s (and all without leading space)
class Clitics(PatternColumn):
    def __init__(self):
        patterns = ((r" 's ",           'spl-un '),
                    (r" &apos;s ",      'spl-esc'),
                    (r" ' s ",          'OVR-un '),
                    (r" &apos; s ",     'OVR-esc'),
                    (r"\S's ",          ' un-un '),
                    (r"\S&apos;s ",     ' un-esc'),
                    (r"\S' s ",         'WRNG-un'),
                    (r"\S&apos; s ",    'WRNG-es'))
        super().__init__('Clitics', 7+6, patterns)

# patterns: ," vs ", vs ,&quot; vs &quot;,
# FIXME: might have a bug: gives low quote counts
class QuotPunc(PatternColumn):
    def __init__(self):
        patterns = ((r'[\.,]\s*"',      'eng-un '),
                    (r'[\.,]\s*&quot;', 'eng-esc'),
                    (r'"\s*[\.,]',      'std-un '),
                    (r'&quot;\s*[\.,]', 'std-esc'))
        super().__init__('QuotPunc', 7+6, patterns)

# patterns: " vs &quot;
class Escaping(PatternColumn):
    def __init__(self):
        patterns = ((r'"',      'un '),
                    (r"'",      'un '),
                    (r' \| ',   'un '),
                    (r'&quot;', 'esc'),
                    (r'&apos;', 'esc'),
                    (r'&#124;', 'esc'))
        super().__init__('Escap', 3+6, patterns)

# some likely errors
class Weird(PatternColumn):
    def __init__(self):
        patterns = ((r'\+ \+', '+ +'),
                    (r'@ @',   '@ @'),
                    (r'\S@\S', 'a@a'))
        super().__init__('Weird', 3+6, patterns)

# xml tags
class Xml(PatternColumn):
    def __init__(self):
        patterns = ((r'<[^>]*>', 'xml'),)
        super().__init__('Xml', 5, patterns)

    @property
    def summary(self):
        return summarize_counter(self.counter, show_label=False, show_prop=False)

# <unk> occurrences
class Unk(PatternColumn):
    def __init__(self):
        patterns = ((r'[<&](unk|UNK)[>;]', 'unk'),)
        super().__init__('UNK', 5, patterns)

    @property
    def summary(self):
        return summarize_counter(self.counter, show_label=False, show_prop=False)


# print columnar summary

DEFAULT_COLUMNS = [FileName(), LineCount(), MaxLen(), FirstCase(), Numbers(),
                   BoundaryMarkers(), Clitics(), QuotPunc(), Escaping(), Weird(),
                   Hyphens(), Periods(), Xml(), Unk(), MultiCol()]

# check for patterns in 100k first lines
CHECK_FIRST = 1000#00

def summarize_files(file_paths, columns=DEFAULT_COLUMNS):
    headings = [column.heading for column in columns]
    print('  '.join(headings))
    for file_path in file_paths:
        # check that it exists
        if not os.path.exists(file_path):
            print('{} DOES NOT EXIST'.format(file_path))
            continue
        if os.path.isdir(file_path):
            print('{} is a directory'.format(file_path))
            continue
        # reset all columns
        for column in columns:
            column.new_file(file_path)
        # process the lines
        ext_lc = False
        for (i, line) in enumerate(open_text_file(file_path,
                                                  mode='rb',
                                                  encoding='utf-8')):
            if i < CHECK_FIRST:
                for column in columns:
                    column.process_line(i, line)
            else:
                if file_path.endswith('.gz'):
                    ext_lc = subprocess.check_output(
                        ['zcat {} | wc -l'.format(file_path)], shell=True).split()[0]
                else:
                    ext_lc = subprocess.check_output(['wc', '-l', file_path]).split()[0]
                ext_lc = int(ext_lc.decode('utf-8'))
                break
        if ext_lc:
            i = ext_lc
        else:
            i += 1  # 0 based enumerrate
        for column in columns:
            column.line_count(i)

        # print columns
        summaries = [column.format_summary() for column in columns]
        print('  '.join(summaries))
