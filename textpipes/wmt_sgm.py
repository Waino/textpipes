""" Processing of WMT SGM file formats """
import collections
import re

from .core.recipe import Rule
from .core.utils import progress

RE_SET = re.compile(r'<refset setid="([^"]*)" ([^>]*)>')
RE_DOC = re.compile(r'<doc sysid="([^"]*)" docid="([^"]*)" ([^>]*)>')
RE_DOCEND = re.compile(r'</doc>')
RE_SEG = re.compile(r'<seg id="([^"]*)">(.*)</seg>')

RE_BLEU = re.compile(r'BLEU score using 4-grams = ([0-9.]*) for system "([^"]*)" '
                     r'on segment ([^ ]*) of document "([^"]*)" (.*)')

FMT_SET = '<refset setid="{setid}" {tail}>\n'
FMT_DOC = '<doc sysid="{sysid}" docid="{docid}" {tail}>\n'
FMT_DOC_MULTIREF = '<doc sysid="REF{sysid}" docid="{docid}" {tail}>\n'
FMT_SEG = '<seg id="{segid}">{text}</seg>\n'

Segment = collections.namedtuple('Segment',
    ['sysid', 'docid', 'segid', 'text'])

Bleu = collections.namedtuple('Bleu',
    ['sysid', 'docid', 'segid', 'bleu'])

def read_sgm(lines):
    docid = None
    sysid = None
    for line in lines:
        line = line.strip()
        m = RE_DOC.match(line)
        if m:
            sysid, docid, _ = m.groups()
            continue
        m = RE_SEG.match(line)
        if m:
            segid, text = m.groups()
            yield Segment(sysid, docid, segid, text)

def read_bleu(lines):
    for line in lines:
        line = line.strip()
        m = RE_BLEU.match(line)
        if m:
            bleu, sysid, segid, docid, _ = m.groups()
            yield Bleu(sysid, docid, segid, float(bleu))
    

class MergeXmlRefs(Rule):
    def __init__(self, inputs, output, setid, resource_class='short'):
        super().__init__(inputs, [output], resource_class=resource_class)
        self.setid = setid

    def merge_sgm(self, lines, outfobj, alt_refs):
        docid = None
        tail = None
        segids = []
        for line in lines:
            line = line.strip()
            m = RE_SET.match(line)
            if m:
                _, tail = m.groups()
                outfobj.write(FMT_SET.format(
                    setid=self.setid, tail=tail))
                continue
            m = RE_DOC.match(line)
            if m:
                _, docid, tail = m.groups()
                outfobj.write(FMT_DOC_MULTIREF.format(
                    sysid=0, docid=docid, tail=tail))
                continue
            m = RE_SEG.match(line)
            if m:
                segid, text = m.groups()
                segids.append(segid)
                outfobj.write(line)
                outfobj.write('\n')
                continue
            m = RE_DOCEND.match(line)
            if m:
                # end the main ref
                outfobj.write(line)
                outfobj.write('\n')
                for i in range(1, len(self.inputs)):
                    if not (i, docid) in alt_refs:
                        continue
                    outfobj.write(FMT_DOC_MULTIREF.format(
                        sysid=i, docid=docid, tail=tail))
                    for segid in segids:
                        outfobj.write(FMT_SEG.format(
                           segid=segid, text=alt_refs[(i, docid)][segid]))
                    outfobj.write(line)
                    outfobj.write('\n')
                segids = []
                continue
            # implicit else
            outfobj.write(line)
            outfobj.write('\n')

    def make(self, conf, cli_args=None):
        alt_refs = collections.defaultdict(dict)
        # read in alt refs, indexed by doc+seg id
        for (i, inp) in enumerate(self.inputs):
            if i == 0:
                continue
            for seg in read_sgm(inp.open(conf, cli_args, mode='rb')):
                alt_refs[(i, seg.docid)][seg.segid] = seg.text
        # loop over main ref, output each doc in turn
        lines = self.inputs[0].open(conf, cli_args, mode='rb')
        with self.outputs[0].open(conf, cli_args, mode='wb') as outfobj:
            lines = progress(lines, self, conf,
                             self.outputs[0](conf, cli_args),
                             total=None)
            self.merge_sgm(lines, outfobj, alt_refs)
