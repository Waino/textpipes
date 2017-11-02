""" Processing of WMT SGM file formats """
import collections
import re

from .components.core import MonoPipeComponent
from .core.recipe import Rule
from .core.utils import progress

RE_SET = re.compile(r'<([a-z]*)set setid="([^"]*)" ([^>]*)>', flags=re.IGNORECASE)
RE_DOC = re.compile(r'<doc sysid="([^"]*)" docid="([^"]*)" ([^>]*)>', flags=re.IGNORECASE)
RE_DOCEND = re.compile(r'</doc>', flags=re.IGNORECASE)
RE_SEG = re.compile(r'<seg id="([^"]*)">(.*)</seg>', flags=re.IGNORECASE)

RE_BLEU = re.compile(r'BLEU score using 4-grams = ([0-9.]*) for system "([^"]*)" '
                     r'on segment ([^ ]*) of document "([^"]*)" (.*)')

FMT_SET = '<{settype}set setid="{setid}" {tail}>\n'
FMT_DOC = '<doc sysid="{sysid}" docid="{docid}" {tail}>\n'
FMT_DOC_MULTIREF = '<doc sysid="REF{sysid}" docid="{docid}" {tail}>\n'
FMT_SEG = '<seg id="{segid}">{text}</seg>\n'
FMT_END = '</{settype}set>\n'

Segment = collections.namedtuple('Segment',
    ['sysid', 'docid', 'segid', 'text', 'tail'])

Bleu = collections.namedtuple('Bleu',
    ['sysid', 'docid', 'segid', 'bleu'])

def read_sgm(lines, meta=None):
    docid = None
    sysid = None
    tail = None
    for line in lines:
        line = line.strip()
        if meta is not None:
            m = RE_SET.match(line)
            if m:
                settype, setid, tail = m.groups()
                meta[settype] = settype
                meta[setid] = setid
                meta[tail] = tail
                continue
        m = RE_DOC.match(line)
        if m:
            sysid, docid, tail = m.groups()
            continue
        m = RE_SEG.match(line)
        if m:
            segid, text = m.groups()
            yield Segment(sysid, docid, segid, text, tail)

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
                _, _, tail = m.groups()
                outfobj.write(FMT_SET.format(
                    settype='ref', setid=self.setid, tail=tail))
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


class WrapInXml(MonoPipeComponent):
    def __init__(self,
                 template_xml=None, settype=None, setid=None, sysid=None,
                 srclang='any', trglang=None):
        side_inputs = [template_xml] if template_xml else []
        self.settype = settype
        self.setid = setid
        self.sysid = sysid
        self.srclang = srclang
        self.trglang = trglang

    def pre_make(self, side_fobjs):
        if self.template_xml:
            fobj = side_fobjs[self.template_xml]
            meta = {}
            self.template = read_sgm(lines, meta)
            if self.settype is None:
                self.settype = meta['settype']
            if self.setid is None:
                self.setid = meta['setid']
        else:
            self.template = self._running_numbers()

    def __call__(self, stream, side_fobjs=None,
                 config=None, cli_args=None):
        yield FMT_SET.format(
            settype=self.settype, setid=self.setid,
            tail='srclang="{src}" trglang="{trg}"'.format(
                src=self.srclang,
                trg=self.trglang)).rstrip()
        current_doc = None
        for line, tmpl in zip(stream, self.template):
            if tmpl.docid != current_doc:
                if current_doc is not None:
                    yield '</p>'
                    yield '</doc>'
                yield FMT_DOC.format(
                    sysid=self.sysid,
                    docid=current_doc,
                    tail=tmpl.tail).rstrip()
            yield FMT_SEG.format(
                segid=tmpl.segid,
                text=line).rstrip()
        yield '</p>'
        yield '</doc>'
        yield FMT_END.format(settype=self.settype).rstrip()


    def _running_numbers(self):
        i = 1
        while True:
            yield Segment(self.sysid, 'dummy', i, 'dummy')
            i += 1
