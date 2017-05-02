from .recipe import Rule
from .pipe import DeadEndPipe
from .components import truecaser

class SplitColumns(Rule):
    def __init__(self, inp, outputs, delimiter='\t'):
        super().__init__([inp], outputs)
        self.delimiter = delimiter

    def make(self, conf, cli_args=None):
        stream = self.inputs[0].open(conf, cli_args, mode='rb')
        writers = [out.open(conf, cli_args, mode='wb')
                   for out in self.outputs]
        for (i, line) in enumerate(stream):
            tpl = line.split(self.delimiter)
            if not len(tpl) == len(writers):
                raise Exception('line {}: Invalid number of columns '
                    'received {}, expecting {}'.format(
                    i, len(tpl), len(writers)))
            for (val, fobj) in zip(tpl, writers):
                fobj.write(val)
                fobj.write('\n')
        for fobj in [stream] + writers:
            fobj.close()

class TrainTrueCaserRule(DeadEndPipe):
    """Convenience Rule allowing easy training of truecaser
    from multiple corpora files."""
    def __init__(self, inputs, model_file, sure_thresh=.6):
        component = truecaser.TrainTrueCaser(model_file, sure_thresh)
        super().__init__([component], inputs)


# FIXME: this is not core, but I don't want to decide where to put it now
import collections
import re
RE_SET = re.compile(r'<refset setid="([^"]*)" ([^>]*)>')
RE_DOC = re.compile(r'<doc sysid="([^"]*)" docid="([^"]*)" ([^>]*)>')
RE_DOCEND = re.compile(r'</doc>')
RE_SEG = re.compile(r'<seg id="([^"]*)">(.*)</seg>')

FMT_SET = '<refset setid="{setid}" {tail}>\n'
FMT_DOC = '<doc sysid="REF{sysid}" docid="{docid}" {tail}>\n'
FMT_SEG = '<seg id="{segid}">{text}</seg>\n'

Segment = collections.namedtuple('Segment',
    ['sysid', 'docid', 'segid', 'text'])

class MergeXmlRefs(Rule):
    def __init__(self, inputs, output, setid):
        super().__init__(inputs, [output])
        self.setid = setid

    def read_sgm(self, lines):
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

    def merge_sgm(self, lines, outfobj, alt_refs):
        # FIXME: mangle sysid, detect docend, output everything into outfile
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
                outfobj.write(FMT_DOC.format(
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
                    outfobj.write(FMT_DOC.format(
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
            for seg in self.read_sgm(inp.open(conf, cli_args, mode='rb')):
                alt_refs[(i, seg.docid)][seg.segid] = seg.text
        # loop over main ref, output each doc in turn
        lines = self.inputs[0].open(conf, cli_args, mode='rb')
        with self.outputs[0].open(conf, cli_args, mode='wb') as outfobj:
            self.merge_sgm(lines, outfobj, alt_refs)
