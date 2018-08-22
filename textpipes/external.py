import os
import subprocess

from .core.recipe import Rule
from .core.platform import run
from .core.utils import safe_zip

# FIXME: use package resources instead
WRAPPER_DIR = os.path.join(
    os.path.dirname(__file__), 'wrappers')

# "transparent" handling of gz for piping scripts
# choose between cat and zcat for input
def maybe_gz_in(infile):
    if infile.endswith('.gz'):
        return 'zcat', infile
    else:
        return 'cat', infile
# choose between tee and gzin for output
# tee used like this is a noop
def maybe_gz_out(outfile):
    if outfile.endswith('.gz'):
        return 'gzip', outfile
    else:
        return 'tee', outfile

def simple_external(name, inputs, outputs, template):
    """Helper to make integrating external tools easier"""
    assert '{argstr}' in template
    for inp_name in inputs:
        assert '{' + inp_name + '}' in template
    for out_name in outputs:
        assert '{' + out_name + '}' in template
    # FIXME: handle forbidding of .gz . fail in --check

    class SimpleExternalRule(Rule):
        def __init__(self, input_rfs, output_rfs, argstr='', **kwargs):
            super().__init__(input_rfs, output_rfs, **kwargs)
            self.argstr = argstr
            self._name = name
            assert len(self.inputs) == len(inputs)
            assert len(self.outputs) == len(outputs)

        def make(self, conf, cli_args):
            template_values = {}
            for inp_name, inp in safe_zip(inputs, self.inputs):
                template_values[inp_name] = inp(conf, cli_args)
            for out_name, out in safe_zip(outputs, self.outputs):
                template_values[out_name] = out(conf, cli_args)
            template_values['argstr'] = self.argstr
            run(template.format(**template_values))

        @property
        def name(self):
            return self._name

    return SimpleExternalRule

def ReEncode(infile, outfile, from_encoding='utf-8', to_encoding='utf-8//IGNORE'):
    argstr = '-f {from_encoding} -t {to_encoding}'.format(
        from_encoding=from_encoding, to_encoding=to_encoding)
    ReEncode = simple_external(
        'ReEncode', ['infile'], ['outfile'],
        'iconv {argstr} {infile} -o {outfile}')
    return ReEncode(infile, outfile, argstr=argstr)

class Concatenate(Rule):
    def __init__(self, *args, resource_class='make_immediately', **kwargs):
        super().__init__(*args, resource_class=resource_class, **kwargs)

    def make(self, conf, cli_args):
        infiles = [inp(conf, cli_args) for inp in self.inputs]
        if all(infile.endswith('.gz') for infile in infiles):
            catcmd = 'zcat'
        elif all(not infile.endswith('.gz') for infile in infiles):
            catcmd = 'cat'
        else:
            raise Exception('trying to concatenate gzipped and plain files')
        zipcmd, outfile = maybe_gz_out(self.outputs[0](conf, cli_args))
        run('{catcmd} {infiles} | {zipcmd} > {outfile}'.format(
                catcmd=catcmd,
                infiles=' '.join(infiles),
                zipcmd=zipcmd,
                outfile=outfile)
            )


class MosesTokenize(Rule):
    def __init__(self, *args, lang, **kwargs):
        super().__init__(*args, **kwargs)
        self.lang = lang

    def make(self, conf, cli_args):
        catcmd, infile = maybe_gz_in(self.inputs[0](conf, cli_args))
        zipcmd, outfile = maybe_gz_out(self.outputs[0](conf, cli_args))
        run('{catcmd} {infile}'
            ' | {moses_dir}/tokenizer.perl -l {lang} -threads 2'
            ' | {zipcmd} > {outfile}'.format(
                catcmd=catcmd,
                infile=infile,
                moses_dir=conf.platform.conf['paths']['moses_dir'],
                lang=self.lang,
                zipcmd=zipcmd,
                outfile=outfile))


class LearnBPE(Rule):
    def __init__(self, *args, vocabulary=10000, wordcounts=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.vocabulary = vocabulary
        self.wordcounts = wordcounts

    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        # FIXME: would be much better if this would fail in --check
        assert not infile.endswith('.gz')
        assert not outfile.endswith('.gz')
        run('{prog} --input {infile} --output {outfile}'
            ' --symbols {vocabulary} {wc}'.format(
                prog=os.path.join(WRAPPER_DIR, 'learn_bpe.py'),
                infile=infile,
                outfile=outfile,
                vocabulary=self.vocabulary,
                wc=' --dict-input' if self.wordcounts else ''
                ))

class ApplyBPE(Rule):
    def __init__(self, *args, sep='@@', **kwargs):
        super().__init__(*args, **kwargs)
        self.sep = sep

    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        codes = self.inputs[1](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        # FIXME: would be much better if this would fail in --check
        assert not infile.endswith('.gz')
        assert not codes.endswith('.gz')
        assert not outfile.endswith('.gz')
        run('{prog} --input {infile} --codes {codes} --output {outfile}'
            ' --separator "{sep}"'.format(
                prog=os.path.join(WRAPPER_DIR, 'apply_bpe.py'),
                infile=infile,
                codes=codes,
                outfile=outfile,
                sep=self.sep))


# FIXME: obsolete
class DummyPipe(Rule):
    def make(self, conf, cli_args):
        inpair = maybe_gz_in(self.inputs[0](conf, cli_args))
        outpair = maybe_gz_out(self.outputs[0](conf, cli_args))
        print('concrete files: {} {}'.format(inpair, outpair))
        # FIXME: use shell=True instead?
        subprocess.check_call(
            (os.path.join(WRAPPER_DIR, 'simple_pipe.sh'),)
            + inpair
            + ('tac',)  # this is the dummy command
            + outpair)
