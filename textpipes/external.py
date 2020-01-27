import os
import subprocess

from .core.recipe import Rule, RecipeFile
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

def simple_external(name, inputs, outputs, template, autolog_stdout=True, mapping=None):
    """Helper to make integrating external tools easier"""
    uses_argstr = '{argstr}' in template
    for inp_name in inputs:
        if '{' + inp_name + '}' not in template:
            raise Exception('{' + inp_name + '} missing from template')
    for out_name in outputs:
        if '{' + out_name + '}' not in template:
            raise Exception('{' + out_name + '} missing from template')
    program, _ = template.split(' ', 1)
    if autolog_stdout and '>' in template:
        print('turning off autolog_stdout for {}'.format(name))
        autolog_stdout = False
    if autolog_stdout:
        template += ' >> {autolog} 2>&1'
    mapping = {} if mapping is None else mapping
    # FIXME: handle forbidding of .gz . fail in --check

    class SimpleExternalRule(Rule):
        def __init__(self, input_rfs, output_rfs, argstr='', extra_in=None, extra_out=None, **kwargs):
            if isinstance(output_rfs, RecipeFile):
                output_rfs = [output_rfs]
            extra_in = [] if extra_in is None else extra_in
            extra_out = [] if extra_out is None else extra_out
            # extra_out are output files that are not specified on the command line
            super().__init__(input_rfs + extra_in, output_rfs + extra_out, **kwargs)
            if not uses_argstr and argstr != '':
                raise Exception('No {argstr} in template, but argstr given')
            self.argstr = argstr
            self._name = name
            assert len(self.inputs) == len(inputs) + len(extra_in), \
                'got {} expecting {}'.format(len(self.inputs), len(inputs))
            assert len(self.outputs) == len(outputs) + len(extra_out)
            self.add_opt_dep(program, binary=True)

        def make(self, conf, cli_args):
            template_values = {}
            # had to go back to unsafe zip due to extra_in
            for inp_name, inp in zip(inputs, self.inputs):
                val = inp(conf, cli_args)
                if inp_name in mapping:
                    val = mapping[inp_name](val)
                template_values[inp_name] = val
            # had to go back to unsafe zip due to extra_out
            for out_name, out in zip(outputs, self.outputs):
                val = out(conf, cli_args)
                if out_name in mapping:
                    val = mapping[out_name](val)
                template_values[out_name] = val
            template_values['argstr'] = self.argstr
            if autolog_stdout:
                template_values['autolog'] = conf.current_autolog_path
            run(template.format(**template_values))

        @property
        def name(self):
            return self._name

    return SimpleExternalRule

Copy = simple_external('Copy', ['inp'], ['out'],
                       'cp --no-clobber {inp} {out}',  autolog_stdout=False)

def ReEncode(infile, outfile, from_encoding='utf-8', to_encoding='utf-8//IGNORE'):
    argstr = '-f {from_encoding} -t {to_encoding}'.format(
        from_encoding=from_encoding, to_encoding=to_encoding)
    ReEncode = simple_external(
        'ReEncode', ['infile'], ['outfile'],
        'iconv {argstr} {infile} > {outfile}',
        autolog_stdout=False)
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
            print('SLOW: concatenating gzipped and plain files')
            self._mixed_concat(conf, cli_args)
            return
        zipcmd, outfile = maybe_gz_out(self.outputs[0](conf, cli_args))
        run('{catcmd} {infiles} | {zipcmd} > {outfile}'.format(
                catcmd=catcmd,
                infiles=' '.join(infiles),
                zipcmd=zipcmd,
                outfile=outfile)
            )
    
    def _mixed_concat(self, conf, cli_args):
        with self.outputs[0].open(conf, cli_args, mode='w') as fobj:
            for inp in self.inputs:
                reader = inp.open(conf, cli_args, mode='r')
                for line in reader:
                    fobj.write(line)
                    fobj.write('\n')
                reader.close()

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
        self.program = os.path.join(WRAPPER_DIR, 'learn_bpe.py')
        self.add_opt_dep(self.program, binary=True)

    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        # FIXME: would be much better if this would fail in --check
        assert not infile.endswith('.gz')
        assert not outfile.endswith('.gz')
        run('python {prog} --input {infile} --output {outfile}'
            ' --symbols {vocabulary} {wc}'.format(
                prog=self.program,
                infile=infile,
                outfile=outfile,
                vocabulary=self.vocabulary,
                wc=' --dict-input' if self.wordcounts else ''
                ))

class ApplyBPE(Rule):
    def __init__(self, *args, bnd_marker='@@', **kwargs):
        super().__init__(*args, **kwargs)
        self.sep = bnd_marker

    def make(self, conf, cli_args):
        infile = self.inputs[0](conf, cli_args)
        codes = self.inputs[1](conf, cli_args)
        outfile = self.outputs[0](conf, cli_args)
        # FIXME: would be much better if this would fail in --check
        assert not infile.endswith('.gz')
        assert not codes.endswith('.gz')
        assert not outfile.endswith('.gz')
        run('python {prog} --input {infile} --codes {codes} --output {outfile}'
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
