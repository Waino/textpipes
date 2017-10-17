import os

from .core.recipe import Rule
from .core.platform import run

class MakeVocabularies(Rule):
    def __init__(self, *args, argstr='', **kwargs):
        super().__init__(*args, **kwargs)
        self.argstr = argstr

    def make(self, conf, cli_args):
        src_corpus_file = self.inputs[0](conf, cli_args)
        trg_corpus_file = self.inputs[1](conf, cli_args)
        src_vocab_file = self.outputs[0](conf, cli_args)
        trg_vocab_file = self.outputs[1](conf, cli_args)
        run('make_vocabularies.py {src_corpus_file} {trg_corpus_file}'
            ' {src_vocab_file} {trg_vocab_file} {argstr}'.format(
                src_corpus_file=src_corpus_file,
                trg_corpus_file=trg_corpus_file,
                src_vocab_file=src_vocab_file,
                trg_vocab_file=trg_vocab_file,
                argstr=self.argstr))

class PrepareData(Rule):
    def __init__(self, *args, corpus='corpus', argstr='', **kwargs):
        super().__init__(*args, **kwargs)
        self.corpus = corpus
        self.argstr = argstr

    def make(self, conf, cli_args):
        src_corpus_file = self.inputs[0](conf, cli_args)
        trg_corpus_file = self.inputs[1](conf, cli_args)
        src_vocab_file = self.inputs[2](conf, cli_args)
        trg_vocab_file = self.inputs[3](conf, cli_args)
        shard_path = self.outputs[0](conf, cli_args)
        out_dir, shard_file = os.path.split(shard_path)
        run('prepare_data.py {corpus} {src_corpus_file} {trg_corpus_file}'
            ' {src_vocab_file} {trg_vocab_file}'
            ' --out-dir {out_dir} --shard-root {shard_file} {argstr}'.format(
                corpus=self.corpus,
                src_corpus_file=src_corpus_file,
                trg_corpus_file=trg_corpus_file,
                src_vocab_file=src_vocab_file,
                trg_vocab_file=trg_vocab_file,
                out_dir=out_dir,
                shard_file=shard_file,
                argstr=self.argstr))

class Train(Rule):
    pass

class Translate(Rule):
    pass
