import os

from .core.recipe import Rule, LoopRecipeFile
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
    def __init__(self,
                 shard_file, heldout_src, heldout_trg,
                 log_file, model_seckey, loop_indices,
                 save_every=2000, aux_type='none', argstr=''):
        assert all(x % save_every == 0 for x in loop_indices)
        self.models = LoopRecipeFile.loop_output(
            model_seckey[0], model_seckey[1], loop_indices)
        self.shard_file = shard_file
        self.heldout_src = heldout_src
        self.heldout_trg = heldout_trg
        self.log_file = log_file
        self.save_every = save_every
        self.aux_type = aux_type
        self.argstr = argstr

        inputs = [shard_file, heldout_src, heldout_trg]
        outputs = self.models + [log_file]
        super().__init__(inputs, outputs)

    def make(self, conf, cli_args):
        # loop index is appended by hnmt to given path
        model_base, _ = self.models[0](conf, cli_args).rsplit('.', 1)
        run('hnmt.py'
            ' --save-model {model_base}'
            ' --train {shard_file}'
            ' --heldout-source {heldout_src}'
            ' --heldout-target {heldout_trg}'
            ' --log-file {log_file}'
            ' --save-every {save_every}'
            ' --aux-type {aux_type}'
            ' {argstr}'.format(
                model_base=model_base,
                shard_file=self.shard_file(conf, cli_args),
                heldout_src=self.heldout_src(conf, cli_args),
                heldout_trg=self.heldout_trg(conf, cli_args),
                log_file=self.log_file(conf, cli_args),
                save_every=self.save_every,
                aux_type=self.aux_type,
                argstr=self.argstr))
        #'--validate-every 5 --translate-every 5 --backwards'

    def is_atomic(self, output):
        # all loop outputs are atomic
        return isinstance(output, LoopRecipeFile)

    def monitor(self, platform, conf, cli_args=None):
        highest = LoopRecipeFile.highest_written(
            self.outputs, conf, cli_args)
        if highest is None:
            'no output'
        return highest(conf, cli_args)


class Translate(Rule):
    def __init__(self,
                 model,
                 inp, out,
                 nbest=0,
                 beam=8,
                 alpha=0.01,
                 beta=0.4,
                 gamma=0.0,
                 len_smooth=5.0):
        self.model = model
        self.inp = inp
        self.out = out
        self.nbest = nbest
        self.beam = beam
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.len_smooth = len_smooth

        inputs = [model, inp]
        outputs = [out]
        super().__init__(inputs, outputs)

    def make(self, conf, cli_args):
        model = self.model(conf, cli_args)
        inp = self.inp(conf, cli_args)
        out = self.out(conf, cli_args)
        run('hnmt.py'
            ' --load-model {model}'
            ' --translate {inp}'
            ' --output {out}'
            ' --nbest-list {nbest}'
            ' --beam-size {beam}'
            ' --alpha {alpha}'
            ' --beta {beta}'
            ' --gamma {gamma}'
            ' --len-smooth {len_smooth}'.format(
                model=model,
                inp=inp,
                out=out,
                nbest=self.nbest,
                beam=self.beam,
                alpha=self.alpha,
                beta=self.beta,
                gamma=self.gamma,
                len_smooth=self.len_smooth))
