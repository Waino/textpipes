import os

from .core.recipe import Rule, LoopRecipeFile
from .core.platform import run

class MakeVocabularies(Rule):
    def __init__(self, *args, argstr='', **kwargs):
        super().__init__(*args, **kwargs)
        self.argstr = argstr

    def make(self, conf, cli_args):
        src_corpus_file = self.inputs[0](conf, cli_args)
        lat_corpus_file = self.inputs[1](conf, cli_args)
        trg_corpus_file = self.inputs[2](conf, cli_args)
        src_vocab_file = self.outputs[0](conf, cli_args)
        lat_vocab_file = self.outputs[1](conf, cli_args)
        trg_vocab_file = self.outputs[2](conf, cli_args)
        run('make_vocabularies_latent.py'
            ' {src_corpus_file} {lat_corpus_file} {trg_corpus_file}'
            ' {src_vocab_file} {lat_vocab_file} {trg_vocab_file} {argstr}'.format(
                src_corpus_file=src_corpus_file,
                lat_corpus_file=lat_corpus_file,
                trg_corpus_file=trg_corpus_file,
                src_vocab_file=src_vocab_file,
                lat_vocab_file=lat_vocab_file,
                trg_vocab_file=trg_vocab_file,
                argstr=self.argstr))

class PrepareData(Rule):
    def __init__(self, *args, corpus='corpus', argstr='', **kwargs):
        super().__init__(*args, **kwargs)
        self.corpus = corpus
        self.argstr = argstr

    def make(self, conf, cli_args):
        src_corpus_file = self.inputs[0](conf, cli_args)
        lat_corpus_file = self.inputs[1](conf, cli_args)
        trg_corpus_file = self.inputs[2](conf, cli_args)
        src_vocab_file = self.inputs[3](conf, cli_args)
        lat_vocab_file = self.inputs[4](conf, cli_args)
        trg_vocab_file = self.inputs[5](conf, cli_args)
        shard_path = self.outputs[0](conf, cli_args)
        out_dir, shard_file = os.path.split(shard_path)
        run('prepare_data_latent.py {corpus}'
            ' {src_corpus_file} {lat_corpus_file} {trg_corpus_file}'
            ' {src_vocab_file} {lat_vocab_file} {trg_vocab_file}'
            ' --out-dir {out_dir} --shard-root {shard_file} {argstr}'.format(
                corpus=self.corpus,
                src_corpus_file=src_corpus_file,
                lat_corpus_file=lat_corpus_file,
                trg_corpus_file=trg_corpus_file,
                src_vocab_file=src_vocab_file,
                lat_vocab_file=lat_vocab_file,
                trg_vocab_file=trg_vocab_file,
                out_dir=out_dir,
                shard_file=shard_file,
                argstr=self.argstr))

class Train(Rule):
    def __init__(self,
                 shard_file,
                 heldout_src, heldout_lat, heldout_trg,
                 log_file, pipe_file,
                 model_seckey, loop_indices,
                 save_every=2000, aux_type='mtl', argstr='',
                 **kwargs):
        assert all(x % save_every == 0 for x in loop_indices)
        self.models = LoopRecipeFile.loop_output(
            model_seckey[0], model_seckey[1], loop_indices)
        self.shard_file = shard_file
        self.heldout_src = heldout_src
        self.heldout_lat = heldout_lat
        self.heldout_trg = heldout_trg
        self.log_file = log_file
        self.pipe_file = pipe_file
        self.save_every = save_every
        self.aux_type = aux_type
        self.argstr = argstr

        inputs = [shard_file, heldout_src, heldout_trg]
        outputs = self.models + [log_file]
        super().__init__(inputs, outputs, **kwargs)

    def make(self, conf, cli_args):
        # loop index is appended by anmt to given path
        model_base, _ = self.models[0](conf, cli_args).rsplit('.', 1)
        run('anmt_latent'
            ' --save-model {model_base}'
            ' --train {shard_file}'
            ' --heldout-source {heldout_src}'
            ' --heldout-latent {heldout_lat}'
            ' --heldout-target {heldout_trg}'
            ' --log-file {log_file}'
            ' --save-every {save_every}'
            ' --aux-type {aux_type}'
            ' {argstr}'
            ' >> {pipe_file} 2>&1'.format(
                model_base=model_base,
                shard_file=self.shard_file(conf, cli_args),
                heldout_src=self.heldout_src(conf, cli_args),
                heldout_lat=self.heldout_lat(conf, cli_args),
                heldout_trg=self.heldout_trg(conf, cli_args),
                log_file=self.log_file(conf, cli_args),
                save_every=self.save_every,
                aux_type=self.aux_type,
                argstr=self.argstr,
                pipe_file=self.pipe_file(conf, cli_args)))
        #'--validate-every 5 --translate-every 5 --backwards'

    def is_atomic(self, output):
        # all loop outputs are atomic
        return isinstance(output, LoopRecipeFile)

    def monitor(self, platform, conf, cli_args=None):
        highest = LoopRecipeFile.highest_written(
            self.models, conf, cli_args)
        if highest is None:
            return 'no output'
        return highest(conf, cli_args)


class Translate(Rule):
    def __init__(self,
                 model,
                 inputs, outputs, latent_outputs,
                 nbest=0,
                 beam=8,
                 alpha=0.01,
                 beta=0.4,
                 gamma=0.0,
                 len_smooth=5.0,
                 argstr='',
                 **kwargs):
        self.model = model
        self.translation_inputs = inputs
        self.translation_outputs = outputs
        self.latent_outputs = latent_outputs
        assert(len(inputs) == len(outputs))
        assert(len(inputs) == len(latent_outputs))
        self.nbest = nbest
        self.beam = beam
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.len_smooth = len_smooth
        self.argstr = argstr

        all_inputs = [model] + inputs
        all_outputs = outputs + latent_outputs
        super().__init__(all_inputs, all_outputs, **kwargs)

    def make(self, conf, cli_args):
        model = self.model(conf, cli_args)
        inputs = ','.join(inp(conf, cli_args)
                          for inp in self.translation_inputs)
        outputs = ','.join(out(conf, cli_args)
                          for out in self.translation_outputs)
        latent = ','.join(out(conf, cli_args)
                          for out in self.latent_outputs)
        run('anmt_latent'
            ' --load-model {model}'
            ' --translate {inp}'
            ' --output {out}'
            ' --output-latent {latent}'
            ' --nbest-list {nbest}'
            ' --beam-size {beam}'
            ' --alpha {alpha}'
            ' --beta {beta}'
            ' --gamma {gamma}'
            ' --len-smooth {len_smooth}'
            ' {argstr}'.format(
                model=model,
                inp=inputs,
                out=outputs,
                latent=latent,
                nbest=self.nbest,
                beam=self.beam,
                alpha=self.alpha,
                beta=self.beta,
                gamma=self.gamma,
                len_smooth=self.len_smooth,
                argstr=self.argstr))

class TranslateTwoStep(Rule):
    def __init__(self,
                 model,
                 inputs, 
                 outputs,
                 latent_inputs=None,
                 step='draft',
                 nbest=0,
                 beam=8,
                 alpha=0.01,
                 beta=0.4,
                 gamma=0.0,
                 len_smooth=5.0,
                 argstr='',
                 **kwargs):
        assert step in ('draft', 'final')
        self.step = step
        self.model = model
        self.translation_inputs = inputs
        self.translation_outputs = outputs
        self.latent_inputs = latent_inputs
        assert(len(inputs) == len(outputs))
        if self.step == 'draft':
            assert latent_inputs is None
        else:
            assert(len(inputs) == len(latent_inputs))
        self.nbest = nbest
        self.beam = beam
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.len_smooth = len_smooth
        self.argstr = argstr

        all_inputs = [model] + inputs
        if latent_inputs is not None:
            all_inputs.extend(latent_inputs)
        all_outputs = outputs
        super().__init__(all_inputs, all_outputs, **kwargs)

    def make(self, conf, cli_args):
        model = self.model(conf, cli_args)
        inputs = ','.join(inp(conf, cli_args)
                          for inp in self.translation_inputs)
        outputs = ','.join(out(conf, cli_args)
                          for out in self.translation_outputs)
        if self.step == 'draft':
            lat_str = '--output-aux'
        else:
            lat_str = '--translate-aux {}'.format(
                ','.join(out(conf, cli_args)
                for out in self.latent_inputs))
        run('anmt_latent'
            ' --step {step}'
            ' --load-model {model}'
            ' --translate {inp}'
            ' {latent}'
            ' --output {out}'
            ' --nbest-list {nbest}'
            ' --beam-size {beam}'
            ' --alpha {alpha}'
            ' --beta {beta}'
            ' --gamma {gamma}'
            ' --len-smooth {len_smooth}'
            ' {argstr}'.format(
                step=self.step,
                model=model,
                inp=inputs,
                latent=lat_str,
                out=outputs,
                nbest=self.nbest,
                beam=self.beam,
                alpha=self.alpha,
                beta=self.beta,
                gamma=self.gamma,
                len_smooth=self.len_smooth,
                argstr=self.argstr))
