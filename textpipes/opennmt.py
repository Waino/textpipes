import os

from .core.recipe import Rule, WildcardLoopRecipeFile
from .core.platform import run

class PrepareData(Rule):
    def __init__(self, *args, opennmt_dir='.', max_shard_size=262144, argstr='', **kwargs):
        super().__init__(*args, **kwargs)
        self.opennmt_dir = opennmt_dir
        self.max_shard_size = max_shard_size
        self.argstr = argstr

    def make(self, conf, cli_args):
        src_corpus_file = self.inputs[0](conf, cli_args)
        trg_corpus_file = self.inputs[1](conf, cli_args)
        src_dev_file = self.inputs[2](conf, cli_args)
        trg_dev_file = self.inputs[3](conf, cli_args)
        out_dir = self.outputs[0](conf, cli_args)
        run('{opennmt_dir}/preprocess.py'
            ' -train_src {src_corpus_file}' \
            ' -train_tgt {trg_corpus_file}' \
            ' -valid_src {src_dev_file}' \
            ' -valid_tgt {trg_dev_file}' \
            ' -save {out_dir}/sharded' \
            ' -max_shard_size {max_shard_size}'
            ' {argstr}'.format(
                opennmt_dir=self.opennmt_dir,
                corpus=self.corpus,
                src_corpus_file=src_corpus_file,
                trg_corpus_file=trg_corpus_file,
                src_dev_file=src_dev_file,
                trg_dev_file=trg_dev_file,
                out_dir=out_dir,
                max_shard_size=self.max_shard_size,
                argstr=self.argstr))

class Train(Rule):
    def __init__(self,
                 data_dir,
                 pipe_file,
                 model_seckey,
                 loop_indices,
                 argstr='',
                 opennmt_dir='.',
                 **kwargs):
        self.models = WildcardLoopRecipeFile.loop_output(
            model_seckey[0], model_seckey[1], loop_indices)
        self.data_dir = data_dir
        self.pipe_file = pipe_file
        self.argstr = argstr
        self.opennmt_dir = opennmt_dir

        inputs = [data_dir]
        outputs = self.models
        super().__init__(inputs, outputs, **kwargs)

    def make(self, conf, cli_args):
        # a lot of stuff is appended to model path
        run('{opennmt_dir}/train.py'
            ' -data {data_dir}/sharded'
            ' -save_model {model_base}'
            ' -gpuid 0 '
            ' -encoder_type brnn '
            ' -share_decoder_embeddings'
            ' {argstr}'
            ' >> {pipe_file} 2>&1'.format(
                opennmt_dir=self.opennmt_dir,
                model_base=self.model_base,
                data_dir=self.data_dir(conf, cli_args),
                argstr=self.argstr,
                pipe_file=self.pipe_file(conf, cli_args)))

    def is_atomic(self, output):
        # all loop outputs are atomic
        return isinstance(output, WildcardLoopRecipeFile)

    def monitor(self, platform, conf, cli_args=None):
        highest = WildcardLoopRecipeFile.highest_written(
            self.models, conf, cli_args)
        if highest is None:
            return 'no output'
        return highest(conf, cli_args)


class Translate(Rule):
    def __init__(self,
                 model,
                 inputs, outputs,
                 nbest=0,
                 beam=8,
                 alpha=0.01,
                 beta=0.4,
                 penalty_type='none',
                 argstr='',
                 opennmt_dir='.',
                 **kwargs):
        self.model = model
        self.translation_inputs = inputs
        self.outputs = outputs
        self.nbest = nbest
        self.beam = beam
        self.alpha = alpha
        self.beta = beta
        self.penalty_type = penalty_type
        self.argstr = argstr
        self.opennmt_dir = opennmt_dir

        all_inputs = [model] + inputs
        super().__init__(all_inputs, outputs, **kwargs)

    def make(self, conf, cli_args):
        model = self.model(conf, cli_args)
        # no support for translating multiple
        inputs = self.translation_inputs[0](conf, cli_args)
        outputs = self.outputs[0](conf, cli_args)
                  
        run('{opennmt_dir}/translate.py'
            ' -model {model}'
            ' -src {inp}'
            ' -output {out}'
            ' -n_best {nbest}'
            ' -beam_size {beam}'
            ' -alpha {alpha}'
            ' -beta {beta}'
            ' -coverage_penalty {penalty_type}'
            ' -length_penalty {penalty_type}'
            ' {argstr}'.format(
                opennmt_dir=self.opennmt_dir,
                model=model,
                inp=inputs,
                out=outputs,
                nbest=self.nbest,
                beam=self.beam,
                alpha=self.alpha,
                beta=self.beta,
                penalty_type=self.penalty_type,
                argstr=self.argstr))


# can reuse anmt Evaluate
