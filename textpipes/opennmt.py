# -*- coding: utf-8 -*-
import os

from .core.recipe import Rule, WildcardLoopRecipeFile
from .core.platform import run
from .core.utils import find_highest_file
from .components.core import SingleCellComponent

class PrepareData(Rule):
    def __init__(self, *args, opennmt_dir='.', argstr='', **kwargs):
        super().__init__(*args, **kwargs)
        self.opennmt_dir = opennmt_dir
        self.argstr = argstr
        self.add_opt_dep(self.opennmt_dir + '/preprocess.py', binary=True)

    def make(self, conf, cli_args):
        src_corpus_file = self.inputs[0](conf, cli_args)
        trg_corpus_file = self.inputs[1](conf, cli_args)
        src_dev_file = self.inputs[2](conf, cli_args)
        trg_dev_file = self.inputs[3](conf, cli_args)
        out_dir = self.outputs[0](conf, cli_args)
        pipe_file = self.outputs[1](conf, cli_args)
        os.makedirs(out_dir, exist_ok=True)
        run('{opennmt_dir}/preprocess.py'
            ' -train_src {src_corpus_file}' \
            ' -train_tgt {trg_corpus_file}' \
            ' -valid_src {src_dev_file}' \
            ' -valid_tgt {trg_dev_file}' \
            ' -save_data {out_dir}/sharded' \
            ' {argstr}'
            ' >> {pipe_file} 2>&1'.format(
                opennmt_dir=self.opennmt_dir,
                src_corpus_file=src_corpus_file,
                trg_corpus_file=trg_corpus_file,
                src_dev_file=src_dev_file,
                trg_dev_file=trg_dev_file,
                out_dir=out_dir,
                pipe_file=pipe_file,
                argstr=self.argstr))


class Train(Rule):
    def __init__(self,
                 data_dir,
                 pipe_file,
                 model_seckey,
                 loop_indices,
                 model_base,
                 argstr='',
                 opennmt_dir='.',
                 save_every=5000,
                 embs_src=None,
                 embs_trg=None,
                 **kwargs):
        assert all(x % save_every == 0 for x in loop_indices)
        self.models = WildcardLoopRecipeFile.loop_output(
            model_seckey[0], model_seckey[1], loop_indices)
        self.save_every = save_every
        self.data_dir = data_dir
        self.pipe_file = pipe_file
        self.model_base = model_base
        self.argstr = argstr
        self.opennmt_dir = opennmt_dir
        self.embs_src = embs_src
        self.embs_trg = embs_trg

        inputs = [data_dir]
        if embs_src is not None:
            inputs.append(embs_src)
        if embs_trg is not None:
            inputs.append(embs_trg)
        outputs = self.models + [pipe_file]
        super().__init__(inputs, outputs, **kwargs)
        self.add_opt_dep(self.opennmt_dir + '/train.py', binary=True)

    # FIXME: use num training steps to finish
    def make(self, conf, cli_args):
        # a lot of stuff is appended to model path
        assert self.models[0](conf, cli_args).startswith(self.model_base)
        resume_str = self.resume()
        embs_str = self.embs(conf, cli_args)
        run('{opennmt_dir}/train.py'
            ' -data {data_dir}/sharded'
            ' -save_model {model_base}'
            ' -save_checkpoint_steps {save_every}'
            ' {resume}'
            ' {embs}'
            ' -gpu_ranks 0 '
            ' {argstr}'
            ' >> {pipe_file} 2>&1'.format(
                opennmt_dir=self.opennmt_dir,
                model_base=self.model_base,
                save_every=self.save_every,
                resume=resume_str,
                embs=embs_str,
                data_dir=self.data_dir(conf, cli_args),
                argstr=self.argstr,
                pipe_file=self.pipe_file(conf, cli_args)))

    def resume(self):
        # also includes values outside chosen loop indices
        idx, model = find_highest_file(self.model_base, r'_step_([0-9]+)\.pt')
        if idx is not None:
            return '-train_from {model}'.format(model=model)
        # nothing to resume
        return ''
    
    def embs(self, conf, cli_args):
        result = ''
        if self.embs_src is not None:
            result += '--pre_word_vecs_enc {} '.format(self.embs_src(conf, cli_args))
        if self.embs_trg is not None:
            result += '--pre_word_vecs_dec {}'.format(self.embs_trg(conf, cli_args))
        return result

    def is_atomic(self, output):
        # all loop outputs are atomic
        return isinstance(output, WildcardLoopRecipeFile)

    def monitor(self, platform, conf, cli_args=None):
        highest = WildcardLoopRecipeFile.highest_written(
            self.models, conf, cli_args)
        if highest is None:
            return 'no output'
        return highest(conf, cli_args)


# train a single ep at a time using a chain of short jobs
class TrainShort(Rule):
    def __init__(self,
                 data_dir,
                 pipe_file,
                 prev_model,
                 model,
                 model_base,
                 argstr='',
                 opennmt_dir='.',
                 epochs_per_job=1,
                 **kwargs):
        self.prev_model = prev_model
        self.model = model
        self.model_base = model_base
        self.data_dir = data_dir
        self.pipe_file = pipe_file
        self.argstr = argstr
        self.opennmt_dir = opennmt_dir
        self.epochs_per_job = epochs_per_job

        inputs = [data_dir]
        if prev_model is not None:
            inputs.append(prev_model)
        outputs = [self.model]
        super().__init__(inputs, outputs, **kwargs)
        self.add_opt_dep(self.opennmt_dir + '/train.py', binary=True)

    def make(self, conf, cli_args):
        # a lot of stuff is appended to model path
        assert self.model(conf, cli_args).startswith(self.model_base)
        resume_str = self.resume(self.epochs_per_job)
        if resume_str != '':
            print('Resuming with: {}'.format(resume_str))
        run('{opennmt_dir}/train.py'
            ' -data {data_dir}/sharded'
            ' -save_model {model_base}'
            ' {resume}'
            ' -gpuid 0 '
            ' -encoder_type brnn '
            ' -share_decoder_embeddings'
            ' {argstr}'
            ' >> {pipe_file} 2>&1'.format(
                opennmt_dir=self.opennmt_dir,
                model_base=self.model_base,
                resume=resume_str,
                data_dir=self.data_dir(conf, cli_args),
                argstr=self.argstr,
                pipe_file=self.pipe_file(conf, cli_args)))

    def resume(self, epochs_per_job=1):
        # also includes values outside chosen loop indices
        idx, model = find_highest_file(self.model_base, r'.*e([0-9]+)\.pt')
        if idx is not None:
            return '-train_from {model} -start_epoch {beg} -epochs {end}'.format(
                model=model, beg=idx + 1, end = idx + epochs_per_job)
        # nothing to resume
        return '-epochs {}'.format(epochs_per_job)

    def is_atomic(self, output):
        # all loop outputs are atomic
        return isinstance(output, WildcardLoopRecipeFile)


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
        self.add_opt_dep(self.opennmt_dir + '/translate.py', binary=True)

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
            ' -gpu 0 '
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


class TranslateEnsemble(Rule):
    def __init__(self,
                 models,
                 inputs, outputs,
                 nbest=0,
                 beam=8,
                 alpha=0.01,
                 beta=0.4,
                 penalty_type='none',
                 argstr='',
                 opennmt_dir='.',
                 **kwargs):
        self.models = models
        self.translation_inputs = inputs
        self.outputs = outputs
        self.nbest = nbest
        self.beam = beam
        self.alpha = alpha
        self.beta = beta
        self.penalty_type = penalty_type
        self.argstr = argstr
        self.opennmt_dir = opennmt_dir

        all_inputs = models + inputs
        super().__init__(all_inputs, outputs, **kwargs)
        self.add_opt_dep(self.opennmt_dir + '/translate.py', binary=True)

    def make(self, conf, cli_args):
        # now uses nargs='+' format, with flag -model only once
        models_str = ' '.join([model(conf, cli_args)
                               for model in self.models])
        # no support for translating multiple
        inputs = self.translation_inputs[0](conf, cli_args)
        outputs = self.outputs[0](conf, cli_args)

        run('{opennmt_dir}/translate.py'
            ' -model {models}'
            ' -src {inp}'
            ' -output {out}'
            ' -n_best {nbest}'
            ' -beam_size {beam}'
            ' -alpha {alpha}'
            ' -beta {beta}'
            ' -coverage_penalty {penalty_type}'
            ' -length_penalty {penalty_type}'
            ' -gpu 0 '
            ' {argstr}'.format(
                opennmt_dir=self.opennmt_dir,
                models=models_str,
                inp=inputs,
                out=outputs,
                nbest=self.nbest,
                beam=self.beam,
                alpha=self.alpha,
                beta=self.beta,
                penalty_type=self.penalty_type,
                argstr=self.argstr))


# can reuse anmt Evaluate


class AddLookupFeature(SingleCellComponent):
    """Adds a feature to each token, based on a mapping file"""
    def __init__(self, feature_map, sep='￨', fallback='unk', boundary_marker='@@', **kwargs):
        super().__init__(side_inputs=[feature_map], **kwargs)
        self.feature_map_file = feature_map
        self.sep = sep
        assert fallback in ('unk', 'copy')
        self.fallback = fallback
        self.boundary_marker = boundary_marker
        self.mapping = {}

    def pre_make(self, side_fobjs):
        for line in side_fobjs[self.feature_map_file]:
            src, trg = line.split('\t')
            self.mapping[src] = trg

    def single_cell(self, line):
        result = []
        for token in line.split():
            # FIXME: extract first feature from input, to enable adding multiple?
            no_bnd = token.replace(self.boundary_marker, '')
            fallback = '<UNK>' if self.fallback == 'unk' else no_bnd
            mapped = self.mapping.get(no_bnd, fallback)
            result.append('{}{}{}'.format(token, self.sep, mapped))
        return ' '.join(result)
