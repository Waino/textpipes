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
                 log_file, pipe_file,
                 model_seckey, loop_indices,
                 save_every=2000, aux_type='none', argstr='',
                 **kwargs):
        assert all(x % save_every == 0 for x in loop_indices)
        self.models = LoopRecipeFile.loop_output(
            model_seckey[0], model_seckey[1], loop_indices)
        self.shard_file = shard_file
        self.heldout_src = heldout_src
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
        run('anmt'
            ' --save-model {model_base}'
            ' --train {shard_file}'
            ' --heldout-source {heldout_src}'
            ' --heldout-target {heldout_trg}'
            ' --log-file {log_file}'
            ' --save-every {save_every}'
            ' --aux-type {aux_type}'
            ' {argstr}'
            ' >> {pipe_file} 2>&1'.format(
                model_base=model_base,
                shard_file=self.shard_file(conf, cli_args),
                heldout_src=self.heldout_src(conf, cli_args),
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
                 inputs, outputs,
                 nbest=0,
                 beam=8,
                 alpha=0.01,
                 beta=0.4,
                 gamma=0.0,
                 len_smooth=5.0,
                 **kwargs):
        self.model = model
        self.translation_inputs = inputs
        self.outputs = outputs
        assert(len(inputs) == len(outputs))
        self.nbest = nbest
        self.beam = beam
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.len_smooth = len_smooth

        all_inputs = [model] + inputs
        super().__init__(all_inputs, outputs, **kwargs)

    def make(self, conf, cli_args):
        model = self.model(conf, cli_args)
        inputs = ','.join(inp(conf, cli_args)
                          for inp in self.translation_inputs)
        outputs = ','.join(out(conf, cli_args)
                          for out in self.outputs)
        run('anmt'
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
                inp=inputs,
                out=outputs,
                nbest=self.nbest,
                beam=self.beam,
                alpha=self.alpha,
                beta=self.beta,
                gamma=self.gamma,
                len_smooth=self.len_smooth))

class Evaluate(Rule):
    def __init__(self,
                 inp_sgm,   # sgm source input
                 hyp_sgm,   # hyp sgm conversion output
                 ref_sgm,   # sgm ref (possibly multiref)
                 out_chrF1,
                 out_chrF2,
                 out_bleu,
                 trg_lang,
                 sys_name,
                 resource_class='make_immediately',
                 **kwargs):
        self.inp_sgm = inp_sgm
        self.hyp_sgm = hyp_sgm
        self.ref_sgm = ref_sgm
        self.out_chrF1 = out_chrF1
        self.out_chrF2 = out_chrF2
        self.out_bleu = out_bleu

        self.trg_lang = trg_lang
        self.sys_name = sys_name

        inputs = [inp_sgm, hyp_sgm, ref_sgm]
        outputs = [out_chrF1, out_chrF2, out_bleu]
        super().__init__(inputs, outputs,
                         resource_class=resource_class, **kwargs)

    def make(self, conf, cli_args):
        inp_sgm = self.inp_sgm(conf, cli_args)
        hyp_sgm = self.hyp_sgm(conf, cli_args)
        ref_sgm = self.ref_sgm(conf, cli_args)

        # wrap plain hyp in sgm
        #run('wrap-xml.perl'
        #    ' {trg_lang} {inp_sgm} {system}'
        #    ' < {hyp}'
        #    ' > {hyp_sgm}'.format(
        #        trg_lang=self.trg_lang,
        #        inp_sgm=inp_sgm,
        #        system=self.sys_name,
        #        hyp=hyp,
        #        hyp_sgm=hyp_sgm)
        # evaluate
        if self.out_chrF1 is not None:
            out = self.out_chrF1(conf, cli_args)
            run('chrF_sgm'
                ' -b 1.0'
                ' {hyp_sgm}'
                ' {ref_sgm}'
                ' > {out}'.format(
                    hyp_sgm=hyp_sgm,
                    ref_sgm=ref_sgm,
                    out=out))
        if self.out_chrF2 is not None:
            out = self.out_chrF2(conf, cli_args)
            run('chrF_sgm'
                ' -b 2.0'
                ' {hyp_sgm}'
                ' {ref_sgm}'
                ' > {out}'.format(
                    hyp_sgm=hyp_sgm,
                    ref_sgm=ref_sgm,
                    out=out))
        if self.out_bleu is not None:
            out = self.out_bleu(conf, cli_args)
            run('mteval-v13a.pl'
                ' -r {ref_sgm}'
                ' -s {inp_sgm}'
                ' -t {hyp_sgm}'
                ' -c -d 2'
                ' > {out}'.format(
                    ref_sgm=ref_sgm,
                    inp_sgm=inp_sgm,
                    hyp_sgm=hyp_sgm,
                    out=out))
