from .core.recipe import Rule, LoopRecipeFile
from .external import simple_external

# args: --emb-dim N --hidden-dim N --n-layers N --dropout-p X --learning-rate X
# --l2-regularization X --grad-clip X --batch-size N --epochs N --patience N
# --eval-every MB --save-every MB

class TrainLmclean(Rule):
    def __init__(self,
                 train_file, dev_file, pipe_file,
                 model_seckey,
                 save_every=2000, final=40000, argstr='',
                 **kwargs):
        loop_indices = [x + save_every for x in range(0, final, save_every)]
        self.models = LoopRecipeFile.loop_output(
            model_seckey[0], model_seckey[1], loop_indices)
        self.train_file = train_file
        self.dev_file = dev_file
        self.pipe_file = pipe_file
        self.argstr = argstr

        inputs = [train_file, dev_file]
        outputs = self.models
        super().__init__(inputs, outputs, **kwargs)
        self.add_opt_dep('lmclean-train', binary=True)

    def make(self, conf, cli_args):
        model_base, _ = self.models[0](conf, cli_args).rsplit('.', 1)
        run('lmclean-train'
            ' {model_base} {train_file} {dev_file} --save-every {save_every} {argstr}'
            ' >> {pipe_file} 2>&1'.format(
                model_base=model_base,
                train_file=self.train_file(conf, cli_args),
                dev_file=self.dev_file(conf, cli_args),
                save_every=self.save_every,
                argstr=self.argstr,
                pipe_file=self.pipe_file(conf, cli_args)))

    def is_atomic(self, output):
        # all loop outputs are atomic
        return isinstance(output, LoopRecipeFile)

    def monitor(self, platform, conf, cli_args=None):
        highest = LoopRecipeFile.highest_written(
            self.models, conf, cli_args)
        if highest is None:
            return 'no output'
        return highest(conf, cli_args)

EvalLmclean = simple_external(
    'EvalLmclean', ['model' 'testfile'], ['scorefile'],
    'lmclean-eval {model} {testfile} --print-params {argstr} > {scorefile}',
    auto_log=False)

ScoreLmclean = simple_external(
    'ScoreLmclean', ['model' 'testfile'], ['scorefile'],
    'lmclean-score {model} {testfile} --output {scorefile} {argstr}')
