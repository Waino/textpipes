import textpipes as tp

recipe = tp.Recipe('usage')
conf = recipe.conf

# all paths are in config, use interpolation
# section [paths.corpora] key foo
foo = recipe.add_input('corpora', 'foo')
bar = recipe.add_input('corpora', 'bar')
para = [recipe.add_input('corpora', 'para.{}'.format(side))
        for side in ('src', 'tgt')]

parapiped = [recipe.add_output('gen', 'para.{}.parapiped'.format(side))
             for side in ('src', 'tgt')]
paradedup = [recipe.add_output('gen', 'para.{}.dedup'.format(side))
             for side in ('src', 'tgt')]

fbl = tp.FilterByLength(min_tokens=1,
                        max_tokens=80,
                        max_chars=250,
                        max_chars_per_token=80)

para_tokenizer = tp.PerColumn((
    tp.components.tokenizer.Tokenize(conf['exp']['src.lang']),
    tp.components.tokenizer.Tokenize(conf['exp']['tgt.lang']),
    ))

class SomePipe(tp.MonoPipe):
    def __init__(self, inp, out, toolong):
        super().__init__(
            [tp.MonoFilter(fbl, toolong),
             tp.components.europarl.RemoveLanguageTags(),
             tp.Clean(),
             tp.MapChars(),
            ],
            [inp], [out])

class ParaPipe(tp.ParallelPipe):
    def __init__(self, inp, out, toolong):
        super().__init__(
            [tp.ParallelFilter(fbl, toolong),
             tp.components.europarl.RemoveLanguageTags(),
             tp.Clean(),
             tp.MapChars(),
             para_tokenizer,
            ],
            inp, out)

def preprocess(key, corpus):
    dp, = recipe.add_rule(
        tp.external.DummyPipe(
            corpus,
            recipe.add_output('gen', '{}.dummypiped'.format(key))
        ))
    sp, _ = recipe.add_rule(
        SomePipe(
            dp,
            recipe.add_output('gen', '{}.somepiped'.format(key)),
            recipe.add_output('gen', '{}.toolong'.format(key))
        ))
    dedup, = recipe.add_rule(
        tp.Deduplicate(
            sp,
            recipe.add_output('gen', '{}.dedup'.format(key))
        ))
    return dedup

def paraprep(inputs, outputs):
    pp0, pp1, _ = recipe.add_rule(
        ParaPipe(inputs, outputs, recipe.add_output('gen', 'para.toolong'))
        )
    dedup = recipe.add_rule(
        tp.Deduplicate([pp0, pp1], paradedup))
    return dedup

# FIXME: loop job that produces numbered outputs
# - ability to continue from last save
# - human friendly output: combine into one line


foo_pre = preprocess('foo', foo)
bar_pre = preprocess('bar', bar)

pp = paraprep(para, parapiped)

# dummy training with foo
loop_indices = (2, 4, 6, 30)
foo_models = recipe.add_rule(
    tp.dummy.DummyTrainLoop(foo_pre, ('mod', 'foo.models'), loop_indices))

def eval(model, idx):
    ev, = recipe.add_rule(
        tp.external.DummyPipe(
            model,
            recipe.add_output('gen', 'foo.evals', loop_index=idx)
        ))
    return ev
foo_evals = [eval(model, idx)
             for (model, idx) in zip(foo_models, loop_indices)]

recipe.add_main_outputs([foo_pre, bar_pre] + list(pp) + foo_evals)

recipe.main()
