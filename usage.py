import textpipes as tp

recipe = tp.Recipe()

# all paths are in config, use interpolation
# section [paths.corpora] key foo
foo = recipe.add_input('corpora', 'foo')
bar = recipe.add_input('corpora', 'bar')
para = [recipe.add_input('corpora', 'para.{}'.format(side))
        for side in ('src', 'tgt')]

parapiped = [recipe.add_output('gen', 'para.{}.parapiped'.format(side))
             for side in ('src', 'tgt')]

fbl = tp.FilterByLength(min_tokens=1,
                        max_tokens=80,
                        max_chars=250,
                        max_chars_per_token=80)

class SomePipe(tp.MonoPipe):
    def __init__(self, inp, out, toolong):
        super().__init__(
            [tp.MonoFilter(fbl, toolong),
             tp.components.europarl.RemoveLanguageTags(),
             tp.Clean(),
             tp.MapChars(),
            ],
            [inp], [out], side_outputs=[toolong])

class ParaPipe(tp.ParallelPipe):
    def __init__(self, inp, out, toolong):
        super().__init__(
            [tp.ParallelFilter(fbl, toolong),
             tp.components.europarl.RemoveLanguageTags(),
             tp.Clean(),
             tp.MapChars(),
            ],
            inp, out, side_outputs=[toolong])

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
    pp = recipe.add_rule(
        ParaPipe(inputs, outputs, recipe.add_output('gen', 'para.toolong'))
        )
    return pp


foo_pre = preprocess('foo', foo)
bar_pre = preprocess('bar', bar)

pp = paraprep(para, parapiped)
print(pp)

# debug
conf = tp.Config('dummy.ini')
nextstep = recipe.get_next_step_for(conf, foo_pre)
print(nextstep)

print(recipe.make_output(conf, nextstep[0].output))
