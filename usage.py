import textpipes as tp

recipe = tp.Recipe()

# all paths are in config, use interpolation
# section [paths.corpora] key foo
foo = recipe.add_input('corpora', 'foo')
bar = recipe.add_input('corpora', 'bar')

class SomePipe(MonoPipe):
    def __init__(self, inp, out, toolong):
        super(self).__init__(
            [tp.FilterByLength(min_tokens=1,
                               max_tokens=100,
                               max_chars=1500,
                               max_chars_per_token=80,
                               log_to=toolong)
             tp.RemoveLanguageTags(),
             tp.Clean(),
             tp.MapChars(),
             tp.Deduplicate()],
            [inp], [out], sie_outputs=[toolong])

def preprocess(key, corpus):
    dp, = recipe.add_rule(
        tp.external.DummyPipe,
        inputs=corpus,
        outputs=recipe.add_output('gen', '{}.dummypiped'.format(key)))
    sp = recipe.add_rule(
        SomePipe(FIXME))
    return dp


foo_pre = preprocess('foo', foo)
bar_pre = preprocess('bar', bar)

# debug
conf = tp.Config('dummy.ini')
print(recipe.get_next_step_for(conf, foo_pre))

print(recipe.make_output(conf, foo_pre))
