import textpipes as tp

recipe = tp.Recipe()

# all paths are in config, use interpolation
# section [paths.corpora] key foo
foo = recipe.use_input('corpora', 'foo')
bar = recipe.use_input('corpora', 'bar')

def preprocess(key, corpus):
    dp = recipe.add_rule(
        tp.ext.DummyPipe,
        inputs=corpus,
        outputs=('gen', '{}.dummypiped'.format(key)))
    return dp

foo_pre = preprocess('foo', foo)
bar_pre = preprocess('bar', bar)
