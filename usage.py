import textpipes as tp

recipe = tp.Recipe()

# all paths are in config, use interpolation
# section [paths.corpora] key foo
foo = recipe.add_input('corpora', 'foo')
bar = recipe.add_input('corpora', 'bar')

def preprocess(key, corpus):
    dp, = recipe.add_rule(
        tp.external.DummyPipe,
        inputs=corpus,
        outputs=recipe.add_output('gen', '{}.dummypiped'.format(key)))
    return dp

foo_pre = preprocess('foo', foo)
bar_pre = preprocess('bar', bar)

# debug
conf = tp.Config('dummy.ini')
print(recipe.get_next_step_for(conf, foo_pre))

print(recipe.make_output(conf, foo_pre))
