import textpipes as tp

recipe = tp.Recipe()

# all paths are in config, use interpolation
# section [paths.corpora] key foo
foo = recipe.use_input('corpora', 'foo')
bar = recipe.use_input('corpora', 'bar')

recipe.add_rule(tp.ext.DummyPipe,
                inputs=foo,
                outputs=('gen', 'foo.dummypiped'))
