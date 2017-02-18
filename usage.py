import textpipes as tp

recipe = tp.Recipe()

foo = recipe.add_input('data/corpora/foo.{src_lang}.gz')
bar = recipe.add_input('data/corpora/bar.{src_lang}.gz')

recipe.add_rule(tp.ext.DummyPipe,
                inputs=foo,
                outputs='data/generated/foo.dummypiped.{src_lang}.gz')
