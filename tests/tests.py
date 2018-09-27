import textpipes as tp

recipe = tp.Recipe()
conf = recipe.conf

# this test suite contains a minimal graph to test the components
# it does NOT represent recommended usage in a preprocessing pipeline

# most external tools (anmt, ) are not tested here

### individual text input

ind_rules = (
    # counting.py
    ('count_tokens', tp.counting.CountTokens, {}),
    ('count_chars', tp.counting.CountChars, {}),
    # truecaser.py
    ('train_truecaser', tp.truecaser.TrainTrueCaser, {}),
    ('lowercase', tp.apply_component(tp.truecaser.LowerCase()), {}),
    # external.py
    ('reencode', tp.external.ReEncode, {'from_encoding': 'ISO_8859-15'}),
    ### built-in features
    # transparent gzip
    ('gzip', tp.dummy.DummyParamPrint, {'name': 'gzip'}),
    # template subconf
    ('template_subconf1', tp.dummy.DummyParamPrint, {'name': 'subconf1'}),
    ('template_subconf2', tp.dummy.DummyParamPrint, {'name': 'subconf2'}),
    )

for name, rule, kwargs in ind_rules:
    inp = recipe.add_input('inputs', name)
    out = recipe.add_output('outputs', name, main=True)
    recipe.add_rule(rule(inp, out, **kwargs))

#### multiple inputs or outputs

# CountTokens with words_only
name = 'count_tokens2'
inp = recipe.add_input('inputs', name)
out = recipe.add_output('outputs', name, main=True)
words = recipe.add_output('outputs', 'count_tokens2_words', main=True)
recipe.add_rule(tp.counting.CountTokens(inp, out, words_only=words))

#### dep on previous steps
dep_rules = (
    ('remove_counts', tp.counting.RemoveCounts, recipe.use_output('outputs', 'count_tokens'), {}),
    ('scale_counts', tp.counting.ScaleCounts, recipe.use_output('outputs', 'count_tokens'), {'scale': 1.5}),
    ('combine_counts', tp.counting.CombineCounts,
        [recipe.use_output('outputs', 'count_tokens'),
         recipe.use_output('outputs', 'count_tokens2')], {}),
    ('combine_counts_balance', tp.counting.CombineCounts,
        [recipe.use_output('outputs', 'count_tokens'),
         recipe.use_output('outputs', 'count_tokens2')], {'balance': True}),
    # morfessor.py
    ('train_morfessor', tp.morfessor.TrainMorfessor, recipe.use_output('outputs', 'count_tokens'),
        {'argstr': '--traindata-list -w 0.4'}),
    # truecaser.py
    ('truecase', tp.apply_component(tp.truecaser.TrueCase(
        model_file=recipe.use_output('outputs', 'train_truecaser'))),
        recipe.add_input('inputs', 'truecase'), {}),
    )

for name, rule, inputs, kwargs in dep_rules:
    out = recipe.add_output('outputs', name, main=True)
    recipe.add_rule(rule(inputs, out, **kwargs))

#### dep on previous steps, second iteration
dep_rules2 = (
    ('combine_wordlists', tp.counting.CombineWordlists, 
        [recipe.use_output('outputs', 'remove_counts'),
         recipe.use_output('outputs', 'count_tokens2_words')], {}),
    # truecaser.py
    ('detruecase', tp.apply_component(tp.truecaser.DeTrueCase()),
        recipe.use_output('outputs', 'truecase'), {}),
    )
for name, rule, inputs, kwargs in dep_rules2:
    out = recipe.add_output('outputs', name, main=True)
    recipe.add_rule(rule(inputs, out, **kwargs))
#SegmentCountsFile (dep on segmentations)

#dep_components =
#FilterCounts

recipe.main()
