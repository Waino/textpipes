import textpipes as tp

recipe = tp.Recipe()
conf = recipe.conf

# most external tools (anmt, ) are not tested here

### individual text input

ind_rules = (
    ('count_tokens', tp.counting.CountTokens),
    ('count_chars', tp.counting.CountChars),
    )

for name, rule in ind_rules:
    inp = recipe.add_input('inputs', name)
    out = recipe.add_output('outputs', name, main=True)
    recipe.add_rule(rule(inp, out))

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
    )

for name, rule, inputs, kwargs in dep_rules:
    out = recipe.add_output('outputs', name, main=True)
    recipe.add_rule(rule(inputs, out, **kwargs))

#### dep on previous steps, second iteration
dep_rules2 = (
    ('combine_wordlists', tp.counting.CombineWordlists, 
        [recipe.use_output('outputs', 'remove_counts'),
         recipe.use_output('outputs', 'count_tokens2_words')], {}),
    )
for name, rule, inputs, kwargs in dep_rules2:
    out = recipe.add_output('outputs', name, main=True)
    recipe.add_rule(rule(inputs, out, **kwargs))
#SegmentCountsFile (dep on segmentations)

#dep_components =
#FilterCounts

recipe.main()
