import textpipes as tp

recipe = tp.Recipe()
conf = recipe.conf

# this test suite contains a minimal graph to test the components
# it does NOT represent recommended usage in a preprocessing pipeline

# most external tools (anmt, ) are not tested here

onmt_tokenize = tp.components.tokenizer.OnmtTokenize()

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
    # components/noise.py
    ('drop_tokens', tp.apply_component(tp.components.noise.DropTokens()), {}),
    ('peturb_order', tp.apply_component(tp.components.noise.PeturbOrder()), {}),
    ('seg_peturb_order', tp.apply_component(tp.components.noise.SegmentationAwarePeturbOrder()), {}),
    # components/preprocessing.py
    ('truncate_words', tp.apply_component(tp.components.preprocessing.TruncateWords()), {}),
    # components/tokenizer.py
    ('onmt_tokenize', tp.apply_component(onmt_tokenize), {}),
    ('simple_tokenize', tp.apply_component(tp.components.tokenizer.SimpleTokenize()), {}),
    ('old_tokenize', tp.apply_component(tp.components.tokenizer.Tokenize('fi')), {}),
    ('force_tok_long_num', tp.apply_component(tp.components.tokenizer.ForceTokenizeLongNumbers()), {}),
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

# ApplySegmentation with logging of missing
name = 'apply_segmentation'
inp = recipe.add_input('inputs', name)
seg = recipe.add_input('inputs', name + '_seg')
out = recipe.add_output('outputs', name, main=True)
missing = recipe.add_output('outputs', name + '_missing', main=True)
component = tp.apply_component(
    tp.ApplySegmentation(seg, bnd_marker=tp.FIVEDOT + ' ', pre_marked=False, log=missing))
recipe.add_rule(component(inp, out))

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
    # truecaser.py
    ('truecase', tp.apply_component(tp.truecaser.TrueCase(
        model_file=recipe.use_output('outputs', 'train_truecaser'))),
        recipe.add_input('inputs', 'truecase'), {}),
    # components/tokenizer.py
    ('onmt_detokenize', tp.apply_component(tp.components.tokenizer.OnmtDeTokenize(onmt_tokenize)),
        recipe.use_output('outputs', 'onmt_tokenize'), {}),
    ('simple_detokenize', tp.apply_component(tp.components.tokenizer.SimpleDeTokenize()),
        recipe.use_output('outputs', 'simple_tokenize'), {}),
    ('old_detokenize', tp.apply_component(tp.components.tokenizer.DeTokenize('fi')),
        recipe.use_output('outputs', 'old_tokenize'), {}),
    )

for name, rule, inputs, kwargs in dep_rules:
    out = recipe.add_output('outputs', name, main=True)
    recipe.add_rule(rule(inputs, out, **kwargs))

# morfessor.py
recipe.add_rule(tp.morfessor.TrainMorfessor(
    recipe.use_output('outputs', 'count_tokens'),
    [recipe.add_output('outputs', 'train_morfessor', main=True),
     recipe.add_output('outputs', 'morfessor.params'),
     recipe.add_output('outputs', 'morfessor.lexicon')],
    argstr='--traindata-list -w 0.4'))

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
