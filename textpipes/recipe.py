import collections

RecipeFile = collections.namedtuple('RecipeFile', ['section', 'key'])

class Recipe(object):
    """Main class for building experiment recipes"""
    def __init__(self):
        # RecipeFile -> Rule or None
        self.files = {}

    def use_input(self, section, key):
        rf = RecipeFile(section, key)
        self.files[rf] = None
        return rf

    def add_rule(self, step, inputs=None, outputs=None):
        # inputs: {key: RecipeFile} or RecipeFile
        # outputs: {key: (section, key)} or (section, key)
        rule = step(inputs, outputs)
        if isinstance(outputs, RecipeFile):
            outlist = [outputs]
        else:
            outlist = outputs.values()
        for rf in outlist:
            self.files[rf] = rule
        # FIXME: do we need to make index of rules?
        return rule.outputs

    def status(self, conf, output):
        # -> Done
        # or Available(rule, inputs)
        # or Needs(inputs)
        # or NoRecipe
        pass
