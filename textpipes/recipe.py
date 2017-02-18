class Recipe(object):
    """Main class for building experiment recipes"""
    def __init__(self):
        # path_template -> Rule or None
        self.files = {}

    def use_input(self, section, key):
        # -> RecipeFile
        pass

    def add_rule(self, step, inputs=None, outputs=None):
        # inputs: {key: RecipeFile} or RecipeFile
        # outputs: {key: (section, key)} or (section, key)
        # -> Rule
        pass

    def status(self, conf, output):
        # -> Done
        # or Available(rule, inputs)
        # or Needs(inputs)
        # or NoRecipe
        pass
