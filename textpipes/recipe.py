import collections
import os

class Recipe(object):
    """Main class for building experiment recipes"""
    def __init__(self):
        # RecipeFile -> Rule or None
        self.files = {}
        # Main outputs, for easy CLI access
        self.main_out = []

    def add_input(self, section, key):
        rf = RecipeFile(section, key)
        self.files[rf] = None
        return rf

    def add_output(self, section, key, main=False):
        rf = RecipeFile(section, key)
        if rf in self.files:
            raise Exception('There is already a rule for {}'.format(rf))
        if main:
            self.main_out.append(rf)
        return rf

    def add_rule(self, step, inputs=None, outputs=None, **kwargs):
        # inputs: {key: RecipeFile} or RecipeFile
        # outputs: {key: RecipeFile} or RecipeFile
        rule = step(inputs, outputs, **kwargs)
        if isinstance(outputs, RecipeFile):
            outlist = [outputs]
        else:
            outlist = outputs.values()
        for rf in outlist:
            if rf in self.files:
                raise Exception('There is already a rule for {}'.format(rf))
            self.files[rf] = rule
        # FIXME: do we need to make index of rules?
        return rule.outputs

    def status(self, conf, output, cli_args=None):
        # -> Done
        # or Running
        # or Available(rule)
        # or Needs(inputs)
        # or raise NoRule
        pass


class RecipeFile(object):
    def __init__(self, section, key):
        self.section = section
        self.key = key

    def __call__(self, conf, cli_args=None):
        path = conf.get_path(self.section, self.key)
        if cli_args is not None:
            path = path.format(**cli_args)
        return path

    def exists(self, conf, cli_args=None):
        return os.path.exists(self(conf, cli_args))

    def __eq__(self, other):
        return (self.section, self.key) == (other.section, other.key)

    def __repr__(self):
        return 'RecipeFile({}, {})'.format(self.section, self.key)
