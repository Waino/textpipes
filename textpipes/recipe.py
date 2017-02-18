import collections
import os

Done = collections.namedtuple('Done', [])
Running = collections.namedtuple('Running', ['outputs'])
Available = collections.namedtuple('Available', ['output', 'rule'])
MissingInputs = collections.namedtuple('MissingInputs', ['inputs'])

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

    def get_next_step_for(self, conf, output, cli_args=None):
        # -> Done
        # or Running
        # or [Available(rule)]
        # or MissingInput(input)
        if isinstance(output, RecipeFile):
            rf = output 
        else:
            rf = RecipeFile(*output.split(':'))
        if rf not in self.files:
            raise Exception('No rule to make target {}'.format(output))
        if rf.exists(conf, cli_args):
            return Done()

        # traverse the DAG
        border = set((rf,))
        running = set()
        potential = set()
        seen_done = set()
        missing = set()
        while len(border) > 0:
            cursor = border.pop()
            if cursor.exists(conf, cli_args):
                seen_done.add(cursor)
                continue
            # FIXME: check log for running jobs
            rule = self.files[cursor]
            if rule is None:
                # an original input, but failed the exists check above
                missing.add(cursor)
                continue
            border.update(rule.inputs)
            potential.add((cursor, rule))

        if len(missing) > 0:
            return MissingInputs(tuple(missing))
        if len(running) > 0:
            # FIXME: should not block other possible availables?
            return Running(tuple(running))

        available = []
        triggered_rules = set()
        for cursor, rule in potential:
            if rule in triggered_rules:
                continue
            if all(inp in seen_done for inp in rule.inputs):
                available.append(Available((cursor, rule)))
                triggered_rules.add(rule)
        return available



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

    def __hash__(self):
        return hash((self.section, self.key))

    def __repr__(self):
        return 'RecipeFile({}, {})'.format(self.section, self.key)
