import collections
import os

from .utils import *

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

    def add_rule(self, rule):
        for rf in rule.outputs:
            if rf in self.files:
                print(self.files)
                raise Exception(
                    'Not adding rule {}. '
                    'There is already a rule for {}'.format(rule, rf))
            self.files[rf] = rule
        # FIXME: do we need to make index of rules?
        # FIXME: inconvenient to return all outputs. Only do main
        return rule.outputs

    def get_next_step_for(self, conf, output, cli_args=None):
        # -> Done
        # or [Available(output, rule)], [Running(output)]
        # or MissingInputs(inputs)
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
            running = [Running(output) for output in running]

        available = []
        triggered_rules = set()
        for cursor, rule in potential:
            if rule in triggered_rules:
                continue
            if all(inp in seen_done for inp in rule.inputs):
                available.append(Available(cursor, rule))
                triggered_rules.add(rule)
        return available, running

    def get_all_next_steps_for(self, conf, outputs, cli_args=None):
        available = []
        running = []
        for output in outputs:
            ns = self.get_next_step_for(conf, output)
            if isinstance(ns, Done):
                continue
            if isinstance(ns, MissingInputs):
                return ns
            available.extend(ns[0])
            running.extend(ns[1])
        if len(available) + len(running) == 0:
            return Done()
        return available, running   # FIXME: bad API

    def make_output(self, conf, output, cli_args=None):
        if isinstance(output, RecipeFile):
            rf = output 
        else:
            rf = RecipeFile(*output.split(':'))
        if rf not in self.files:
            raise Exception('No rule to make target {}'.format(output))
        if rf.exists(conf, cli_args):
            return Done()

        rule = self.files[rf]
        return rule.make(conf, cli_args)


class Rule(object):
    """A part of a recipe.

    a Rule tells how to make some particular output RecipeFile(s)
    The subclass defines how to make the output.
    The object is initialized with RecipeFiles defining
    input and output paths.
    """
    def __init__(self, inputs, outputs):
        if isinstance(inputs, RecipeFile):
            self.inputs = (inputs,)
        else:
            self.inputs = tuple(inputs)
        if isinstance(outputs, RecipeFile):
            self.outputs = (outputs,)
        else:
            self.outputs = tuple(outputs)

    def make(self, conf, cli_args=None):
        raise NotImplementedError()

    def __repr__(self):
        return '{}(inputs={}, outputs={})'.format(
            self.__class__.__name__,
            self.inputs,
            self.outputs)


class RecipeFile(object):
    """A RecipeFile is a file template
    that points to a concrete file when given conf and cli_args
    """
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

    def open(self, conf, cli_args=None, mode='rb', strip_newlines=True):
        lines = open_text_file(self(conf, cli_args), mode)
        if strip_newlines and not 'w' in mode:
            lines = (line.rstrip('\n') for line in lines)
        return lines

    def __eq__(self, other):
        return (self.section, self.key) == (other.section, other.key)

    def __hash__(self):
        return hash((self.section, self.key))

    def __repr__(self):
        return 'RecipeFile({}, {})'.format(self.section, self.key)
