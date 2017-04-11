import collections
import os

from .utils import *

Done = collections.namedtuple('Done', ['output'])
Waiting = collections.namedtuple('Waiting', ['output'])
Running = collections.namedtuple('Running', ['output'])
Available = collections.namedtuple('Available', ['outputs', 'rule'])
MissingInput = collections.namedtuple('MissingInputs', ['input'])

class Recipe(object):
    """Main class for building experiment recipes"""
    def __init__(self, name):
        from . import cli
        self.name = name
        # RecipeFile -> Rule or None
        self.files = {}
        # Main outputs, for easy CLI access
        self._main_out = set()
        # conf will be needed before main is called
        self.cli = cli.CLI(self)
        self.conf = self.cli.conf
        self.log = self.cli.log

    def add_input(self, section, key):
        rf = RecipeFile(section, key)
        self.files[rf] = None
        return rf

    def add_output(self, section, key, main=False):
        rf = RecipeFile(section, key)
        if rf in self.files:
            raise Exception('There is already a rule for {}'.format(rf))
        if main:
            self._main_out.add(rf)
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

    def get_rule(self, output):
        return self.files.get(self._rf(output), None)

    def _rf(self, output):
        if isinstance(output, RecipeFile):
            rf = output 
        else:
            sec_key = output.split(':')
            if len(sec_key) == 2:
                rf = RecipeFile(*sec_key)
            elif len(sec_key) == 3:
                rf = LoopRecipeFile(*sec_key)
            else:
                raise Exception('Cannot parse section:key "{}"'.format(output))
        if rf not in self.files:
            raise Exception('No rule to make target {}'.format(output))
        return rf

    def get_next_steps_for(self, outputs=None, cli_args=None):
        # -> [Done(output)]
        # or [Available(outputs, rule), ... Running(output)]
        # or [MissingInput(input)]
        outputs = outputs 
        if not outputs:
            outputs = self.main_outputs
        else:
            if isinstance(outputs, str):
                outputs = [outputs]
            outputs = [self._rf(out) for out in outputs]

        border = set()
        done = []
        for rf in outputs:
            if rf.exists(self.conf, cli_args):
                done.append(Done(rf))
            else:
                border.add(rf)

        # traverse the DAG
        waiting = set()
        running = set()
        potential = set()
        seen_done = set()
        missing = set()
        while len(border) > 0:
            cursor = border.pop()
            rule = self.files[cursor]
            # check log for waiting/running jobs
            status, job_fields = self.log.get_status_of_output(
                cursor(self.conf, cli_args))
            if status == 'running' and not rule.is_atomic(cursor):
                running.add(cursor)
                continue
            if cursor.exists(self.conf, cli_args):
                seen_done.add(cursor)
                continue
            if status == 'scheduled':
                waiting.add(cursor)
                continue
            if rule is None:
                # an original input, but failed the exists check above
                missing.add(cursor)
                continue
            border.update(rule.inputs)
            potential.add((cursor, rule))

        if len(missing) > 0:
            # missing inputs block anything from running
            return [MissingInput(inp) for inp in missing]
        waiting = [Waiting(output) for output in waiting]
        running = [Running(output) for output in running]

        available = []
        potential = sorted(potential, key=lambda x: hash(x[1]))
        for (rule, pairs) in itertools.groupby(potential, lambda x: x[1]):
            if any(inp not in seen_done for inp in rule.inputs):
                # inputs need to be built first
                continue
            print(rule, pairs)
            available.append(
                Available(tuple(output for (output, rule) in pairs), rule))

        return done + available + waiting + running

    def make_output(self, output, cli_args=None):
        rf = self._rf(output)
        if rf not in self.files:
            raise Exception('No rule to make target {}'.format(output))
        if rf.exists(self.conf, cli_args):
            return Done()

        rule = self.files[rf]
        return rule.make(self.conf, cli_args)

    def add_main_outputs(self, outputs):
        self._main_out.update(outputs)
    
    def main(self):
        self.cli.main()

    @property
    def main_inputs(self):
        return sorted(rf for (rf, val) in self.files.items()
                      if val is None)

    @property
    def main_outputs(self):
        return sorted(self._main_out)


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

    def monitor(self, platform, file_paths):
        """Return a short summary of the status of a running job.

        By default this is the line count of the first output file.
        Subclasses can override this, to e.g. show a percentage,
        minibatch number, training loss or whatever is appropriate."""
        if not os.path.exists(file_paths[0]):
            return 'no output'
        lc = external_linecount(file_paths[0])
        return '{} lines'.format(lc)

    def is_atomic(self, output):
        """Returns True, if the existence of the output file can be
        assumed to indicate that it is ready.
        Returning False indicates the normal condition:
        contents are piped into output during the entire course of the
        job, so clients must wait until the job finishes.
        """
        # Subclasses with atomic outputs should override this
        return False

    @property
    def name(self):
        return self.__class__.__name__

    def __eq__(self, other):
        return (self.name, self.inputs, self.outputs) \
            == (other.name, other.inputs, other.outputs)

    def __hash__(self):
        return hash((self.name, self.inputs, self.outputs))

    def __repr__(self):
        return '{}(inputs={}, outputs={})'.format(
            self.name,
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

    def sec_key(self):
        return '{}:{}'.format(self.section, self.key)

    def __eq__(self, other):
        return (self.section, self.key) == (other.section, other.key)

    def __hash__(self):
        return hash((self.section, self.key))

    def __lt__(self, other):
        return self.section + self.key < other.section + other.key

    def __repr__(self):
        return 'RecipeFile({}, {})'.format(self.section, self.key)


class LoopRecipeFile(RecipeFile):
    """ Use special formatting {_loop_index} to include the
    loop index in the file path template."""
    def __init__(self, section, key, loop_index):
        super().__init__(section, key)
        self.loop_index = loop_index

    def __call__(self, conf, cli_args=None):
        path = conf.get_path(self.section, self.key)
        fmt_args = {}
        if cli_args is not None:
            fmt_args.update(cli_args)
        fmt_args['_loop_index'] = self.loop_index
        path = path.format(**fmt_args)
        return path

    def sec_key(self):
        return '{}:{}:{}'.format(self.section, self.key, self.loop_index)

    def __eq__(self, other):
        return (self.section, self.key, self.loop_index) == \
               (other.section, other.key, other.loop_index)

    def __hash__(self):
        return hash((self.section, self.key, self.loop_index))

    def __lt__(self, other):
        if (self.section, self.key) == (other.section, other.key):
            return self.loop_index < other.loop_index
        return self.section + self.key < other.section + other.key

    def __repr__(self):
        return 'LoopRecipeFile({}, {}, {})'.format(
            self.section, self.key, self.loop_index)

    @staticmethod
    def loop_output(section, key, loop_indices):
        return [LoopRecipeFile(section, key, loop_index)
                for loop_index in loop_indices]
