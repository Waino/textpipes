import collections
import logging
import os

from .utils import *

logger = logging.getLogger('textpipes')

class JobStatus(object):
    def __init__(self, status, outputs, inputs=None, rule=None, job_id='-'):
        assert status in (
            'done', 'waiting', 'running',   # info only: no need to schedule
            'available',                    # schedule these
            'missing_inputs')               # error
        self.status = status
        self.outputs = tuple(outputs)
        # only unsatisfied inputs
        self.inputs = inputs if inputs is not None else tuple()
        self.rule = rule
        self.job_id = job_id

    @property
    def sec_key(self):
        return self.outputs[0].sec_key()

    def __hash__(self):
        return hash(self.outputs)

    def __eq__(self, other):
        return self.outputs == other.outputs

    def __repr__(self):
        return "{}('{}', {}, {}, {}, {})".format(
            self.__class__.__name__,
            self.status,
            self.outputs,
            self.inputs,
            self.rule,
            self.job_id)


class Recipe(object):
    """Main class for building experiment recipes"""
    def __init__(self, name, argv=None):
        from . import cli
        self.name = name
        # RecipeFile -> Rule or None
        self.files = {}
        # Main outputs, for easy CLI access
        self._main_out = set()
        # conf will be needed before main is called
        self.cli = cli.CLI(self, argv)
        self.conf = self.cli.conf
        self.log = self.cli.log

    def add_input(self, section, key):
        rf = RecipeFile(section, key)
        if rf not in self.files:
            self.files[rf] = None
        return rf

    def add_output(self, section, key, loop_index=None, main=False):
        if loop_index is None:
            rf = RecipeFile(section, key)
        else:
            rf = LoopRecipeFile(section, key, loop_index)
        if rf in self.files:
            raise Exception('There is already a rule for {}'.format(rf))
        if main:
            self._main_out.add(rf)
        return rf

    def add_rule(self, rule):
        for rf in rule.outputs:
            if rf in self.files:
                raise Exception(
                    'Not adding rule {}. '
                    'There is already a rule for {}'.format(rule, rf))
            self.files[rf] = rule
        # FIXME: do we need to make index of rules?
        # FIXME: inconvenient to return all outputs. Only do main
        return rule.outputs

    def get_rule(self, output):
        return self.files.get(self._rf(output), None)

    def _rf(self, output, check=True):
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
        if check and rf not in self.files:
            print(rf)
            print(self.files.keys())
            raise Exception('No rule to make target {}'.format(output))
        return rf

    def get_next_steps_for(self, outputs=None, cli_args=None, recursive=False):
        # -> [JobStatus]
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
                done.append(JobStatus('done', [rf]))
            else:
                border.add(rf)

        # JobStatus
        waiting = set()
        running = set()
        # (ouput, rule)
        potential = set()
        # potential outputs in reverse DAG order
        order = []
        # input
        visited = set()
        seen_done = set()
        missing = set()
        # traverse the DAG
        while len(border) > 0:
            cursor = border.pop()
            if cursor in visited:
                continue
            visited.add(cursor)
            rule = self.files[cursor]
            # FIXME: pass self.conf and cli_args so that flexible rules can adjust?
            # check log for waiting/running jobs
            status, job_fields = self.log.get_status_of_output(
                cursor(self.conf, cli_args))
            if status == 'running':
                if not (rule.is_atomic(cursor) and cursor.exists(self.conf, cli_args)):
                    # must wait for non-atomic files until job stops running
                    # also wait for an atomic file that doesn't yet exist
                    running.add(JobStatus('running', [cursor], job_id=job_fields.job_id))
                    continue
            if cursor.exists(self.conf, cli_args):
                seen_done.add(cursor)
                continue
            if status == 'scheduled':
                waiting.add(JobStatus('waiting', [cursor], job_id=job_fields.job_id))
                continue
            if rule is None:
                # an original input, but failed the exists check above
                missing.add(cursor)
                continue
            border.update(rule.inputs)
            potential.add((cursor, rule))
            order.append(cursor)

        if len(missing) > 0:
            # missing inputs block anything at all from running
            return [JobStatus('missing_inputs', [inp], inputs=[inp]) for inp in missing]

        # rule -> JobStatus
        items = {}
        # outputs
        available = set()
        delayed = set()
        # one rule can produce multiple outputs, but we only want to schedule once
        # so we group by the rule
        potential = sorted(potential, key=lambda x: (hash(x[1]), x[0]))
        for (rule, pairs) in itertools.groupby(potential, lambda x: x[1]):
            pairs = list(pairs)
            not_done = tuple(inp for inp in rule.inputs
                             if inp not in seen_done)
            output = pairs[0][0]
            if len(not_done) > 0:
                # some inputs need to be built first
                if recursive:
                    delayed.add(output)
                    items[output] = JobStatus('available',
                        tuple(output for (output, rule) in pairs),
                        inputs=not_done,
                        rule=rule)
                continue
            available.add(output)
            items[output] = JobStatus('available',
                tuple(output for (output, rule) in pairs),
                rule=rule)

        waiting = sorted(waiting, key=lambda job: (job.job_id, job.sec_key))
        running = sorted(running, key=lambda job: (job.job_id, job.sec_key))
        avail   = [items[output] for output in reversed(order) if output in available]
        delayd  = [items[output] for output in reversed(order) if output in delayed]
        return done + waiting + running + avail + delayd

    def make_output(self, output, cli_args=None):
        rf = self._rf(output)
        if rf not in self.files:
            raise Exception('No rule to make target {}'.format(output))
        if rf.exists(self.conf, cli_args):
            return JobStatus('done', [rf])

        rule = self.files[rf]
        return rule.make(self.conf, cli_args)

    def add_main_outputs(self, outputs):
        for out in outputs:
            if not isinstance(out, RecipeFile):
                raise Exception('output {} is not a RecipeFile'.format(out))
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
    def __init__(self, inputs, outputs, resource_class='default'):
        if isinstance(inputs, RecipeFile):
            self.inputs = (inputs,)
        else:
            self.inputs = tuple(inputs)
        if isinstance(outputs, RecipeFile):
            self.outputs = (outputs,)
        else:
            self.outputs = tuple(outputs)
        self.resource_class = resource_class

    def make(self, conf, cli_args=None):
        raise NotImplementedError()

    def monitor(self, platform, conf, cli_args=None):
        """Return a short summary of the status of a running job.

        By default this is the line count of the first output file.
        Subclasses can override this, to e.g. show a percentage,
        minibatch number, training loss or whatever is appropriate."""
        if len(self.outputs) == 0:
            return '-'
        main_out_path = self.outputs[0](conf, cli_args)
        if not os.path.exists(main_out_path):
            return 'no output'
        lc = external_linecount(main_out_path)
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
        self.loop_index = int(loop_index)

    def __call__(self, conf, cli_args=None):
        path = conf.get_path(self.section, self.key)
        if '{_loop_index}' not in path and not self._silence_warn:
            logger.warning('LoopRecipeFile without _loop_index in template')
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
        """Create a sequence of LoopRecipeFiles with the given indices"""
        return [LoopRecipeFile(section, key, loop_index)
                for loop_index in loop_indices]

    @staticmethod
    def highest_written(outputs, conf, cli_args=None):
        """Find the file with the highest loop_index that has been created.
        (if the Rule is non-atomic, the file may not necessarily be done)"""
        existing = [out for out in outputs
                    if out.exists(conf, cli_args)]
        if len(existing) == 0:
            return None
        return max(existing, key=lambda x: x.loop_index)
