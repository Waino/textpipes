import collections
import glob
import logging
import os
import sys

from .utils import *

logger = logging.getLogger('textpipes')

class JobStatus(object):
    def __init__(self, status, outputs, inputs=None, rule=None, job_id='-'):
        assert status in (
            'done', 'waiting', 'running',   # info only: no need to schedule
            'available', 'delayed',         # schedule these
            'missing_inputs')               # error
        self.status = status
        self.outputs = tuple(outputs)
        # only unsatisfied inputs
        self.inputs = inputs if inputs is not None else tuple()
        self.rule = rule
        self.job_id = str(job_id)

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

NextSteps = collections.namedtuple('NextSteps',
    ['done', 'waiting', 'running', 'available', 'delayed'])


class Recipe(object):
    """Main class for building experiment recipes"""
    def __init__(self, name=None, argv=None):
        from . import cli
        try:
            import __main__
            self.name, _ = os.path.splitext(__main__.__file__)
            if name is not None and name != self.name:
                raise Exception('If Recipe name ({}) is given, '
                                'it must match file name ({})'
                    .format(name, __main__.__file__))
        except AttributeError:
            self.name = name
            assert name is not None
        # RecipeFile -> Rule or None
        self.files = {}
        # Main outputs, for easy CLI access
        self._main_out = set()
        # conf will be needed before main is called
        self.cli = cli.CLI(self, argv)
        self.conf = self.cli.conf
        self.log = self.cli.log

    @classmethod
    def _make_rf(cls, section, key, loop_index=None, **kwargs):
        if loop_index is None:
            return RecipeFile(section, key, **kwargs)
        else:
            return LoopRecipeFile(section, key, loop_index, **kwargs)

    def add_input(self, section, key, loop_index=None, **kwargs):
        rf = self._make_rf(section, key, loop_index=loop_index, **kwargs)
        rf.atomic = True
        if rf not in self.files:
            self.files[rf] = None
        return rf

    def add_output(self, section, key, loop_index=None, main=False, **kwargs):
        rf = self._make_rf(section, key, loop_index=loop_index, **kwargs)
        if rf in self.files:
            if self.files[rf] is None:
                raise Exception('{} already defined as input. Not adding output.'.format(rf))
            else:
                raise Exception('There is already a rule for {}'.format(rf))
        if main:
            self._main_out.add(rf)
        return rf

    def use_output(self, section, key, loop_index=None, **kwargs):
        rf = self._make_rf(section, key, loop_index=loop_index, **kwargs)
        if rf not in self.files:
            raise Exception('No rule for {}'.format(rf))
        return rf

    def add_rule(self, rule):
        for rf in rule.outputs:
            if rf in self.files:
                if self.files[rf] is None:
                    raise Exception(
                        'Not adding rule {}. '
                        '{} already defined as input.'.format(rule, rf))
                else:
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

        # outputs. nodes not yet visited for input gathering
        border = set()
        # outputs. was on border, now visited
        visited = set()
        # outputs. all nodes in DAG except done/running/scheduled
        needed = set()
        # outputs. done or part of plan
        known = set()
        # outputs. done (subset of known)
        seen_done = set()
        # inputs
        missing = set()
        # JobStatus. informational only
        done = []
        running = []
        waiting = []
        # JobStatus. to schedule
        available = []
        delayed = []

        for rf in outputs:
            if self.log.is_done(rf, self.conf, cli_args):
                done.append(JobStatus('done', [rf]))
                seen_done.add(rf)
            else:
                border.add(rf)

        # traverse the DAG
        while len(border) > 0:
            cursor = border.pop()
            if cursor in visited:
                continue
            visited.add(cursor)
            try:
                rule = self.files[cursor]
            except KeyError:
                raise Exception('No rule to build requested output {}'.format(cursor))
            # FIXME: pass self.conf and cli_args so that flexible rules can adjust?
            # check log for waiting/running jobs
            job_fields = self.log.get_status_of_output(
                cursor(self.conf, cli_args))
            if job_fields.status == 'running':
                if not self.log.is_done(cursor, self.conf, cli_args):
                    # must wait for non-atomic files until job stops running
                    # also wait for an atomic file that doesn't yet exist
                    running.append(JobStatus('running', [cursor], job_id=job_fields.job_id))
                    known.add(cursor)
                    continue
            if cursor.exists(self.conf, cli_args):
                known.add(cursor)
                if not self.log.is_done(cursor, self.conf, cli_args):
                    logger.warning('"{}" exists, but is neither running nor done ({})'.format(
                        cursor(self.conf, cli_args), job_fields.status))
                else:
                    seen_done.add(cursor)
                continue
            if job_fields.status == 'scheduled':
                waiting.append(JobStatus('waiting', [cursor], job_id=job_fields.job_id))
                known.add(cursor)
                continue
            if rule is None:
                # an original input, but failed the exists check above
                missing.add(cursor)
                continue
            border.update(rule.inputs)
            needed.add(cursor)

        if len(missing) > 0 and not config.force:
            # missing inputs block anything at all from running
            raise Exception(
            '\n'.join(str(JobStatus('missing_inputs', [inp], inputs=[inp])) for inp in missing))

        # iteratively sort needed
        while len(needed) > 0:
            remaining = set()
            for cursor in needed:
                if cursor in known:
                    # don't reschedule
                    continue
                rule = self.files[cursor]
                if any(inp not in known for inp in rule.inputs):
                    # has inputs that are not yet part of the plan
                    remaining.add(cursor)
                    continue
                known.update(rule.outputs)
                not_done = tuple(inp for inp in rule.inputs
                                 if inp not in seen_done)
                if len(not_done) > 0:
                    # must wait for some inputs to be built first
                    delayed.append(JobStatus('delayed',
                        rule.outputs,
                        inputs=not_done,
                        rule=rule))
                    continue
                # implicit else: ready for scheduling
                not_done_outputs = [out for out in rule.outputs
                                    if not self.log.is_done(out, self.conf, cli_args)]
                if len(not_done_outputs) == 0:
                    raise Exception('tried to schedule job '
                        'even though all outputs exist: {}'.format(rule))
                available.append(JobStatus('available',
                    not_done_outputs,
                    rule=rule))
            if len(remaining) == len(needed):
                raise Exception('unmet dependencies: {}'.format(remaining))
            needed = remaining

        delayed = delayed if recursive else []
        return NextSteps(done, waiting, running, available, delayed)

    def check_mtime_inversions(self, outputs=None, cli_args=None):
        if not outputs:
            outputs = self.main_outputs
        else:
            if isinstance(outputs, str):
                outputs = [outputs]
            outputs = [self._rf(out) for out in outputs]

        border = set(outputs)
        mtimes = {}
        nonexistent = set()
        while len(border) > 0:
            cursor = border.pop()
            if cursor in mtimes:
                continue
            if not cursor.exists(self.conf, cli_args):
                nonexistent.add(cursor)
                continue
            mtime = os.path.getmtime(cursor(self.conf, cli_args))
            mtimes[cursor] = mtime
            rule = self.files[cursor]
            if rule is None:
                continue
            border.update(rule.inputs)
        inversions = []
        for cursor in mtimes:
            rule = self.files[cursor]
            if rule is None:
                continue
            for inp in rule.inputs:
                if mtimes.get(inp, 0) > mtimes[cursor]:
                    inversions.append((cursor, inp, 'inversion'))
                    continue
                elif inp in nonexistent and cursor in mtimes:
                    inversions.append((cursor, inp, 'orphan'))
        return inversions

    def make_output(self, output, cli_args=None):
        rf = self._rf(output)
        if rf not in self.files:
            raise Exception('No rule to make target {}'.format(output))
        if rf.exists(self.conf, cli_args):
            return JobStatus('done', [rf])

        rule = self.files[rf]
        for out_rf in rule.outputs:
            filepath = out_rf(self.conf, cli_args)
            subdir, _ = os.path.split(filepath)
            os.makedirs(subdir, exist_ok=True)
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
    def __init__(self, inputs, outputs, resource_class='default', chain_schedule=1):
        if isinstance(inputs, RecipeFile):
            self.inputs = (inputs,)
        else:
            self.inputs = tuple(inp for inp in inputs if inp is not None)
        if isinstance(outputs, RecipeFile):
            self.outputs = (outputs,)
        else:
            self.outputs = tuple(out for out in outputs if out is not None)
        self.resource_class = resource_class
        for rf in self.inputs:
            assert isinstance(rf, RecipeFile)
        for rf in self.outputs:
            assert isinstance(rf, RecipeFile)
            rf.atomic = self.is_atomic(rf)
        self.chain_schedule = max(chain_schedule, 1)

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
    def __init__(self, section, key, exact_linecount=None):
        self.section = section
        self.key = key
        # set if exact expected linecount is known
        self.exact_linecount = exact_linecount
        self.atomic = False

    def __call__(self, conf, cli_args=None):
        path = conf.get_path(self.section, self.key)
        if cli_args is not None:
            path = path.format(**cli_args)
        return path

    def status(self, conf, cli_args=None):
        path = self(conf, cli_args)
        if not self.exists(conf, cli_args):
            # if it doesn't exist, it can't be done
            return 'not done'
        if self.exact_linecount is not None:
            lc = external_linecount(path)
            if lc == self.exact_linecount:
                return 'done'
            else:
                # linecount doesn't match expected, so not done
                if lc > self.exact_linecount:
                    # shouldn't be longer than expected
                    logger.warning(
                        'File "{}" is {} lines, longer than expected {}'.format(
                            path, lc, self.exact_linecount))
                return 'unknown'
        # check for emptiness
        if os.path.isdir(path):
            if dir_is_empty(path):
                return 'not done'
        else:
            if os.stat(path).st_size == 0:
                return 'not done'
        if self.atomic:
            # aready checked that it exists
            return 'done'
        return 'unknown'

    def exists(self, conf, cli_args=None):
        return os.path.exists(self(conf, cli_args))

    def open(self, conf, cli_args=None, mode='r', strip_newlines=True):
        filepath = self(conf, cli_args)
        if 'w' in mode:
            subdir, _ = os.path.split(filepath)
            os.makedirs(subdir, exist_ok=True)
        lines = open_text_file(filepath, mode)
        if strip_newlines and 'r' in mode:
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
    def __init__(self, section, key, loop_index, **kwargs):
        super().__init__(section, key, **kwargs)
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


class WildcardLoopRecipeFile(LoopRecipeFile):
    """ Use special formatting {_loop_index} to include the loop index,
    and * to include random unpredictable garbage,
    in the file path template."""

    def __call__(self, conf, cli_args=None):
        super_path = super().__call__(conf, cli_args=cli_args)
        matches = glob.glob(super_path)
        if len(matches) == 0:
            # not present
            return super_path
        elif len(matches) == 1:
            return matches[0]
        raise Exception('{} matched multiple files'.format(self))

    def open(self, conf, cli_args=None, mode='r', strip_newlines=True):
        assert 'w' not in mode, 'Cannot write into WildcardLoopRecipeFile'
        return super().open(conf, cli_args=cli_args, mode=mode, strip_newlines=strip_newlines)

    @staticmethod
    def loop_output(section, key, loop_indices):
        """Create a sequence of LoopRecipeFiles with the given indices"""
        return [WildcardLoopRecipeFile(section, key, loop_index)
                for loop_index in loop_indices]
