import collections
import glob
import itertools
import logging
import os
import sys

from .utils import *
from .configuration import GridConfig

logger = logging.getLogger('textpipes')

class JobStatus(object):
    def __init__(self, status, outputs, inputs=None, rule=None, job_id='-',
                 concrete=None, overrides=None):
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
        self.concrete = concrete if concrete else []
        self.overrides = overrides if overrides else dict()

    @property
    def sec_key(self):
        return self.outputs[0].sec_key()

    def __hash__(self):
        return hash(self.outputs)

    def __eq__(self, other):
        return self.outputs == other.outputs

    def __repr__(self):
        return "{}('{}', {}, {}, {}, {}, {}, {})".format(
            self.__class__.__name__,
            self.status,
            self.outputs,
            self.inputs,
            self.rule,
            self.job_id,
            self.concrete,
            self.overrides)

NextSteps = collections.namedtuple('NextSteps',
    ['done', 'waiting', 'running', 'available', 'delayed'])

OptionalDep = collections.namedtuple('OptionalDep',
    ['name', 'binary', 'component'])

class UnboundOutput(object):
    def __init__(self):
        self.opt_deps = set()
        self.name = 'unbound output'
        self.inputs = []

    def __repr__(self):
        return 'UNBOUND_OUTPUT'
UNBOUND_OUTPUT = UnboundOutput()

# RecipeFile statuses
NO_FILE = 'no file'
SCHEDULED = 'scheduled'
RUNNING = 'running'
DONE = 'done'
EMPTY = 'empty'
FAILED = 'failed'
CONTINUE = 'can continue'
TOO_SHORT = 'too short'


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
        # RecipeFile -> Rule, None or UNBOUND_OUTPUT
        self.files = {}
        # Main outputs, for easy CLI access
        self._main_out = set()
        # conf will be needed before main is called
        self.cli = cli.CLI(self, argv)
        self.conf = self.cli.conf
        self.log = self.cli.log
        self.status_of = FileStatusCache(self.log, self.conf.platform)

    @classmethod
    def _make_rf(cls, section, key, loop_index=None, **kwargs):
        if loop_index is None:
            return RecipeFile(section, key, **kwargs)
        else:
            return LoopRecipeFile(section, key, loop_index, **kwargs)

    def add_input(self, section, key, loop_index=None, **kwargs):
        if isinstance(section, RecipeFile) and key is None:
            rf = section
        else:
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
        self.files[rf] = UNBOUND_OUTPUT
        return rf

    def use_output(self, section, key, loop_index=None, **kwargs):
        rf = self._make_rf(section, key, loop_index=loop_index, **kwargs)
        if rf not in self.files:
            raise Exception('No rule for {}'.format(rf))
        return rf

    def add_rule(self, rule):
        for rf in rule.outputs:
            if rf in self.files:
                if self.files[rf] == UNBOUND_OUTPUT:
                    # replacing placeholder with Rule
                    pass
                elif self.files[rf] is None:
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

    def grid_next_steps(self,
                        grid,
                        outputs=None,
                        cli_args=None,
                        recursive=False):
        seen = set()
        result = NextSteps([], [], [], [], [])
        for overrides in grid:
            steps = self.get_next_steps_for(outputs=outputs,
                                            cli_args=cli_args,
                                            recursive=recursive,
                                            overrides=overrides)
            for (i, phase) in enumerate(steps):
                for step in phase:
                    if any(path in seen for path in step.concrete):
                        # this step has a non-grid-differentiated output
                        # we need only one copy of it
                        continue
                    seen.update(step.concrete)
                    result[i].append(step)
        return result

    def get_next_steps_for(self, outputs=None, cli_args=None, recursive=False,
                           overrides=None):
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
        # jobs that depend on a blocking job
        blocked = set()
        # JobStatus. informational only
        done = []
        running = []
        waiting = []
        # JobStatus. to schedule
        available = []
        delayed = []

        for rf in outputs:
            if self.status_of(rf, self.conf, cli_args) == DONE:
                done.append(JobStatus('done',
                                      [rf],
                                      concrete=[rf(self.conf, cli_args)],
                                      overrides=overrides))
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
            if rule == UNBOUND_OUTPUT:
                continue
            # check log for waiting/running jobs
            if rule is not None:
                concrete = [rf(self.conf, cli_args) for rf in rule.outputs]
                job_id = self.log.job_id_from_outputs(concrete)
            else:
                concrete = []
                job_id = None

            job_status = self.status_of(cursor, self.conf, cli_args)
            if FileStatusCache.wait(job_status):
                if job_status == SCHEDULED:
                    tmp = waiting
                    js_status = 'waiting'
                elif job_status == RUNNING:
                    tmp = running
                    js_status = 'running'
                tmp.append(JobStatus(js_status,
                                     [cursor],
                                     job_id=job_id,
                                     concrete=concrete,
                                     overrides=overrides))
                known.add(cursor)
                continue
            elif FileStatusCache.continue_next(job_status):
                known.add(cursor)
                seen_done.add(cursor)
                continue
            elif FileStatusCache.error(job_status):
                known.add(cursor)
                if self.conf.force:
                    seen_done.add(cursor)
                continue
            if rule is None:
                # an original input, but failed the exists check above
                missing.add(cursor)
                continue
            border.update(rule.inputs)
            needed.add(cursor)

        if len(missing) > 0:
            if self.conf.force:
                known.update(missing)
            else:
                # missing inputs block anything at all from running
                raise Exception(
                '\n'.join(str(JobStatus('missing_inputs',
                                        [inp],
                                        inputs=[inp],
                                        concrete=[inp(self.conf, cli_args)],
                                        overrides=overrides)) for inp in missing))

        # iteratively sort needed
        while len(needed) > 0:
            remaining = set()
            not_yet = {}
            for cursor in needed:
                if cursor in known:
                    # don't reschedule
                    continue
                rule = self.files[cursor]
                if any(inp not in known for inp in rule.inputs):
                    # has inputs that are not yet part of the plan
                    remaining.add(cursor)
                    not_yet[cursor] = [inp for inp in rule.inputs
                                       if inp not in known]
                    continue
                known.update(rule.outputs)
                not_done = tuple(inp for inp in rule.inputs
                                 if inp not in seen_done)
                concrete = [rf(self.conf, cli_args) for rf in rule.outputs]
                if rule.blocks_recursion:
                    blocked.add(cursor)
                if any(inp in blocked for inp in rule.inputs):
                    # this job is blocked from running
                    blocked.add(cursor)
                    continue
                if len(not_done) > 0:
                    # must wait for some inputs to be built first
                    delayed.append(JobStatus('delayed',
                        rule.outputs,
                        inputs=not_done,
                        rule=rule,
                        concrete=concrete,
                        overrides=overrides))
                    continue
                # implicit else: ready for scheduling
                not_done_outputs = [
                    out for out in rule.outputs
                    if self.status_of(cursor, self.conf, cli_args) != DONE]

                if len(not_done_outputs) == 0:
                    raise Exception('tried to schedule job '
                        'even though all outputs exist: {}'.format(rule))
                available.append(JobStatus('available',
                    not_done_outputs,
                    rule=rule,
                    concrete=concrete,
                    overrides=overrides))
            if len(remaining) == len(needed):
                #err_str = '\n'.join(['{} depends on: {}'.format(
                #    cursor, not_yet[cursor])
                #    for cursor in remaining])
                cursor, closure = self._detect_circles(not_yet)
                err_str = '{} has circular dep: {}'.format(
                    cursor, closure)
                raise Exception('unmet dependencies:\n{}'.format(err_str))
            needed = remaining

        delayed = delayed if recursive else []
        return NextSteps(done, waiting, running, available, delayed)

    @staticmethod
    def _detect_circles(not_yet):
        closure = collections.defaultdict(set)
        for cursor, deps in not_yet.items():
            closure[cursor].update(deps)
        for _ in range(50):
            for cursor, deps in list(closure.items()):
                for dep in deps:
                    closure[cursor].update(closure[dep])
                    if cursor in closure[cursor]:
                        return cursor, closure[cursor]
        return None, set()

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
            if rule is None or rule == UNBOUND_OUTPUT:
                continue
            border.update(rule.inputs)
        inversions = []
        for cursor in mtimes:
            rule = self.files[cursor]
            if rule is None or rule == UNBOUND_OUTPUT:
                continue
            for inp in rule.inputs:
                if mtimes.get(inp, 0) > mtimes[cursor]:
                    inversions.append((cursor, inp, 'inversion'))
                    continue
                elif inp in nonexistent and cursor in mtimes:
                    inversions.append((cursor, inp, 'orphan'))
        return inversions

    def make_output(self, output, conf=None, cli_args=None):
        if conf is None:
            conf = self.conf
        rf = self._rf(output)
        if rf not in self.files:
            raise Exception('No rule to make target {}'.format(output))
        if rf.exists(conf, cli_args):
            return JobStatus('done', [rf])

        rule = self.files[rf]
        for out_rf in rule.outputs:
            filepath = out_rf(conf, cli_args)
            subdir, _ = os.path.split(filepath)
            os.makedirs(subdir, exist_ok=True)
        return rule.make(conf, cli_args)

    def add_main_outputs(self, outputs=None):
        if outputs is None:
            # set all outputs to main
            outputs = [rf for (rf, val) in self.files.items()
                       if val is not None]
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

    @property
    def opt_deps(self):
        all_deps = set()
        for rule in self.files.values():
            if rule is not None:
                all_deps.update(rule.opt_deps)
        grouped = []
        all_deps = sorted(
            all_deps,
            key=lambda x: (x.binary, x.name, x.component))
        # group by module/binary, then by name of dep
        for binary, tgroup in itertools.groupby(all_deps,
                                           key=lambda x: x.binary):
            for dep, dgroup in itertools.groupby(tgroup,
                                                 key=lambda x: x.name):
                grouped.append(
                    (dep, binary, [x.component for x in dgroup]))
        return grouped


class Rule(object):
    """A part of a recipe.

    a Rule tells how to make some particular output RecipeFile(s)
    The subclass defines how to make the output.
    The object is initialized with RecipeFiles defining
    input and output paths.
    """
    def __init__(self, inputs, outputs,
                 resource_class='default',
                 chain_schedule=1):
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
            rf.atomic = rf.atomic or self.is_atomic(rf)
        self.chain_schedule = max(chain_schedule, 1)
        self._opt_deps = set()
        self.blocks_recursion = False

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

    def add_opt_dep(self, name, binary=False):
        self._opt_deps.add(OptionalDep(name, binary, self.name))

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def opt_deps(self):
        return self._opt_deps

    def __eq__(self, other):
        if other == UNBOUND_OUTPUT:
            return False
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
    def __init__(self, section, key, exact_linecount=None, allow_empty=False):
        self.section = section
        self.key = key
        # set if exact expected linecount is known
        self.exact_linecount = exact_linecount
        # non-atomic files grow line by line
        self.atomic = False
        # is an empty file indicative of an error?
        self.allow_empty = allow_empty
        # can a partial result be continued by rescheduling?
        self.can_continue = False

    def __call__(self, conf, cli_args=None):
        path = conf.get_path(self.section, self.key)
        if cli_args is not None:
            path = path.format(**cli_args)
        return path

    def check_length(self, conf, cli_args=None):
        # assumes existence has been checked already
        status = DONE
        lc = None
        path = self(conf, cli_args)
        # check for emptiness
        if not self.allow_empty:
            if os.path.isdir(path):
                if dir_is_empty(path):
                    return EMPTY, lc, self.exact_linecount
            else:
                if os.stat(path).st_size == 0:
                    return EMPTY, lc, self.exact_linecount
        if self.exact_linecount is not None:
            lc = external_linecount(path)
            if lc < self.exact_linecount:
                status = TOO_SHORT
        return status, lc, self.exact_linecount

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
        self._silence_warn = False

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
        raise Exception('{} matched multiple files:\n{}'.format(self, '\n'.join(matches)))

    def open(self, conf, cli_args=None, mode='r', strip_newlines=True):
        assert 'w' not in mode, 'Cannot write into WildcardLoopRecipeFile'
        return super().open(conf, cli_args=cli_args, mode=mode, strip_newlines=strip_newlines)

    @staticmethod
    def loop_output(section, key, loop_indices):
        """Create a sequence of LoopRecipeFiles with the given indices"""
        return [WildcardLoopRecipeFile(section, key, loop_index)
                for loop_index in loop_indices]


class IndirectRecipeFile(RecipeFile):
    """A RecipeFile that reads a concrete file name from a separate file
    """
    def __call__(self, conf, cli_args=None):
        link_path = super().__call__(conf, cli_args)
        if not os.path.exists(link_path):
            return link_path
        with open(link_path, 'r') as fobj:
            target_path = fobj.readline().strip()
        return target_path

    def status(self, conf, cli_args=None):
        link_path = super().__call__(conf, cli_args)
        if not os.path.exists(link_path):
            # if the link doesn't exist, it can't be done
            return 'not done'
        return super().status(conf, cli_args)

    def __repr__(self):
        return 'IndirectRecipeFile({}, {})'.format(self.section, self.key)


class FileStatusCache(object):
    def __init__(self, log, platform):
        self.log = log
        self.platform = platform
        self._cache = {}

    @staticmethod
    def continue_this(status):
        """True if the status indicates that this output
           could be scheduled"""
        return status in (NO_FILE, CONTINUE)

    @staticmethod
    def continue_next(status):
        """True if the status indicates that a job depending on this output
           could be scheduled"""
        return status == DONE

    @staticmethod
    def wait(status):
        """True if the status indicates that you should wait"""
        return status in (SCHEDULED, RUNNING)

    @staticmethod
    def error(status):
        """True if the status indicates an error"""
        return status in (EMPTY, FAILED, TOO_SHORT)

    def clear(self):
        self._cache = {}

    def __call__(self, rf, conf, cli_args=None):
        if rf not in self._cache:
            self._cache[rf] = self._status(rf, conf, cli_args)
        result = self._cache[rf]
        if result not in (NO_FILE, SCHEDULED, RUNNING, DONE,
                          EMPTY, FAILED, CONTINUE, TOO_SHORT):
            raise Exception('unknown status "{}"'.format(result))
        return result

    def _status(self, rf, conf, cli_args=None):
        if rf.exists(conf, cli_args):
            if rf.atomic:
                # if atomic file exists, it is done
                return DONE
            else:
                # not atomic
                true_status = self._get_true_status(rf, conf, cli_args, exists=True)
                if true_status == SCHEDULED:
                    self.warn(rf, conf, cli_args, true_status,
                              'Non-atomic output of scheduled job '
                              'already exists')
                    return true_status
                elif true_status == RUNNING:
                    return true_status
                elif true_status == DONE:
                    true_status, true_length, expected_length = \
                        rf.check_length(conf, cli_args)
                    if true_status == TOO_SHORT:
                        self.warn(rf, conf, cli_args, true_status,
                                  '{} lines, shorter than expected {}'.format(
                                        true_length, expected_length))
                    elif expected_length is not None and true_length > expected_length:
                        self.warn(rf, conf, cli_args, true_status,
                                  '{} lines, longer than expected {}'.format(
                                        true_length, expected_length))
                    return true_status
                elif true_status == FAILED:
                    if rf.can_continue:
                        self.warn(rf, conf, cli_args, true_status,
                                  'Reschedule to continue')
                        return CONTINUE
                    else:
                        # can not continue
                        self.warn(rf, conf, cli_args, true_status,
                                  'Partial output of FAILED job')
                    return FAILED
                elif true_status == 'unknown':
                    # let's be optimistic
                    return DONE
                elif true_status == 'not scheduled':
                    # logs contain no explanation
                    # for the existence of this file
                    if conf.ingest_manual:
                        self.log.ingest_manual(rf, conf, cli_args)
                        return DONE
                    elif conf.force:
                        return DONE
                    else:
                        self.warn(rf, conf, cli_args, true_status,
                                'The origin of this file is unkown '
                                '(use --force or --ingest-manual to fix)')
                        return FAILED
                return true_status
        else:
            # file doesn't exist
            if self.log.was_scheduled(rf(conf, cli_args)):
                # was scheduled at some point
                true_status = self._get_true_status(rf, conf, cli_args)
                if true_status == FAILED:
                    self.warn(rf, conf, cli_args, true_status,
                              'This job failed earlier, '
                              'but output file does not exist (anymore)')
                    # failing without output doesn't prevent relaunch
                    return NO_FILE
                elif true_status == 'unknown':
                    return NO_FILE
                return true_status
            else:
                # has not been scheduled
                return NO_FILE
        raise Exception('problem in FileStatusCache if-tree'
            ' {} {}'.format(rf, rf(conf, cli_args)))

    def _get_true_status(self, rf, conf, cli_args=None, exists=False):
        job_fields = self.log.get_status_of_output(rf(conf, cli_args))
        expected = job_fields.status
        expected = DONE if expected == 'finished' else expected   # FIXME tmp
        if expected == FAILED:
            return FAILED
        if expected == DONE:
            if exists:
                return DONE
            else:
                return NO_FILE
        true_status = self.platform.check_job(job_fields.job_id)
        if true_status == 'local':
            true_status = expected
        elif expected != true_status:
            self.log.update_status(true_status, job_fields.job_id, rf, conf, cli_args)
        return true_status

    def warn(self, rf, conf, cli_args, status, message):
        path = rf(conf, cli_args)
        job_fields = self.log.get_status_of_output(path)
        logger.warning('{msg}: {sec_key} {job_id} {status} {path}'.format(
            msg=message,
            sec_key=rf.sec_key(),
            job_id=job_fields.job_id,
            status=status,
            path=path))
