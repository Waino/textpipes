import argparse
import collections
import importlib
import itertools
import logging
import os
import re
from datetime import datetime, timedelta

from .configuration import Config, GridConfig
from .platform import run, parse_override_string
from .recipe import *
from .utils import *

logger = logging.getLogger('textpipes')

RE_TEMPLATE = re.compile('#[^#]*#')

def get_parser(recipe):
    parser = argparse.ArgumentParser(
        description='TextPipes (recipe: {})'.format(recipe.name))

    parser.add_argument('conf', type=str, metavar='CONF',
                        help='Name of the experiment conf file')
    parser.add_argument('output', type=str, nargs='*', metavar='OUTPUT',
                        help='Output(s) to schedule, in section:key format')
    parser.add_argument('--check', default=False, action='store_true',
                        help='Perform validity check')
    parser.add_argument('--dump-paths', default=False, action='store_true',
                        help='Print (templated) paths when performing validity check or mtimes')
    parser.add_argument('--status', default=False, action='store_true',
                        help='Status of ongoing experiments')
    parser.add_argument('--mtimes', default=False, action='store_true',
                        help='Look for outputs that are older than their inputs')
    parser.add_argument('-q', '--quiet', default=False, action='store_true',
                        help='Less verbose output, by hiding some info')
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='More verbose output, e.g. print inputs')
    parser.add_argument('--show-all', default=False, action='store_true',
                        help='More verbose output, by showing all grid points')
    parser.add_argument('-n', '--dryrun', default=False, action='store_true',
                        help='Show what would be done, but do not do it')
    parser.add_argument('-r', '--recursive', default=False, action='store_true',
                        help='Schedule the whole DAG recursively. '
                        'Default is to only schedule jobs that are ready to run.')
    parser.add_argument('-f', '--force', default=False, action='store_true',
                        help='Ignore failed dependencies when scheduling. ')
    parser.add_argument('--no-fork', default=False, action='store_true',
                        help='Do not use multiprocessing to speed up.')
    parser.add_argument('--resource-classes', default=None, type=str,
                        help='Only schedule jobs with one of these resource classes. '
                        'Comma separated list of strings. '
                        'UNTESTED: if used with --recursive.')
    parser.add_argument('--only-rules', default=None, type=str,
                        help='Only schedule jobs with one of these Rules. '
                        'Comma separated list of strings. '
                        'UNTESTED: if used with --recursive.')
    parser.add_argument('--grid', default=None, type=str, metavar='CONF',
                        help='Perform grid search specified by the given conf. ')
    parser.add_argument('--patch-conf', default=[], type=str, metavar='CONF',
                        action='append',
                        help='Override variables specified by the given conf(s):. ')
    parser.add_argument('--blame', default=None, type=str,
                        help='When given a concrete filename, '
                        'shows which rule built it, and its inputs.')
    parser.add_argument('--platform', default=None, type=str,
                        help='Temporarily override the platform. '
                        'Generally not recommended. '
                        'Use the file current_platform instead.')
    parser.add_argument('--fail-running', default=False, action='store_true',
                        help='Mark all running jobs as failed. '
                        '(to kill zombies in exceptional cases)')
    parser.add_argument('--ingest-manual', default=False, action='store_true',
                        help='Mark all files of unknown origin as ok. '
                        '(if logs are out of sync for some reason)')

    parser.add_argument('--make', default=None, type=str, metavar='OUTPUT',
                        help='Output to make, in section:key format. '
                        'You should NOT call this directly')
    parser.add_argument('--overrides', default=None, type=str, metavar='str',
                        help='Overridden params in grid search. '
                        'You should NOT call this directly')

    return parser

class CLI(object):
    def __init__(self, recipe, argv=None):
        """Called before building the recipe"""
        self.recipe = recipe
        parser = get_parser(recipe)
        self.args = parser.parse_args(args=argv)
        self.conf = Config()
        self.conf.read(self.args.conf, self.args)
        for patch_conf_file in self.args.patch_conf:
            patch_conf = Config()
            patch_conf.read(patch_conf_file, self.args)
            self.conf = GridConfig.apply_override(self.conf, patch_conf)
        if self.args.grid is not None:
            self.grid_conf = GridConfig(self.args.grid, self.args)
        else:
            self.grid_conf = None
            if self.args.make is not None and self.args.overrides is not None:
                # when in making mode,
                # apply overrides before constructing recipe
                # to enable e.g. conf-based control flow
                overrides = parse_override_string(self.args.overrides)
                self.conf = GridConfig.apply_override(self.conf, overrides)
        # the recipe-altering cli args
        self.cli_args = None # FIXME
        self.platform = self.conf.platform
        # experimentlog is parsed and used to control behavior
        self.log = ExperimentLog(self.recipe, self.args.conf, self.platform)
        self.platform.read_log(self.log)
        self._configure_debug_logger()

    def _configure_debug_logger(self):
        # logger from logging module is used for stuff that is not parsed
        logger.setLevel(logging.DEBUG)
        os.makedirs('logs', exist_ok=True)
        logfile = os.path.join('logs', 'debug.{}.log'.format(self.recipe.name))
        fh = logging.FileHandler(logfile)
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.WARNING)
        logger.addHandler(fh)
        logger.addHandler(ch) 

    def main(self):
        """Called after building the recipe,
        when we are ready for action"""
        if self.args.check:
            self.check_validity()
            return  # don't do anything more
        if self.args.status:
            self.status()
            return  # don't do anything more
        if self.args.mtimes:
            self.mtimes()
            return  # don't do anything more
        if self.args.blame is not None:
            self.blame(self.args.blame)
            return  # don't do anything more
        if self.args.make is not None:
            self.make(self.args.make, self.args.overrides)
            return  # don't do anything more
        # implicit else 

        if self.grid_conf is None:
            nextsteps = self.recipe.get_next_steps_for(
                outputs=self.args.output,
                cli_args=self.cli_args,
                recursive=self.args.recursive)
        else:
            nextsteps = self.recipe.grid_next_steps(
                grid=self.grid_conf.get_overrides(self.conf),
                outputs=self.args.output,
                cli_args=self.cli_args,
                recursive=self.args.recursive)

        if self.args.resource_classes is not None:
            if self.args.recursive:
                print('WARNING: recursive with filtered is experimental')
            nextsteps = self._filter_by_resource(nextsteps,
                                                 self.args.resource_classes.split(','))
        if self.args.only_rules is not None:
            if self.args.recursive:
                print('WARNING: recursive with filtered is experimental')
            nextsteps = self._filter_by_rule(nextsteps,
                                             self.args.only_rules.split(','))

        if self.args.fail_running:
            for step in nextsteps.running:
                self.log.failed(step.job_id)
                print('Marked job {} as failed'.format(step.job_id))
            return  # don't do anything more

        if self.platform.make_immediately:
            # show before running locally
            self.show_next_steps(nextsteps,
                                 dryrun=self.args.dryrun,
                                 immediate=self.platform.make_immediately,
                                 verbose=self.args.verbose,
                                 show_all=self.args.show_all)
        if not self.args.dryrun:
            self.schedule(nextsteps)
        if not self.platform.make_immediately:
            # show after schduling on cluster
            self.show_next_steps(nextsteps,
                                 dryrun=self.args.dryrun,
                                 immediate=self.platform.make_immediately,
                                 verbose=self.args.verbose,
                                 show_all=self.args.show_all)

    def check_validity(self):
        # check that script is correctly named
        try:
            import __main__
            if self.recipe.name + '.py' not in __main__.__file__:
                raise Exception('Recipe name ({}) must match file name ({})'
                    .format(self.recipe.name, __main__.__file__))
        except AttributeError:
            print('**** unable to check filename of recipe')
        # check validity of interpolations in conf
        for section in self.conf.conf.sections():
            for key in self.conf.conf[section]:
                val = self.conf.conf[section][key]
                if not 'subconf' in section:
                    for tmpl in RE_TEMPLATE.findall(val):
                        print('UNFILLED TEMPLATE: {}:{} contains {}'.format(section, key, tmpl))
        print('Config interpolations OK')
        # exp section is assumed to always exist
        if 'exp' not in self.conf.conf.sections():
            print('********** WARNING! NO "exp" SECTION in conf **********')
        try:
            seed = self.conf.conf['exp']['seed']
        except KeyError:
            print('exp:seed NOT set, randomness is non-repeatable')
        if 'git' in self.platform.conf:
            if len(self.platform.conf['git']) == 0:
                print('********** WARNING! EMPTY gitdir list in platform conf **********')
            for key in self.platform.conf['git']:
                gitdir = self.platform.conf['git'][key]
                if not os.path.exists(gitdir) or \
                        not os.path.exists(os.path.join(gitdir, 'HEAD')):
                    print('********** WARNING! invalid gitdir **********')
                    print(key, gitdir)
        else:
            print('********** WARNING! NO "git" section in platform conf **********')
        os.makedirs('slurmlogs', exist_ok=True)
        # check existence of original inputs
        warn = False
        for rf in self.recipe.main_inputs:
            try:
                fname = rf(self.conf, self.cli_args)
                if rf.exists(self.conf, self.cli_args):
                    if os.stat(fname).st_size == 0:
                        print('EMPTY:   {} = {}'.format(rf.sec_key(), fname))
                        warn = True
                    else:
                        print('input OK:  {}'.format(fname))
                else:
                    print('MISSING:   {} = {}'.format(rf.sec_key(), fname))
                    warn = True
            except KeyError:
                print(    'UNDEFINED: {}'.format(rf.sec_key()))
                warn = True
        if warn:
            print('********** WARNING! Some inputs are missing **********')
        else:
            if 'paths.dirs' in self.conf.conf:
                # ensure that subdirs exist
                for subdir in self.conf.conf['paths.dirs'].values():
                    os.makedirs(subdir, exist_ok=True)
            else:
                print('********** WARNING! No paths.dirs defined')
        # check for self-clobbering outputs
        rev_paths = collections.defaultdict(set)
        for rf in self.recipe.all_outputs:
            try:
                fname = rf(self.conf, self.cli_args)
                rev_paths[fname].add((section, key))
            except KeyError:
                pass
        for path, sec_keys in rev_paths.items():
            if len(sec_keys) > 1:
                print('WARNING: {} all point to {}'.format(
                    ', '.join('{}:{}'.format(sec, key) for sec, key in sec_keys),
                    path))
        # check that output paths are in config
        warn = []
        for (rf, rule) in self.recipe.files.items():
            if rule == UNBOUND_OUTPUT:
                print('unbound output: {}:{}'.format(rf.section, rf.key))
            try:
                fname = rf(self.conf, self.cli_args)
                if fname == '':
                    print('empty path: {}:{}'.format(rf.section, rf.key))
            except KeyError:
                warn.append(rf)
        if warn:
            print('*** add these path definitions to config:')
            for sec, group in itertools.groupby(sorted(warn), key=lambda rf: rf.section):
                print('[paths.{}]'.format(sec))
                for rf in group:
                    print('{} = '.format(rf.key))
            print('********** WARNING! Some paths are missing **********')
        for dep, binary, components in self.recipe.opt_deps:
            msg = ', '.join(components)
            if not binary:
                try:
                    importlib.import_module(dep)
                    print('Module dep {} OK'.format(dep))
                except ImportError:
                    print('*** Unable to import optional dependency "{}"'.format(dep))
                    print('You will not be able to use {}'.format(msg))
            else:
                if run('which ' + dep, allow_fail=True).status_code != 0:
                    print('*** Optional binary "{}" not on PATH'.format(dep))
                    print('You will not be able to use {}'.format(msg))
                else:
                    print('Binary dep {} OK'.format(dep))
        #for (dep, msg) in OPT_DEPS:
        #for (dep, msg) in OPT_BINS:
        if self.args.dump_paths:
            for section in self.conf.conf.sections():
                if not section.startswith('paths.'):
                    continue
                print('[{}]'.format(section))
                for key in self.conf.conf[section]:
                    print('{} = {}'.format(key, self.conf.conf[section][key]))

    def status(self):
        files_by_job_id = collections.defaultdict(list)
        for (filepath, job_id) in sorted(self.log.outputs.items()):
            files_by_job_id[job_id].append(filepath)
        keyfunc = lambda x: x.exp
        for (exp, jobs) in itertools.groupby(
                sorted(self.log.status(), key=keyfunc), keyfunc):
            if exp not in self.log.ongoing_experiments:
                continue
            # FIXME: passing current args to another experiments conf
            exp_conf = Config(exp, self.args)
            print('=' * 80)
            print('Experiment: {}'.format(exp))
            tpls = []
            for job in sorted(jobs, key=lambda x: (x.status, x.time)):
                if job.status not in ('scheduled', 'running', 'failed'):
                    continue
                if self.args.quiet and job.status == 'failed':
                    # suppress failed jobs when quiet
                    continue
                # suppress failed jobs if it has been relaunched
                # (no longer the designated job for any files)
                if job.status == 'failed' and len(files_by_job_id[job.job_id]) == 0:
                    continue
                try:
                    rule = self.recipe.get_rule(job.sec_key)
                    if job.status == 'running':
                        monitoring = rule.monitor(self.platform, exp_conf, None)
                    else:
                        monitoring = '-'
                    # FIXME: truncate too long?
                    tpls.append(
                        (job.status, job.job_id, job.sec_key, rule.name, monitoring))
                except Exception:
                    #print('Rule for "{}" is obsolete'.format(job.sec_key))
                    pass
            table_print(tpls, line_before='-')
            # FIXME: if nothing is scheduled or running, check if more is available?

    def mtimes(self):
        inversions = self.recipe.check_mtime_inversions(
            outputs=self.args.output,
            cli_args=self.cli_args,
            dump_paths=self.args.dump_paths)
        if len(inversions) == 0:
            print('Everything in order')
        else:
            for cursor, inp, invtype in inversions:
                print('{}\t\t\t{} {}'.format(
                    cursor(self.conf, self.cli_args),
                    'is older than' if invtype == 'inversion' else 'orphan of',
                    inp(self.conf, self.cli_args)))

    def blame(self, concrete_output):
        abs_output = os.path.abspath(concrete_output)
        for rf in self.recipe.files:
            if os.path.abspath(rf(self.conf, self.cli_args)) == abs_output:
                tpls = []
                sec_key = rf.sec_key()
                rule = self.recipe.get_rule(sec_key)
                rule_name = rule.name if rule is not None else 'input'
                tpls.append(('Rule:', sec_key, rule_name, concrete_output))
                if rule is not None:
                    for inp in rule.inputs:
                        tpls.append((' ^input:', inp.sec_key(), '', inp(self.conf, self.cli_args)))
                table_print(tpls)
                return
        print('No rule to make {}'.format(concrete_output))

    def schedule(self, nextsteps):
        # output -> job_id of job that builds it
        wait_ids = {}
        for step in nextsteps.waiting + nextsteps.running:
            if step.job_id != '-':
                for output in step.outputs:
                    wait_ids[output] = step.job_id

        for step in nextsteps.available + nextsteps.delayed:
            wait_for_jobs = []
            unk_deps = False
            if step.rule.blocks_recursion:
                print('Not scheduling manual job "{}"'.format(step))
                continue
            for inp in step.inputs:
                if inp not in wait_ids:
                    if not self.platform.make_immediately:
                        print('Dont know what id to wait on for ', inp)
                        unk_deps = True
                    continue
                wait_for_jobs.append(wait_ids[inp])
            if unk_deps:
                continue
            if step.overrides:
                conf = GridConfig.apply_override(self.conf, step.overrides)
            else:
                conf = self.conf
            output_files = [(output.sec_key(), output(conf, self.cli_args))
                            for output in sorted(step.outputs)]
            job_id = self.platform.schedule(
                self.recipe, conf, step.rule, step.sec_key,
                output_files, self.cli_args, deps=wait_for_jobs,
                overrides=step.overrides, patches=self.args.patch_conf)
            if job_id is None:
                # not scheduled for some reason
                continue
            step.job_id = job_id
            self.log.scheduled(step.rule.name, step.sec_key, job_id, output_files)
            if step.job_id is not None and step.job_id != '-':
                for output in step.outputs:
                    wait_ids[output] = step.job_id
            try:
                self.platform.post_schedule(
                    job_id, self.recipe, self.conf, step.rule, step.sec_key,
                    output_files, self.cli_args, deps=wait_for_jobs,
                    overrides=step.overrides, patches=self.args.patch_conf)
            except Exception as e:
                self.log.failed(job_id)
                raise e
            except KeyboardInterrupt as ki:
                self.log.failed(job_id)
                raise ki

    def make(self, output, override_str):
        overrides = parse_override_string(override_str)
        next_steps = self.recipe.get_next_steps_for(
            outputs=[output], cli_args=self.cli_args, overrides=overrides)
        concat = next_steps.waiting + next_steps.running + next_steps.available
        if len(concat) == 0:
            raise Exception('Cannot start running {}: {}'.format(
                output, next_steps))
        next_step = concat[0]
        job_id = self.log.job_id_from_outputs(next_step.concrete)
        if job_id is None:
            raise Exception('No scheduled job id for {}'.format(
                next_step.concrete[0]))
        self._make_helper(output, next_step, job_id, overrides=overrides)

    def _make_helper(self, output, next_step, job_id, overrides=None):
        rule = self.recipe.files.get(next_step.outputs[0], None)
        self.log.started_running(next_step, job_id, rule.name)
        if overrides:
            conf = GridConfig.apply_override(self.conf, overrides)
        else:
            conf = self.conf
        conf.autolog_for_jobid(job_id, output)
        self.recipe.make_output(output=output, conf=conf, cli_args=self.cli_args)
        self.log.finished_running(next_step, job_id, rule.name)

    def show_next_steps(self, nextsteps, dryrun=False, immediate=False, verbose=False, show_all=False):
        # FIXME: don't filter out redundant scheduled?
        if not show_all:
            nextsteps = self._remove_redundant(nextsteps, dryrun=dryrun)
        albl = 'scheduled:'
        if dryrun:
            albl = 'available:'
        elif immediate:
            albl = 'immediate:'
        tpls = []
        for step in nextsteps.done + nextsteps.waiting + nextsteps.running:
            if self.args.quiet and step.status == 'done':
                # suppress done jobs if quiet
                continue
            outfile = step.concrete[0]
            logitem = self.log.get_status_of_output(outfile)
            lbl = step.status + ':'
            tpls.append((
                lbl, step.job_id, step.sec_key, outfile))
            # FIXME: show monitoring for running jobs here?
        table_print(tpls, line_before='-')
        tpls = []
        for step in nextsteps.available:
            outfile = step.concrete[0]
            rclass = step.rule.resource_class
            sec_key = step.outputs[0].sec_key()
            tpls.append((
                albl, step.job_id, sec_key, step.rule.name, rclass, outfile))
            if verbose:
                # also show other outputs
                for out in step.outputs[1:]:
                    tpls.append((
                        '     +out:', '', out.sec_key(), '+', rclass, out(self.conf, self.cli_args)))
                # step.inputs only has unsatisfied
                for inp in step.rule.inputs:
                    tpls.append((
                        '   ^input:', '', inp.sec_key(), '', '', inp(self.conf, self.cli_args)))
        for step in nextsteps.delayed:
            lbl = 'delayed:'
            rclass = step.rule.resource_class
            outfile = step.concrete[0]
            tpls.append((
                lbl, step.job_id, step.sec_key, step.rule.name, rclass, outfile))
        table_print(tpls, line_before='-')

    def _remove_redundant(self, nextsteps, dryrun=False):
        result = []
        for status in nextsteps:
            seen = set()    # filter each status separately
            result.append([])
            for step in status:
                try:
                    out = step.outputs[0]
                    idx = out.loop_index
                    key = (out.section, out.key, step.status)
                    if key not in seen or (not dryrun and step.status == 'available'):
                        # keep one from each loop
                        result[-1].append(step)
                    seen.add(key)
                except AttributeError:
                    # keep all non-loop
                    result[-1].append(step)
        return NextSteps(*result)

    def _filter_by_resource(self, nextsteps, classes):
        result = []
        removed = set()
        for status in nextsteps:
            result.append([])
            for step in status:
                if step.rule is None:
                    # unknown and uninteresting resouce class
                    result[-1].append(step)
                    continue
                if step.rule.resource_class in classes and \
                        all(inp not in removed for inp in step.inputs):
                    # valid resource class
                    result[-1].append(step)
                    continue
                # else remove
                removed.update(step.outputs)
        return NextSteps(*result)

    def _filter_by_rule(self, nextsteps, rules):
        result = []
        removed = set()
        for status in nextsteps:
            result.append([])
            for step in status:
                if step.rule is None:
                    # can't match if no name
                    continue
                if step.rule.name in rules and \
                        all(inp not in removed for inp in step.inputs):
                    # valid Rule
                    result[-1].append(step)
                    continue
                # else remove
                removed.update(step.outputs)
        return NextSteps(*result)

# keep a log of jobs
# - always: recipe, experiment id, timestamp
# - when something is launched
#   - job id (platform dependent, e.g. slurm or pid)
#   - which file(s) are being made
# - when it starts running
#   - git commit:  git --git-dir=/path/to/.git rev-parse HEAD  (or: git describe --always)
#   - git branch?
# - when it finishes running
#   - ended successfully
# - when you check status
#   - parse log to find jobs that should be waiting/running
#       - check their status (platform dependent), log the failed ones
#   - display some monitoring
#       - app and step dependent, e.g. last saved model, dev loss, number of output lines, eval, ...
#       - how long has it been running
# - manually: mark an experiment as ended (won't show up in status list anymore)

LogItem = collections.namedtuple('LogItem',
    ['time', 'recipe', 'exp', 'status', 'job_id', 'sec_key', 'rule'])
ITEM_UNKNOWN = LogItem('-', '-', '-', 'unknown', '-', '-', '-')
ITEM_NOT_SCHEDULED = LogItem('-', '-', '-', 'not scheduled', '-', '-', '-')

LOG_HISTORY = '%Y.%m.%d'
TIMESTAMP = '%d.%m.%Y %H:%M:%S'
GIT_FMT = '{time} {recipe} {exp} : {key} git commit {commit} branch {branch}'
LOG_FMT = '{time} {recipe} {exp} : status {status} {job_id} {sec_key} {rule}'
FILE_FMT = '{time} {recipe} {exp} : output {job} {sec_key} {filename}'
END_FMT = '{time} {recipe} {exp} : experiment ended'

LOG_RE = re.compile(r'([0-9\.]+ [0-9:]+) ([^ ]+) ([^ ]+) : status ([^ ]+) ([^ ]+) ([^ ]+) (.*)')
FILE_RE = re.compile(r'([0-9\.]+ [0-9:]+) ([^ ]+) ([^ ]+) : output ([^ ]+) ([^ ]+) (.*)')
END_RE = re.compile(r'([0-9\.]+ [0-9:]+) ([^ ]+) ([^ ]+) : experiment ended')

STATUSES = ('scheduled', 'running', 'done', 'failed')

class ExperimentLog(object):
    def __init__(self, recipe, conf, platform, history_days=15):
        self.recipe = recipe
        self.conf = conf
        self.platform = platform
        self.now = datetime.now()
        self.log_file_to_jobid_current = os.path.join(
            'logs', 'file_to_jobid.{date}.log'.format(
                date=self.now.strftime(LOG_HISTORY)))
        self.history_days = history_days
        self.log_horizon = self.now - timedelta(days=history_days)
        self.parsed_job_logs = {}
        self.outputs = {}
        self.ongoing_experiments = set()
        self._parse_combined_log()

    #def get_jobs_with_status(self, status='running'):
    #    """Returns e.g. Waiting and Running output files"""
    #    return [job_id for (job_id, job_status) in self.job_statuses.items()
    #            if job_status == status]

    def was_scheduled(self, outfile):
        """Returns True if this output was ever scheduled"""
        assert not isinstance(outfile, RecipeFile)
        job_id = self.outputs.get(outfile, None)
        return job_id is not None

    def get_status_of_output(self, outfile):
        # outfile: a concrete file path
        # status: a string from STATUSES
        # logitem: a LogItem
        assert not isinstance(outfile, RecipeFile)
        job_id = self.outputs.get(outfile, None)
        if job_id is None:
            return ITEM_NOT_SCHEDULED
        if job_id not in self.parsed_job_logs:
            job_logfile = os.path.join('logs', 'job.{}.{}.log'.format(
                self.recipe.name, job_id))
            self._parse_log(job_logfile)
        logitem = self.parsed_job_logs.get(job_id, ITEM_UNKNOWN)
        return logitem

    def update_status(self, platform_status, job_id, rf, conf, cli_args):
        if platform_status in ('unknown', 'local', 'running'):
            return
        if platform_status == 'done':
            self.missed_finish(job_id)
            return
        if platform_status != 'failed':
            print('WARNING: unknown platform_status {}'.format(platform_status))
        logitem = self.failed(job_id)
        self.parsed_job_logs[job_id] = logitem

    @property
    def last_job_id(self):
        return max([0] + list(int(x) for x in self.outputs.values()
                              if x != 'IngestedManual'))

    def job_id_from_outputs(self, concrete):
        job_ids = [self.outputs[out] for out in concrete
                   if out in self.outputs]
        job_ids = [int(x) for x in job_ids 
                   if x not in ('-', 'IngestedManual')]
        if len(job_ids) == 0:
            return None
        return max(job_ids)

    def status(self):
        """Status summary from deeply parsing log"""
        for outp in self.outputs.keys():
            log_item = self.get_status_of_output(outp)
            yield log_item

    def _parse_log(self, logfile):
        try:
            for line in open_text_file(logfile, mode='r'):
                line = line.strip()
                # git lines are ignored
                m = LOG_RE.match(line)
                if m:
                    exp = m.group(3)
                    status = m.group(4)
                    job_id = m.group(5)
                    status = 'done' if status == 'finished' else status
                    if status not in STATUSES:
                        print('unknown status {} in {}'.format(status, m.groups()))
                    if not job_id == '-':
                        self.parsed_job_logs[job_id] = LogItem(*m.groups())
                    self.ongoing_experiments.add(exp)
                    continue
                m = FILE_RE.match(line)
                if m:
                    exp = m.group(3)
                    job_id = m.group(4)
                    filename = m.group(6)
                    if not job_id == '-':
                        self.outputs[filename] = job_id
                    self.ongoing_experiments.add(exp)
                    continue
                m = END_RE.match(line)
                if m:
                    exp = m.group(3)
                    try:
                        self.ongoing_experiments.remove(exp)
                    except KeyError:
                        pass
        except FileNotFoundError:
            pass

    def _parse_combined_log(self):
        self._parse_log(self.log_file_to_jobid_current)
        date_cursor = self.log_horizon
        for _ in range(self.history_days):
            date_cursor += timedelta(days=1)
            logfile = os.path.join('logs', 'file_to_jobid.{date}.log'.format(
                date=date_cursor.strftime(LOG_HISTORY)))
            self._parse_log(logfile)
        #self._parse_log(os.path.join('logs', 'job.{}.local.log'.format(
        #        self.recipe.name)))


    # the status updates are written to job log file

    def _job_log_file(self, job_id):
        logfile = os.path.join('logs', 'job.{}.{}.log'.format(
            self.recipe.name,
            job_id if job_id != '-' else 'local'))
        return logfile

    def scheduled(self, rule, sec_key, job_id, output_files):
        # main sec_key is the one used as --make argument
        # output_files are (sec_key, concrete) tuples
        # job id (platform dependent, e.g. slurm or pid)
        logfile = self._job_log_file(job_id)
        timestamp = datetime.now().strftime(TIMESTAMP)
        log_item = LogItem(
            time=timestamp,
            recipe=self.recipe.name,
            exp=self.conf,
            status='scheduled',
            job_id=job_id,
            sec_key=sec_key,
            rule=rule)
        self._append_item(log_item, logfile=logfile)
        for (sub_sec_key, output) in output_files:
            self.outputs[output] = job_id
            self._append(FILE_FMT.format(
                time=timestamp,
                recipe=self.recipe.name,
                exp=self.conf,
                job=job_id,
                sec_key=sub_sec_key,
                filename=output,
                ))
        return log_item

    def started_running(self, step, job_id, rule):
        logfile = self._job_log_file(job_id)
        timestamp = datetime.now().strftime(TIMESTAMP)
        log_item = LogItem(
            time=timestamp,
            recipe=self.recipe.name,
            exp=self.conf,
            status='running',
            job_id=job_id,
            sec_key=step.sec_key,
            rule=rule)
        self._append_item(log_item, logfile=logfile)
        # alternative would be git describe --always, but that mainly works with tags
        for gitkey in  self.platform.conf['git']:
            gitdir = self.platform.conf['git'][gitkey]
            commit = run('git --git-dir={} rev-parse HEAD'.format(gitdir)).std_out.strip()
            branch = run('git --git-dir={} symbolic-ref --short HEAD'.format(gitdir)).std_out.strip()
            timestamp = datetime.now().strftime(TIMESTAMP)
            self._append(GIT_FMT.format(
                key=gitkey,
                time=timestamp,
                recipe=self.recipe.name,
                exp=self.conf,
                commit=commit,
                branch=branch,
                ),
                logfile=logfile)
        return log_item

    def finished_running(self, step, job_id, rule):
        logfile = self._job_log_file(job_id)
        timestamp = datetime.now().strftime(TIMESTAMP)
        log_item = LogItem(
            time=timestamp,
            recipe=self.recipe.name,
            exp=self.conf,
            status='done',
            job_id=job_id,
            sec_key=step.sec_key,
            rule=rule)
        self._append_item(log_item, logfile=logfile)
        return log_item

    def failed(self, job_id):
        logfile = self._job_log_file(job_id)
        timestamp = datetime.now().strftime(TIMESTAMP)
        if job_id in self.parsed_job_logs:
            fields = self.parsed_job_logs[job_id]
        else:
            fields = ITEM_UNKNOWN

        log_item = LogItem(
            time=timestamp,
            recipe=self.recipe.name,
            exp=fields.exp,
            status='failed',
            job_id=job_id,
            sec_key=fields.sec_key,
            rule=fields.rule,)
        self._append_item(log_item, logfile=logfile)
        return log_item

    def missed_finish(self, job_id):
        logfile = self._job_log_file(job_id)
        if job_id in self.parsed_job_logs:
            fields = self.parsed_job_logs[job_id]
        else:
            fields = ITEM_UNKNOWN
        timestamp = datetime.min.strftime(TIMESTAMP)
        log_item = LogItem(
            time=timestamp,
            recipe=self.recipe.name,
            exp=fields.exp,
            status='done',
            job_id=job_id,
            sec_key=fields.sec_key,
            rule=fields.rule)
        self._append_item(log_item, logfile=logfile)
        return log_item

    def ingest_manual(self, rf, config, cli_args):
        timestamp = datetime.now().strftime(TIMESTAMP)
        output = rf(config, cli_args)
        self._append(FILE_FMT.format(
            time=timestamp,
            recipe=self.recipe.name,
            exp=self.conf,
            job='IngestedManual',
            sec_key=rf.sec_key(),
            filename=output
            ))
        self.outputs[output] = 'IngestedManual'

    def _append_item(self, log_item, logfile=None):
        self._append(LOG_FMT.format(**log_item._asdict()), logfile=logfile)

    def _append(self, msg, logfile=None):
        os.makedirs('logs', exist_ok=True)
        logfile = logfile if logfile is not None else self.log_file_to_jobid_current
        with open_text_file(logfile, mode='a') as fobj:
            fobj.write(msg)
            fobj.write('\n')
