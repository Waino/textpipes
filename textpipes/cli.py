import argparse
from datetime import datetime
import collections
import itertools
import os
import re

from .configuration import Config
from .platform import run
from .recipe import *
from .utils import *

def get_parser(recipe):
    parser = argparse.ArgumentParser(
        description='TextPipes (recipe: {})'.format(recipe.name))

    parser.add_argument('conf', type=str, metavar='CONF',
                        help='Name of the experiment conf file')
    parser.add_argument('output', type=str, nargs='*', metavar='OUTPUT',
                        help='Output(s) to schedule, in section:key format')
    parser.add_argument('--check', default=False, action='store_true',
                        help='Perform validity check')
    parser.add_argument('--status', default=False, action='store_true',
                        help='Status of ongoing experiments')
    parser.add_argument('--dryrun', default=False, action='store_true',
                        help='Show what would be done, but dont do it')
    parser.add_argument('-r', '--recursive', default=False, action='store_true',
                        help='Schedule the whole DAG recursively. '
                        'Default is to only schedule jobs that are ready to run.')
    parser.add_argument('--no-fork', default=False, action='store_true',
                        help='Do not use multiprocessing to speed up.')

    parser.add_argument('--make', default=None, type=str, metavar='OUTPUT',
                        help='Output to make, in section:key format. '
                        'You should NOT call this directly')

    return parser

class CLI(object):
    def __init__(self, recipe, argv=None):
        """Called before building the recipe"""
        self.recipe = recipe
        parser = get_parser(recipe)
        self.args = parser.parse_args(args=argv)
        self.conf = Config(self.args.conf, self.args)
        # the recipe-altering cli args
        self.cli_args = None # FIXME
        self.platform = self.conf.platform
        self.log = ExperimentLog(self.recipe, self.args.conf, self.platform)
        self.platform.read_log(self.log)

    def main(self):
        """Called after building the recipe,
        when we are ready for action"""
        if self.args.check:
            self.check_validity()
            return  # don't do anything more
        if self.args.status:
            self.status()
            return  # don't do anything more
        if self.args.make is not None:
            self.make(self.args.make)
            return  # don't do anything more
        # implicit else 

        nextsteps = self.recipe.get_next_steps_for(
            outputs=self.args.output,
            cli_args=self.cli_args,
            recursive=self.args.recursive)
        if not self.args.dryrun:
            self.schedule(nextsteps)
        self.show_next_steps(nextsteps,
                             dryrun=self.args.dryrun,
                             immediate=self.platform.make_immediately)
        if self.platform.make_immediately and not self.args.dryrun:
            self.make_all(nextsteps)

    def check_validity(self):
        # check validity of interpolations in conf
        for section in self.conf.conf.sections():
            for key in self.conf.conf[section]:
                self.conf.conf[section][key]
        print('Config interpolations OK')
        # exp section is assumed to always exist
        if 'exp' not in self.conf.conf.sections():
            print('********** WARNING! NO "exp" SECTION in conf **********')
        try:
            gitdir = self.platform.conf['git']['gitdir']
            if not os.path.exists(gitdir) or \
                    not os.path.exists(os.path.join(gitdir, 'HEAD')):
                print('********** WARNING! invalid gitdir **********')
                print(gitdir)
        except KeyError:
            print('********** WARNING! NO "git.gitdir" in platform conf **********')
        # check existence of original inputs
        warn = False
        for rf in self.recipe.main_inputs:
            fname = rf(self.conf, self.cli_args)
            if rf.exists(self.conf, self.cli_args):
                print('input OK: {}'.format(fname))
            else:
                print('MISSING:  {}'.format(fname))
                warn = True
        if warn:
            print('********** WARNING! Some inputs are missing **********')
        else:
            # ensure that subdirs exist
            for subdir in self.conf.conf['paths.dirs'].values():
                os.makedirs(subdir, exist_ok=True)
        # check that output paths are in config
        warn = False
        for rf in self.recipe.files:
            try:
                fname = rf(self.conf, self.cli_args)
            except KeyError:
                print('config is missing path: {}'.format(rf))
                warn = True
        if warn:
            print('********** WARNING! Some paths are missing **********')

    def status(self):
        files_by_job_id = collections.defaultdict(list)
        for (filepath, job_id) in sorted(self.log.outputs.items()):
            files_by_job_id[job_id].append(filepath)
        keyfunc = lambda x: x.exp
        for (exp, jobs) in itertools.groupby(
                sorted(self.log.jobs.values(), key=keyfunc), keyfunc):
            if exp not in self.log.ongoing_experiments:
                continue
            print('=' * 80)
            print('Experiment: {}'.format(exp))
            tpls = []
            for job in sorted(jobs, key=lambda x: (x.status, x.last_time)):
                status = self.log.job_statuses[job.job_id]
                if status not in ('scheduled', 'running', 'failed'):
                    continue
                # suppress failed jobs if it has been relaunched
                # (no longer the designated job for any files)
                if status == 'failed' and len(files_by_job_id[job.job_id]) == 0:
                    continue
                rule = self.recipe.get_rule(job.sec_key)
                if status == 'running':
                    monitoring = rule.monitor(self.platform, self.conf, self.cli_args)
                else:
                    monitoring = '-'
                # FIXME: truncate too long?
                tpls.append(
                    (status, job.job_id, job.sec_key, rule.name, monitoring))
            table_print(tpls, line_before='-')
            # FIXME: if nothing is scheduled or running, check if more is available?

    def schedule(self, nextsteps):
        # output -> job_id of job that builds it
        wait_ids = {}
        for step in nextsteps:
            if step.job_id != '-':
                for output in step.outputs:
                    wait_ids[output] = step.job_id
            # FIXME: delayed jobs: add deps
            if step.status != 'available':
                continue
            wait_for_jobs = []
            for inp in step.inputs:
                if inp not in wait_ids:
                    print('Dont know what id to wait on for ', inp)
                    continue
                wait_for_jobs.append(wait_ids[inp])
            output_files = [(output.sec_key(), output(self.conf, self.cli_args))
                            for output in sorted(step.outputs)]
            job_id = self.platform.schedule(
                self.recipe, self.conf, step.rule, step.sec_key,
                output_files, self.cli_args, deps=wait_for_jobs)
            if job_id is None:
                # not scheduled for some reason
                continue
            step.job_id = job_id
            self.log.scheduled(step.rule.name, step.sec_key, job_id, output_files)

    def make(self, output):
        next_step = self.recipe.get_next_steps_for(
            outputs=[output], cli_args=self.cli_args)[0]
        if not isinstance(next_step, Waiting):
            raise Exception('Cannot start running {}: {}'.format(
                output, next_step))
        job_id = self.log.outputs[next_step.output(self.conf, self.cli_args)]
        self._make_helper(output, next_step, job_id)

    def _make_helper(self, output, next_step, job_id):
        rule = self.recipe.files.get(next_step.outputs[0], None)
        self.log.started_running(next_step, job_id, rule.name)
        self.recipe.make_output(output=output, cli_args=self.cli_args)
        self.log.finished_running(next_step, job_id, rule.name)

    def make_all(self, nextsteps):
        """immediately, sequentially make everything"""
        remaining = [step for step in nextsteps
                     if step.status == 'available']
        while len(remaining) > 0:
            delayed = []
            for step in remaining:
                if any(not rf.exists(self.conf, self.cli_args)
                       for rf in step.inputs):
                    delayed.append(step)
                    continue
                self._make_helper(step.outputs[0], step, '-')
            if len(delayed) == len(remaining):
                raise Exception('Unmeetable dependencies')
            remaining = delayed

    def show_next_steps(self, nextsteps, dryrun=False, immediate=False):
        albl = 'scheduled:'
        if dryrun:
            albl = 'available:'
        elif immediate:
            albl = 'immediate:'
        tpls = []
        for step in nextsteps:
            if step.status == 'available':
                continue
            outfile = step.outputs[0](self.conf, self.cli_args)
            lbl = step.status + ':'
            tpls.append((
                lbl, step.job_id, step.sec_key, outfile))
            # FIXME: show monitoring for running jobs here?
        table_print(tpls, line_before='-')
        tpls = []
        for step in nextsteps:
            if step.status != 'available':
                continue
            if len(step.inputs) > 0:
                lbl = 'delayed:'
            else:
                lbl = albl
            outfile = step.outputs[0](self.conf, self.cli_args)
            tpls.append((
                lbl, step.job_id, step.sec_key, step.rule.name, outfile))
        table_print(tpls, line_before='-')

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
    ['last_time', 'recipe', 'exp', 'status', 'job_id', 'sec_key', 'rule'])

TIMESTAMP = '%d.%m.%Y %H:%M:%S'
GIT_FMT = '{time} {recipe} {exp} : git commit {commit} branch {branch}'
LOG_FMT = '{time} {recipe} {exp} : status {status} {job} {sec_key} {rule}'
FILE_FMT = '{time} {recipe} {exp} : output {job} {sec_key} {filename}'
END_FMT = '{time} {recipe} {exp} : experiment ended'

LOG_RE = re.compile(r'([0-9\.]+ [0-9:]+) ([^ ]+) ([^ ]+) : status ([^ ]+) ([^ ]+) ([^ ]+) (.*)')
FILE_RE = re.compile(r'([0-9\.]+ [0-9:]+) ([^ ]+) ([^ ]+) : output ([^ ]+) ([^ ]+) (.*)')
END_RE = re.compile(r'([0-9\.]+ [0-9:]+) ([^ ]+) ([^ ]+) : experiment ended')

STATUSES = ('scheduled', 'running', 'finished', 'failed')

class ExperimentLog(object):
    def __init__(self, recipe, conf, platform):
        self.recipe = recipe
        self.conf = conf
        self.platform = platform
        self.logfile = os.path.join('logs', 'experiment.{}.log'.format(self.recipe.name))
        self.jobs = {}
        self.job_statuses = {}
        self.outputs = {}
        self.ongoing_experiments = set()
        self._parse_combined_log()

    def get_jobs_with_status(self, status='running'):
        """Returns e.g. Waiting and Running output files"""
        return [job_id for (job_id, job_status) in self.job_statuses.items()
                if job_status == status]

    def get_status_of_output(self, outfile):
        # outfile: a concrete file path
        # status: a string from STATUSES
        # fields: a LogItem
        job_id = self.outputs.get(outfile, None)
        if job_id is None:
            return 'not scheduled', None
        status = self.job_statuses.get(job_id, None)
        fields = self.jobs.get(job_id, None)
        return status, fields

    def get_summary(self):
        """Status summary from parsing log"""
        pass

    def _parse_log(self, logfile):
        try:
            for line in open_text_file(logfile, mode='rb'):
                line = line.strip()
                # git lines are ignored
                m = LOG_RE.match(line)
                if m:
                    exp = m.group(3)
                    status = m.group(4)
                    job_id = m.group(5)
                    if status not in STATUSES:
                        print('unknown status {} in {}'.format(status, m.groups()))
                    if not job_id == '-':
                        self.job_statuses[job_id] = status
                        self.jobs[job_id] = LogItem(*m.groups())
                    self.ongoing_experiments.add(exp)
                    continue
                m = FILE_RE.match(line)
                if m:
                    job_id = m.group(4)
                    filename = m.group(6)
                    if not job_id == '-':
                        self.outputs[filename] = job_id
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
        self._parse_log(self.logfile)

        waiting = self.get_jobs_with_status('scheduled')
        running = self.get_jobs_with_status('running')
        # check their status (platform dependent), log the failed ones
        for job_id in waiting + running:
            job_logfile = os.path.join('logs', 'job.{}.{}.log'.format(
                self.recipe.name, job_id))
            self._parse_log(job_logfile)
            status = self.platform.check_job(job_id)
            if status == 'finished':
                self.consolidate_finished(job_id)
                self.job_statuses[job_id] = 'finished'
            elif status == 'failed':
                self.failed(job_id)
                self.job_statuses[job_id] = 'failed'

    def scheduled(self, rule, sec_key, job_id, output_files):
        # main sec_key is the one used as --make argument
        # output_files are (sec_key, concrete) tuples
        # job id (platform dependent, e.g. slurm or pid)
        timestamp = datetime.now().strftime(TIMESTAMP)
        self._append(LOG_FMT.format(
            time=timestamp,
            recipe=self.recipe.name,
            exp=self.conf,
            status='scheduled',
            job=job_id,
            sec_key=sec_key,
            rule=rule,
            ))
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

    def consolidate_finished(self, job_id):
        timestamp = datetime.now().strftime(TIMESTAMP)
        if job_id in self.jobs:
            fields = self.jobs[job_id]
        else:
            fields = LogItem(*['-'] * len(LogItem._fields))

        self._append(LOG_FMT.format(
            time=timestamp,
            recipe=self.recipe.name,
            exp=fields.exp,
            status='finished',
            job=job_id,
            sec_key=fields.sec_key,
            rule=fields.rule,
            ))
        pass

    def failed(self, job_id):
        timestamp = datetime.now().strftime(TIMESTAMP)
        if job_id in self.jobs:
            fields = self.jobs[job_id]
        else:
            fields = LogItem(*['-'] * len(LogItem._fields))

        self._append(LOG_FMT.format(
            time=timestamp,
            recipe=self.recipe.name,
            exp=fields.exp,
            status='failed',
            job=job_id,
            sec_key=fields.sec_key,
            rule=fields.rule,
            ))
        pass

    # the following two are written to job log file

    def started_running(self, step, job_id, rule):
        logfile = os.path.join('logs', 'job.{}.{}.log'.format(
            self.recipe.name,
            job_id if job_id != '-' else 'local'))
        timestamp = datetime.now().strftime(TIMESTAMP)
        self._append(LOG_FMT.format(
            time=timestamp,
            recipe=self.recipe.name,
            exp=self.conf,
            status='running',
            job=job_id,
            sec_key=step.sec_key,
            rule=rule,
            ),
            logfile=logfile)
        # alternative would be git describe --always, but that mainly works with tags
        gitdir = self.platform.conf['git']['gitdir']
        commit = run('git --git-dir={} rev-parse HEAD'.format(gitdir)).std_out.strip()
        branch = run('git --git-dir={} symbolic-ref --short HEAD'.format(gitdir)).std_out.strip()
        timestamp = datetime.now().strftime(TIMESTAMP)
        self._append(GIT_FMT.format(
            time=timestamp,
            recipe=self.recipe.name,
            exp=self.conf,
            commit=commit,
            branch=branch,
            ),
            logfile=logfile)

    def finished_running(self, step, job_id, rule):
        logfile = os.path.join('logs', 'job.{}.{}.log'.format(
            self.recipe.name,
            job_id if job_id != '-' else 'local'))
        timestamp = datetime.now().strftime(TIMESTAMP)
        self._append(LOG_FMT.format(
            time=timestamp,
            recipe=self.recipe.name,
            exp=self.conf,
            status='finished',
            job=job_id,
            sec_key=step.sec_key,
            rule=rule,
            ),
            logfile=logfile)

    def _append(self, msg, logfile=None):
        os.makedirs('logs', exist_ok=True)
        logfile = logfile if logfile is not None else self.logfile
        with open_text_file(logfile, mode='ab') as fobj:
            fobj.write(msg)
            fobj.write('\n')
