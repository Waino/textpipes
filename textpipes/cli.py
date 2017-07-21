import argparse
from datetime import datetime
import collections
import itertools
import os
import re

from .configuration import Config
from .platform import run, MakeImmediately
from .recipe import *
from .utils import *

def get_parser(recipe):
    parser = argparse.ArgumentParser(
        description='TextPipes (recipe: {})'.format(recipe.name))

    parser.add_argument('conf', type=str, metavar='CONF',
                        help='Name of the experiment conf file')
    parser.add_argument('output', type=str, nargs='*', metavar='OUTPUT',
                        help='Output(s) to schedule, in section:key format')
    parser.add_argument('--status', default=False, action='store_true',
                        help='Status of ongoing experiments')
    parser.add_argument('--check', default=False, action='store_true',
                        help='Perform validity check')
    parser.add_argument('--dryrun', default=False, action='store_true',
                        help='Show what would be done, but dont do it')
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
        self.conf = Config(self.args.conf)
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

        # debug
        nextsteps = self.recipe.get_next_steps_for(
            outputs=self.args.output, cli_args=self.cli_args)
        if not self.args.dryrun:
            self.schedule(nextsteps)
        self.show_next_steps(nextsteps, dryrun=self.args.dryrun)


    def check_validity(self):
        # check validity of interpolations in conf
        for section in self.conf.conf.sections():
            for key in self.conf.conf[section]:
                self.conf.conf[section][key]
        print('Config interpolations OK')
        # exp section is assumed to always exist
        if 'exp' not in self.conf.conf.sections():
            print('********** WARNING! NO "exp" SECTION in conf **********')
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
            print('*** Experiment: {}'.format(exp))
            for job in sorted(jobs, key=lambda x: (x.status, x.last_time)):
                status = self.log.job_statuses[job.job_id]
                if status not in ('scheduled', 'running'):
                    continue
                rule = self.recipe.get_rule(job.sec_key)
                if status == 'running':
                    monitoring = rule.monitor(self.platform, files_by_job_id[job.job_id])
                else:
                    monitoring = '-'
                # FIXME: truncate too long?
                print('{job_id:10} {rule:15} {sec_key:25} {status:10} {monitoring}'.format(
                    job_id=job.job_id,
                    rule=rule.name,
                    sec_key=job.sec_key,
                    status=status,
                    monitoring=monitoring))
            # FIXME: if nothing is scheduled or running, check if more is available?

    def schedule(self, nextsteps):
        job_ids = {}
        for step in nextsteps:
            if isinstance(step, Available):
                sec_key = step.outputs[0].sec_key()
                output_files = [(output.sec_key(), output(self.conf, self.cli_args))
                                for output in sorted(step.outputs)]
                job_id = self.platform.schedule(
                    self.recipe, self.conf, step.rule, sec_key,
                    output_files, self.cli_args)
                if job_id is None:
                    # not scheduled for some reason
                    continue
                elif job_id == MakeImmediately:
                    # (small) local job to make instead of schedule
                    # FIXME: will missing job_id break stuff?
                    self._make_helper(step.outputs[0],
                                      Waiting(step.outputs[0]),
                                      '-')
                    continue
                job_ids[step] = job_id
                self.log.scheduled(step.rule.name, sec_key, job_id, output_files)
        return job_ids

    def make(self, output):
        next_step = self.recipe.get_next_steps_for(
            outputs=[output], cli_args=self.cli_args)[0]
        if not isinstance(next_step, Waiting):
            raise Exception('Cannot start running {}: {}'.format(
                output, next_step))
        job_id = self.log.outputs[next_step.output(self.conf, self.cli_args)]
        self._make_helper(output, next_step, job_id)

    def _make_helper(self, output, next_step, job_id):
        rule = self.recipe.files.get(next_step.output, None)
        self.log.started_running(next_step, job_id, rule.name)
        self.recipe.make_output(output=output, cli_args=self.cli_args)
        self.log.finished_running(next_step, job_id, rule.name)

    def show_next_steps(self, nextsteps, dryrun=False):
        for step in nextsteps:
            if isinstance(step, Done):
                print('Done: {}'.format(step.output(self.conf, self.cli_args)))
        print('-' * 80)
        for step in nextsteps:
            if isinstance(step, Waiting):
                outfile = step.output(self.conf, self.cli_args)
                job_id = self.log.outputs.get(outfile, '-')
                print('Waiting: {} {}'.format(job_id, outfile))
        for step in nextsteps:
            if isinstance(step, Running):
                # FIXME: show monitoring here?
                outfile = step.output(self.conf, self.cli_args)
                job_id = self.log.outputs.get(outfile, '-')
                print('Running: {} {}'.format(job_id, outfile))
        tpls = []
        for step in nextsteps:
            if isinstance(step, Available):
                outfile = step.outputs[0](self.conf, self.cli_args)
                job_id = self.log.outputs.get(outfile, '-')
                if dryrun:
                    lbl = 'Available:'
                else:
                    if job_id in ('-', MakeImmediately):
                        lbl = 'Immediate:'
                    else:
                        lbl = 'Scheduled:'
                if job_id == MakeImmediately:
                    job_id = '-'
                tpls.append((
                    lbl, job_id, step.outputs[0].sec_key(), step.rule.name, outfile))
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

    def started_running(self, waiting, job_id, rule):
        logfile = os.path.join('logs', 'job.{}.{}.log'.format(
            self.recipe.name, job_id))
        timestamp = datetime.now().strftime(TIMESTAMP)
        self._append(LOG_FMT.format(
            time=timestamp,
            recipe=self.recipe.name,
            exp=self.conf,
            status='running',
            job=job_id,
            sec_key=waiting.output.sec_key(),
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

    def finished_running(self, running, job_id, rule):
        timestamp = datetime.now().strftime(TIMESTAMP)
        self._append(LOG_FMT.format(
            time=timestamp,
            recipe=self.recipe.name,
            exp=self.conf,
            status='finished',
            job=job_id,
            sec_key=running.output.sec_key(),
            rule=rule,
            ),
            logfile=os.path.join('logs', 'job.{}.{}.log'.format(
                self.recipe.name, job_id))
            )

    def _append(self, msg, logfile=None):
        os.makedirs('logs', exist_ok=True)
        logfile = logfile if logfile is not None else self.logfile
        with open_text_file(logfile, mode='ab') as fobj:
            fobj.write(msg)
            fobj.write('\n')
