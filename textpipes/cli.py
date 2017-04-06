import argparse
from datetime import datetime
import collections
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

def main(recipe):
    parser = get_parser(recipe)
    args = parser.parse_args()
    conf = Config(args.conf)
    platform = conf.platform
    cli_args = None # FIXME
    log = ExperimentLog(recipe, args.conf, platform)

    if args.check:
        # FIXME: implement check
        return  # don't do anything more
    if args.status:
        # FIXME: implement check
        return  # don't do anything more
    if args.make is not None:
        make(args.make, recipe, conf, cli_args, platform, log)
        return  # don't do anything more
    # implicit else 

    # debug
    nextsteps = recipe.get_next_steps_for(
        conf, log, outputs=args.output, cli_args=cli_args)
    if not args.dryrun:
        schedule(nextsteps, recipe, conf, cli_args, platform, log)
    show_next_steps(nextsteps, conf, cli_args,
                    dryrun=args.dryrun)

    #f len(nextsteps) == 0:
    #    print('all done')
    #else:
    #    for ns in nextsteps[0]:


def schedule(nextsteps, recipe, conf, cli_args, platform, log):
    job_ids = {}
    for step in nextsteps:
        if isinstance(step, Available):
            sec_key = step.outputs[0].sec_key()
            output_files = [(output.sec_key(), output(conf, cli_args))
                            for output in step.outputs]
            job_id = platform.schedule(
                recipe, conf, step.rule, sec_key, output_files, cli_args)
            job_ids[step] = job_id
            log.scheduled(step.rule.name, sec_key, job_id, output_files)
    return job_ids

def make(output, recipe, conf, cli_args, platform, log):
    next_step = recipe.get_next_steps_for(
        conf, log, outputs=[output], cli_args=cli_args)[0]
    if not isinstance(next_step, Waiting):
        raise Exception('Cannot start running {}: {}'.format(
            output, next_step))
    job_id = log.outputs[next_step.output(conf, cli_args)]
    rule = recipe.files.get(next_step.output, None)
    log.started_running(next_step, job_id, rule.name)
    recipe.make_output(conf, output=output, cli_args=cli_args)
    log.finished_running(next_step, job_id, rule.name)

def show_next_steps(nextsteps, conf, cli_args=None, dryrun=False, job_ids=None):
    job_ids = job_ids if job_ids is not None else {}
    for step in nextsteps:
        if isinstance(step, Done):
            print('Done: {}'.format(step.output(conf, cli_args)))
    print('-' * 80)
    for step in nextsteps:
        if isinstance(step, Waiting):
            job_id = job_ids.get(step, '-')
            print('Waiting: {} {}'.format(job_id, step.output(conf, cli_args)))
    for step in nextsteps:
        if isinstance(step, Running):
            # FIXME: monitoring? sec_key -> Rule
            job_id = job_ids.get(step, '-')
            print('Running: {} {}'.format(job_id, step.output(conf, cli_args)))
    print('-' * 80)
    lbl = 'Available' if dryrun else 'Scheduled'
    for step in nextsteps:
        if isinstance(step, Available):
            job_id = job_ids.get(step, '-')
            print('{}: {} {}\t{}\t{}'.format(
                lbl, job_id, step.outputs[0].sec_key(), step.rule.name,
                step.outputs[0](conf, cli_args)))

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

STATUSES = ('scheduled', 'running', 'finished', 'failed')

class ExperimentLog(object):
    def __init__(self, recipe, conf, platform):
        self.recipe = recipe
        self.conf = conf
        self.platform = platform
        self.logfile = os.path.join('logs', 'experiment.{}.log'.format(self.recipe.name))
        self._log_parsed = False
        self.jobs = {}
        self.job_statuses = {}
        self.outputs = {}

    def get_jobs_with_status(self, status='running'):
        """Returns e.g. Waiting and Running output files"""
        return [job_id for (job_id, job_status) in self.job_statuses.items()
                if job_status == status]

    def get_status_of_output(self, outfile):
        self._parse_log()
        job_id = self.outputs.get(outfile, None)
        if job_id is None:
            return 'not scheduled', None
        status = self.job_statuses.get(job_id, None)
        fields = self.jobs.get(job_id, None)
        return status, fields

    def get_summary(self):
        """Status summary from parsing log"""
        pass

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
            self._append(FILE_FMT.format(
                time=timestamp,
                recipe=self.recipe.name,
                exp=self.conf,
                job=job_id,
                sec_key=sub_sec_key,
                filename=output,
                ))

    def started_running(self, waiting, job_id, rule):
        timestamp = datetime.now().strftime(TIMESTAMP)
        self._append(LOG_FMT.format(
            time=timestamp,
            recipe=self.recipe.name,
            exp=self.conf,
            status='running',
            job=job_id,
            sec_key=waiting.output.sec_key(),
            rule=rule,
            ))
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
            ))

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
            ))

    def failed(self, job_id):
        # FIXME: need to cache fields, keyed by job_id
        pass

    def _append(self, msg):
        with open_text_file(self.logfile, mode='ab') as fobj:
            fobj.write(msg)
            fobj.write('\n')

    def _parse_log(self):
        if self._log_parsed:
            return
        self._log_parsed = True
        try:
            print('PARSING THE LOG')
            lines = open_text_file(self.logfile, mode='rb')
            for line in lines:
                line = line.strip()
                # git lines are ignored
                m = LOG_RE.match(line)
                if m:
                    status = m.group(4)
                    job_id = m.group(5)
                    if status not in STATUSES:
                        print('unknown status {} in {}'.format(status, tpl))
                    self.job_statuses[job_id] = status
                    self.jobs[job_id] = LogItem(*m.groups())
                    continue
                m = FILE_RE.match(line)
                if m:
                    job_id = m.group(4)
                    filename = m.group(6)
                    self.outputs[filename] = job_id
            waiting = self.get_jobs_with_status('scheduled')
            running = self.get_jobs_with_status('running')
            # check their status (platform dependent), log the failed ones
            for job_id in waiting + running:
                status = self.platform.check_job(job_id)
                if status == 'failed':
                    self.failed(job_id)
                    job_statuses[job_id] = 'failed'
                    redo = True
        except FileNotFoundError:
            pass
