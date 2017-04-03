import argparse
from datetime import datetime
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

    # debug
    nextsteps = recipe.get_next_steps_for(conf)
    if not args.dryrun:
        schedule(nextsteps, conf, cli_args, platform, log)
    show_next_steps(nextsteps, conf, cli_args,
                    dryrun=args.dryrun)

    #f len(nextsteps) == 0:
    #    print('all done')
    #else:
    #    for ns in nextsteps[0]:


def schedule(nextsteps, conf, cli_args, platform, log):
    job_ids = {}
    for step in nextsteps:
        if isinstance(step, Available):
            sec_key = step.outputs[0].sec_key()
            output_files = [(output.sec_key(), output(conf, cli_args))
                            for output in step.outputs]
            # FIXME: platform.foobar
            job_id = 'dummy_id'
            job_ids[step] = job_id
            log.scheduled(step.rule.name, sec_key, job_id, output_files)
    return job_ids

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
            # FIXME: monitoring? seckey -> Rule
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

TIMESTAMP = '%d.%m.%Y %H:%M:%S'
GIT_FMT = '{time} {recipe} {exp} : git commit {commit} branch {branch}'
LOG_FMT = '{time} {recipe} {exp} : {status} {job} {seckey} {rule}'
FILE_FMT = '{time} {recipe} {exp} : output {job} {seckey} {filename}'
END_FMT = '{time} {recipe} {exp} : experiment ended'

LOG_RE = re.compile(r'([0-9\.]+ [0-9:]+) ([^ ]+) ([^ ]+) : ([^ ]+) ([^ ]+) ([^ ]+) (.*)')

class ExperimentLog(object):
    def __init__(self, recipe, conf, platform):
        self.recipe = recipe
        self.conf = conf
        self.platform = platform
        self.logfile = os.path.join('logs', 'experiment.{}.log'.format(self.recipe.name))

    def get_running_jobs(self):
        """Returns Waiting and Running output files"""
#   - parse log to find jobs that should be waiting/running
#       - check their status (platform dependent), log the failed ones
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
            seckey=sec_key,
            rule=rule,
            ))
        for (sub_sec_key, output) in output_files:
            self._append(FILE_FMT.format(
                time=timestamp,
                recipe=self.recipe.name,
                exp=self.conf,
                job=job_id,
                seckey=sub_sec_key,
                filename=output,
                ))
        self.started_running(None, job_id)    # FIXME

    def started_running(self, available, job_id):
#   - git commit:  git --git-dir=/path/to/.git rev-parse HEAD  (or: git describe --always, git symbolic-ref --short HEAD)
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

    def finished_running(self, available, job_id):
        pass

    def failed(self, available, job_id):
        pass

    def status(self):
        pass

    def _append(self, msg):
        with open_text_file(self.logfile, mode='ab') as fobj:
            fobj.write(msg)
            fobj.write('\n')

    def _parse_log(self):
        lines = open_text_file(self.logfile, mode='rb')
        for line in lines:
            line = line.strip()
            # ignore git lines
            m = LOG_RE.match(line)
            if m:
                print(m.groups())
