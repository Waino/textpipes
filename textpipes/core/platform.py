import logging
import os
import re
import shlex
import subprocess
import threading

logger = logging.getLogger('textpipes')

MULTISPACE_RE = re.compile(r'\s+')

# built-in resource classes:
# make_immediately, default, short
# other suggestions:
# gpu, gpushort, multicore, bigmem, long

class Platform(object):
    def __init__(self, name, conf):
        self.name = name
        self.conf = conf
        self.make_immediately = False

    def read_log(self, log):
        pass

    def schedule(self, recipe, conf, rule, sec_key, output_files, cli_args, log):
        # -> job id (or None if not scheduled)
        raise NotImplementedError()

    def check_job(self, job_id):
        raise NotImplementedError()

    def resource_class(self, resource_class):
        if 'resource_classes' not in self.conf:
            return ''
        if 'resource_classes.map' in self.conf:
            resource_class = self.conf['resource_classes.map'].get(
                resource_class, resource_class)
        if resource_class not in self.conf['resource_classes']:
            return self.conf['resource_classes']['default']
        return self.conf['resource_classes'][resource_class]


class Local(Platform):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.make_immediately = True

    """Run immediately, instead of scheduling"""
    def schedule(self, recipe, conf, rule, sec_key, output_files, cli_args, deps=None):
        return None

    def check_job(self, job_id):
        return 'unknown'

# --gres=gpu:1 -p gpushort --mem=5000 --time=0-04:00:00
# --time=5-00:00:00 --mem=23500
# --gres=gpu:teslak80:1
# --exclude=gpu12,gpu13,gpu14,gpu15,gpu16
# -p coin --mem=30000

SLURM_STATUS_MAP = {
    'RUNNING': 'running',
    'COMPLETI': 'running',
    'PENDING': 'scheduled',
    'COMP': 'finished',
    'FAIL': 'failed',
    'CANC': 'failed',}
RE_SLURM_SUBMITTED_ID = re.compile(r'Submitted batch job (\d*)')

class Slurm(Platform):
    """Schedule and return job id"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._job_status = None

    def schedule(self, recipe, conf, rule, sec_key, output_files, cli_args, deps=None):
        rc_args = self.resource_class(rule.resource_class)
        assert rc_args != 'make_immediately'
        cmd = 'python {recipe}.py {conf}.ini --make {sec_key}'.format(
            recipe=recipe.name, conf=conf.name, sec_key=sec_key)
        job_name = '{}:{}'.format(conf.name, sec_key)
        if deps:
            dep_args = ' --dependency=afterok:' + ':'.join(
                str(dep) for dep in deps)
        else:
            dep_args = ''
        sbatch = 'sbatch --job-name {name} {rc_args}{dep_args} --wrap="{cmd}"'.format(
            name=job_name, cmd=cmd, rc_args=rc_args, dep_args=dep_args)
        r = run(sbatch)
        try:
            job_id = int(RE_SLURM_SUBMITTED_ID.match(r.std_out).group(1))
        except Exception:
            raise Exception('Unexpected output from slurm: ' + r.describe())
        return job_id

    def check_job(self, job_id):
        if self._job_status is None:
            self._parse_q()
        if job_id in self._job_status:
            (_, _, status, _) = self._job_status[job_id]
            result = SLURM_STATUS_MAP.get(status, status)
            return result
        return 'unknown'

    def _parse_q(self):
        self._job_status = {}
        r = run('slurm q')
        for (i, line) in enumerate(r.std_out.split('\n')):
            if i == 0:
                continue
            line = line.strip()
            if len(line) == 0:
                continue
            fields = MULTISPACE_RE.split(line)
            (job_id, _, _, time, start, status) = fields[:6]
            reason = ' '.join(fields[6:])
            # FIXME: non-slurm-specific namedtuple?
            self._job_status[job_id] = (time, start, status, reason)
        r = run('slurm history')
        for (i, line) in enumerate(r.std_out.split('\n')):
            if i == 0:
                continue
            line = line.strip()
            if len(line) < 12:
                continue
            fields = MULTISPACE_RE.split(line)
            job_id = fields[0]
            time = fields[6]
            start = fields[2]
            status = fields[12]
            reason = ''
            # FIXME: non-slurm-specific namedtuple?
            self._job_status[job_id] = (time, start, status, reason)

class LogOnly(Slurm):
    """dummy platform for testing"""
    def read_log(self, log):
        self.job_id = max([0] + list(int(x) for x in log.jobs.keys()))

    def schedule(self, recipe, conf, rule, sec_key, output_files, cli_args, deps=None):
        rc_args = self.resource_class(rule.resource_class)
        # FIXME: formatting cli args
        cmd = 'python {recipe}.py {conf}.ini --make {sec_key}'.format(
            recipe=recipe.name, conf=conf.name, sec_key=sec_key)
        job_name = '{}:{}'.format(conf.name, sec_key)
        if deps:
            dep_args = ' --dependency=afterok:' + ':'.join(
                str(dep) for dep in deps)
        else:
            dep_args = ''
        print('DUMMY: sbatch --job-name {name} {rc_args}{dep_args} --wrap="{cmd}"'.format(
            name=job_name, cmd=cmd, rc_args=rc_args, dep_args=dep_args))
        # dummy incremental job_id
        self.job_id += 1 
        return self.job_id

    def check_job(self, job_id):
        if self._job_status is None:
            self._parse_q()
        return 'running'    # FIXME


classes = {
    'logonly': LogOnly,
    'local': Local,
    'slurm': Slurm,
}

class Command(object):
    def __init__(self, cmd):
        if '|' in cmd or '>' in cmd:
            # subshell args should not be split
            self.cmd = cmd
            self.subshell = True
        else:
            self.cmd = shlex.split(cmd, posix=True)
            self.subshell = False
        self.process = None
        self.out = None
        self.err = None
        self.returncode = None
        self.data = None

    def run(self):
        if self.subshell:
            self._run_shell()
        else:
            self._run_popen()
        return self.out, self.err

    def _run_shell(self):
        self.returncode = subprocess.check_call(
            self.cmd, shell=True)
        self.out, self.err = None, None

    def _run_popen(self):
        def target():
            self.process = subprocess.Popen(
                self.cmd,
                universal_newlines=True,
                shell=self.subshell,
                env=os.environ,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
            self.out, self.err = self.process.communicate(None)

        thread = threading.Thread(target=target)
        thread.start()

        thread.join()
        if thread.is_alive():
            self.process.terminate()
            thread.join()
        if self.process is None:
            raise Exception('Running failed: {}'.format(self.cmd))
        self.returncode = self.process.returncode


class Response(object):
    """A command's response"""

    def __init__(self, process=None):
        super(Response, self).__init__()

        self._process = process
        self.command = None
        self.std_err = None
        self.std_out = None
        self.status_code = None
        self.history = []

    def __repr__(self):
        if len(self.command):
            return '<Response [{0}]>'.format(self.command[0])
        else:
            return '<Response>'

    def describe(self):
        return ('command:\n{}\nreturn code:\n{}\n'
            'stdout:\n{}\nstderr:\n{}\n'.format(
                self.command, self.status_code,
                self.std_out, self.std_err))



def run(command, allow_fail=False):
    """Executes given command as subprocess.
    If pipeing is necessary, uses a subshell."""
    logger.info(command)
    cmd = Command(command)
    out, err = cmd.run()

    r = Response(process=cmd)

    r.command = command
    r.std_out = out
    r.std_err = err
    r.status_code = cmd.returncode

    if not allow_fail:
        if r.status_code != 0:
            print(r.std_err)
            raise Exception('Nonzero status code {} when running {}'.format(
                r.status_code, r.command))

    return r
