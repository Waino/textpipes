import os
import shlex
import subprocess
import threading

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
        

class LogOnly(Platform):
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
            dep_args = ' --dependency=' + ','.join(
                'afterok:{}'.format(dep) for dep in deps)
        else:
            dep_args = ''
        print('DUMMY: sbatch --job-name {name} {rc_args}{dep_args} --wrap="{cmd}"'.format(
            name=job_name, cmd=cmd, rc_args=rc_args, dep_args=dep_args))
        # dummy incremental job_id
        self.job_id += 1 
        return self.job_id

    def check_job(self, job_id):
        return 'running'    # FIXME

class Local(Platform):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.make_immediately = True

    """Run immediately, instead of scheduling"""
    def schedule(self, recipe, conf, rule, sec_key, output_files, cli_args, deps=None):
        return None

    def check_job(self, job_id):
        return 'unknown'

class Slurm(Platform):
    """Schedule and return job id"""
    def schedule(self, recipe, conf, rule, sec_key, output_files, cli_args, deps=None):
        rc_args = self.resource_class(rule.resource_class)
        cmd = 'python {recipe}.py {conf}.ini --make {sec_key}'.format(
            recipe=recipe.name, conf=conf.name, sec_key=sec_key)
        job_name = '{}:{}'.format(conf.name, sec_key)
        sbatch = 'sbatch --job-name {name} --wrap="{cmd}" {rc_args}'.format(
            name=job_name, cmd=cmd, rc_args=rc_args)
        r = run(sbatch)
        try:
            job_id = int(r.std_out)
        except ValueError:
            raise Exception('Unexpected output from slurm: ' + r.describe())
        return job_id

    def check_job(self, job_id):
        # FIXME: parse output of slurm q
        return 'unknown'

classes = {
    'logonly': LogOnly,
    'local': Local,
    #'slurm': ,
}

class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None
        self.out = None
        self.err = None
        self.returncode = None
        self.data = None

    def run(self):
        def target():

            self.process = subprocess.Popen(self.cmd,
                universal_newlines=True,
                shell=False,
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
        self.returncode = self.process.returncode
        return self.out, self.err


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

def expand_args(command):
    """Parses command strings and returns a Popen-ready list."""
    if '|' in command or '>' in command:
        raise Exception('You can NOT use pipeing')

    return shlex.split(command, posix=True)


def run(command):
    """Executes given command as subprocess.
    You can NOT use pipeing. This is intentional,
    as pipeing large data would fail anyhow.
    If pipeing is necessary, use a subshell."""
    command = expand_args(command)
    cmd = Command(command)
    out, err = cmd.run()

    r = Response(process=cmd)

    r.command = command
    r.std_out = out
    r.std_err = err
    r.status_code = cmd.returncode

    return r
