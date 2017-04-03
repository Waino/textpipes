import os
import shlex
import subprocess
import threading

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


def expand_args(command):
    """Parses command strings and returns a Popen-ready list."""
    if '|' in command or '>' in command:
        raise Exception('You can NOT use pipeing')

    return shlex.split(command, posix=True)


def run(command):
    """Executes given command as subprocess.
    You can NOT use pipeing. This is intentional,
    as pipeing large data would fail anyhow."""
    command = expand_args(command)
    cmd = Command(command)
    out, err = cmd.run()

    r = Response(process=cmd)

    r.command = command
    r.std_out = out
    r.std_err = err
    r.status_code = cmd.returncode

    return r


