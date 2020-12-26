# Example uses RPS to run log monitor service.

# Run this on each node to monitor and 'rps_log_watch_client' on a client to get log messages that
# match a pattern from all the nodes to that client

import sys
import os
import subprocess
import pycos
import pycos.asyncfile
# import netpycos to add networking to Pycos
import pycos.netpycos

if os.name == 'nt':
    from pycos.asyncfile import Popen
else:
    from subprocess import Popen

# PyPI / pip packaging adjusts assertion below for Python 3.7+
if sys.version_info.major == 3:
    assert sys.version_info.minor < 7, \
        ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
         (__file__, sys.version_info.major, sys.version_info.minor))


# this task is invoked by client with RPS; runs 'tail -f' on log file piped through 'grep'
def rps_log_monitor(client, task=None):
    if not isinstance(client, pycos.Task):
        pycos.logger.warning('Invalid client: %s', type(client))
        raise StopIteration
    pycos.logger.info('%s serving client %s', task, client)
    tail = Popen(['tail', '-f', log_file], stdout=subprocess.PIPE)
    grep = Popen(['grep', '-i', '--line-buffered', pattern],
                 stdin=tail.stdout, stdout=subprocess.PIPE)
    pipe = pycos.asyncfile.AsyncPipe(tail, grep)
    while True:
        try:
            line = yield pipe.readline()
            if not line:
                break
        except Exception:
            pycos.logger.warning('read failed')
            break
        if client.send((task, line)):
            pycos.logger.warning('Could not send message to %s', client)
            break
    pipe.terminate()


if __name__ == '__main__':
    pycos.logger.setLevel(pycos.Logger.DEBUG)

    # file to monitor - can be changed with first argument to this program
    log_file = '/var/log/auth.log'
    # 'grep' regexp pattern to filter - can be changed with second argument to this program
    pattern = 'failed'
    if len(sys.argv) > 1:
        log_file = sys.argv[1]
        if len(sys.argv) > 2:
            pattern = sys.argv[2]

    # use 'secret' so peers that use same secret are recognized
    scheduler = pycos.Pycos(name='server', secret='LogMon')
    # register rps_server so remote clients can request execution
    rps = pycos.RPS(rps_log_monitor)
    rps.register()

    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input
    while True:
        try:
            line = read_input('Enter "quit" or "exit" to quit: ').strip().lower()
            if line in ('quit', 'exit'):
                break
        except Exception:
            break
    # TODO: maintain and terminate server tasks?
