# Run 'dispycosnode.py' program to start servers to execute computations sent by
# this client, along with this program.

# This example shows how to use 'httpd' module to start HTTP server so cluster /
# node / server / remote task status can be monitored in a web browser at
# http://127.0.0.1:8181

import pycos.netpycos as pycos
from pycos.dispycos import *


# objects of C are exchanged between client and servers
class C(object):
    def __init__(self, i):
        self.i = i
        self.n = None

    def __repr__(self):
        return '%d: %s' % (self.i, self.n)


# this generator function is sent to remote dispycos servers to run tasks there
def compute(obj, client, task=None):
    # obj is an instance of C
    yield task.sleep(obj.n)


def client_proc(computation, task=None):
    if (yield computation.schedule()):
        raise Exception('schedule failed')

    i = 0
    while True:
        cmd = yield task.receive()
        if cmd is None:
            break
        i += 1
        c = C(i)
        try:
            c.n = float(cmd)
        except:
            print('  "%s" is not a number' % cmd)
            continue
        else:
            # unlike in dispycos_client*.py, here 'run_async' is used to run as
            # many tasks as given on servers (i.e., possibly more than one task
            # on a server at any time).
            yield computation.run_async(compute, c, task)

    # close computation with 'await_async=True' to wait until all running async
    # tasks to finish before closing computation
    yield computation.close(await_async=True)


if __name__ == '__main__':
    import os, pycos.dispycos, pycos.httpd, sys, random
    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    Scheduler()
    # send generator function and class C (as the computation uses
    # objects of C)
    # use MinPulseInterval so node status updates are sent more frequently
    # (instead of default 2*MinPulseInterval)
    computation = Computation([compute, C], pulse_interval=pycos.dispycos.MinPulseInterval)
    # create http server to monitor nodes, servers, tasks
    http_server = pycos.httpd.HTTPServer(computation)
    task = pycos.Task(client_proc, computation)
    print('   Enter "quit" or "exit" to end the program, or ')
    print('   Enter a number to schedule a task on one of the servers')
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input

    while True:
        try:
            cmd = read_input().strip().lower()
            if cmd in ('quit', 'exit'):
                break
        except:
            break
        task.send(cmd)
    task.send(None)
    http_server.shutdown()
