# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# Distributed computing example where this client sends computation to remote
# dispycos process to run as remote tasks. At any time at most one computation
# task is scheduled at a process, as the computation is supposed to be CPU heavy
# (although in this example they are not). Status of remote tasks can be
# monitored in a web browser at http://127.0.0.1:8181

import pycos.netpycos as pycos
from pycos.dispycos import *
import pycos.httpd


# user computations should be generator functions.
def compute(n, task=None):
    yield task.sleep(n)

def client_proc(computation, njobs, task=None):
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

    # run jobs
    for i in range(njobs):
        # computation is supposed to be CPU bound so 'run' is used so at most
        # one computations runs at a server at any time; for mostly idle
        # computations, use 'run_async' to run more than one computation at a
        # server at the same time.
        rtask = yield computation.run(compute, random.uniform(5, 10))
        if isinstance(rtask, pycos.Task):
            print('  job %s processed by %s' % (i, rtask.location))
        else:
            print('rtask %s failed: %s' % (i, rtask))

    # wait for all jobs to be done and close computation
    yield computation.close()


if __name__ == '__main__':
    import random, sys, pycos.dispycos
    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program), start
    # private scheduler:
    Scheduler()
    # send 'compute' generator function; use MinPulseInterval so node status
    # updates are sent more frequently (instead of default 2*MinPulseInterval)
    computation = Computation([compute], pulse_interval=pycos.dispycos.MinPulseInterval)

    # to illustrate relaying of status messages to multiple tasks, httpd is
    # also used in this example; this sets computation's status_task to httpd's status_task
    httpd = pycos.httpd.HTTPServer(computation)
    # run 10 (or given number of) jobs
    pycos.Task(client_proc, computation, 10 if len(sys.argv) < 2 else int(sys.argv[1])).value()
    # shutdown httpd only after computation is closed; alternately, close it in
    # 'client_proc' after the computation is closed.
    httpd.shutdown()
