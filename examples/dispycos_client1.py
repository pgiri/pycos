# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# Distributed computing example where this client sends computation ('compute'
# function) to remote dispycos servers to run as remote tasks and obtain
# results. At any time at most one computation task is scheduled at a process,
# as the computation is supposed to be CPU heavy (although in this example they
# are not).

import pycos
import pycos.netpycos
from pycos.dispycos import *


# this generator function is sent to remote dispycos servers to run tasks there
def compute(i, n, task=None):
    import time
    yield task.sleep(n)  # computation is simulated here with sleep
    raise StopIteration((i, task.location, time.asctime()))  # result of 'compute' is current time


# client (local) task submits tasks
def client_proc(client, njobs, task=None):
    # schedule client with the scheduler; scheduler accepts one client
    # at a time, so if scheduler is shared, the client is queued until it
    # is done with already scheduled clients
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    # arguments must correspond to arguments for computaiton; multiple arguments
    # (as in this case) can be given as tuples
    args = [(i, random.uniform(2, 5)) for i in range(njobs)]
    results = yield client.run_results(compute, args)
    # Tasks may not be executed in the order of given list of args, but
    # results would be in the same order of given list of args
    for result in results:
        print('    result for %d from %s: %s' % result)

    # wait for all jobs to be done and close client
    yield client.close()


if __name__ == '__main__':
    import sys, random
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # PyPI / pip packaging adjusts assertion below for Python 3.7+
    if sys.version_info.major == 3:
        assert sys.version_info.minor < 7, \
            ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
             (__file__, sys.version_info.major, sys.version_info.minor))

    # if scheduler is not already running (on a node as a program), start
    # private scheduler:
    Scheduler()
    # package client fragments
    client = Client([compute])
    # run 10 (or given number of) jobs
    pycos.Task(client_proc, client, 10 if len(sys.argv) < 2 else int(sys.argv[1]))
