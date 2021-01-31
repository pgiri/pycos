# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# Distributed computing example where this client sends computation ('compute'
# function) to remote dispycos servers to run as remote tasks and obtain
# results. At any time at most one computation task is scheduled at a process,
# as the computation is supposed to be CPU heavy (although in this example they
# are not).


# this generator function is sent to remote dispycos servers to run tasks there
def compute(i, n, task=None):
    import time
    yield task.sleep(n)  # computation is simulated here with sleep
    raise StopIteration((i, task.location, time.asctime()))  # result of 'compute' is current time


# -- code below is executed locally --

# client (local) task submits tasks
def client_proc(njobs, task=None):
    # schedule client with the scheduler; scheduler accepts one client
    # at a time, so if scheduler is shared, the client is queued until it
    # is done with already scheduled clients
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    # schedule tasks on dispycos servers
    rtasks = []
    for i in range(njobs):
        rtask = yield client.rtask(compute, i, random.uniform(2, 5))
        if isinstance(rtask, pycos.Task):
            rtasks.append(rtask)
        else:
            print('  ** rtask failed for %s' % i)
    # wait for results
    for rtask in rtasks:
        result = yield rtask()
        if isinstance(result, tuple) and len(result) == 3:
            print('    result for %d from %s: %s' % result)
        elif isinstance(result, pycos.MonitorStatus):
            print('    ** rtask %s failed: %s with %s' % (rtask, result.type, result.value))
        else:
            print('    ** rtask %s failed' % rtask)

    # close client
    yield client.close()


if __name__ == '__main__':
    import sys, random
    import pycos
    import pycos.netpycos
    from pycos.dispycos import *

    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # PyPI / pip packaging adjusts assertion below for Python 3.7+
    if sys.version_info.major == 3:
        assert sys.version_info.minor < 7, \
            ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
             (__file__, sys.version_info.major, sys.version_info.minor))

    # package client fragments
    client = Client([compute])
    # run 10 (or given number of) jobs
    pycos.Task(client_proc, 10 if len(sys.argv) < 2 else int(sys.argv[1]))
