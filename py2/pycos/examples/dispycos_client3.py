# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# Example where this client sends computation to remote dispycos process to run
# as remote tasks. Remote tasks and client use message passing to
# exchange objects (instance of class 'C'). Instead of using 'map_results' as
# done in 'dispycos_client1.py', remote task sends the result back to client
# with message passing.

import pycos.netpycos as pycos
from pycos.dispycos import *


# objects of C are exchanged between client and servers
class C(object):
    def __init__(self, i):
        self.i = i
        self.n = None

    def __repr__(self):
        return '%d: %s' % (self.i, self.n)


# this generator function is sent to remote dispycos servers to run tasks
# there
def compute(obj, client, task=None):
    # obj is an instance of C
    import math
    # this task and client can use message passing
    print('process at %s received: %s' % (task.location, obj.n))
    yield task.sleep(obj.n)
    obj.n = math.sqrt(obj.n)
    # send result back to client
    yield client.deliver(obj, timeout=5)


def client_proc(computation, njobs, task=None):
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

    # create a separate task to receive results, so they can be processed
    # as soon as received
    def recv_results(task=None):
        for i in range(njobs):
            msg = yield task.receive()
            print('    result for job %d: %s' % (i, msg))

    # remote tasks send replies as messages to this task
    results_task = pycos.Task(recv_results)

    # run njobs; each job will be executed by one dispycos server
    for i in range(njobs):
        cobj = C(i)
        cobj.n = random.uniform(5, 10)
        # as noted in 'dispycos_client2.py', 'run' method is used to run jobs
        # sequentially; use 'run_async' to run multiple jobs on one server
        # concurrently
        print('  request %d: %s' % (i, cobj.n))
        rtask = yield computation.run(compute, cobj, results_task)
        if not isinstance(rtask, pycos.Task):
            print('failed to create rtask %s: %s' % (i, rtask))

    # wait for all results and close computation
    yield computation.close()


if __name__ == '__main__':
    import random, sys
    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program),
    # start it (private scheduler):
    Scheduler()
    # send generator function and class C (as the computation uses
    # objects of C)
    computation = Computation([compute, C])
    # run 10 (or given number of) jobs
    pycos.Task(client_proc, computation, 10 if len(sys.argv) < 2 else int(sys.argv[1])).value()
