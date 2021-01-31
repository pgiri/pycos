# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# Example where this client sends computation to remote dispycos process to run
# as remote tasks. Remote tasks and client use message passing to
# exchange objects (instance of class 'C'). Instead of using 'map_results' as
# done in 'dispycos_client1.py', remote task sends the result back to client
# with message passing.

# objects of C are exchanged between client and servers
class C(object):
    def __init__(self, i):
        self.i = i
        self.n = None

    def __repr__(self):
        return '%d: %s' % (self.i, self.n)


# this generator function is sent to remote dispycos servers to run tasks there
def compute(obj, result_task, task=None):
    # obj is an instance of C
    import math
    # this task and client can use message passing
    print('process at %s received: %s' % (task.location, obj.n))
    yield task.sleep(obj.n)
    obj.n = math.sqrt(obj.n)
    # send result back to client
    yield result_task.deliver(obj, timeout=5)


# -- code below is executed locally --

# local task submits computation tasks at dispycos servers
def client_proc(njobs, task=None):
    # schedule client with the scheduler; scheduler accepts one client
    # at a time, so if scheduler is shared, the client is queued until it
    # is done with already scheduled clients
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    # create a separate task to receive results, so they can be processed
    # as soon as received
    def recv_results(task=None):
        for i in range(njobs):
            result = yield task.receive()
            print('    result for job %d: %s' % (i, result))

    # remote tasks send replies as messages to this task
    results_task = pycos.Task(recv_results)

    # run njobs; each job will be executed by one dispycos server
    for i in range(njobs):
        cobj = C(i)
        cobj.n = random.uniform(5, 10)
        # as noted in 'dispycos_client2.py', 'run' method is used to run jobs
        # sequentially; use 'io_rtask' to run multiple jobs on one server
        # concurrently
        print('  request %d: %s' % (i, cobj.n))
        rtask = yield client.rtask(compute, cobj, results_task)
        if not isinstance(rtask, pycos.Task):
            print('  ** rtask failed %s: %s' % (i, rtask))

    # not required to wait for results_task (client.close will wait for results), but improves
    # clarity
    yield results_task.finish()
    # wait for all results and close client
    yield client.close()


if __name__ == '__main__':
    import random, sys
    import pycos
    import pycos.netpycos
    from pycos.dispycos import *

    # send generator function and class C (as the messages sent / received are objects of C)
    client = Client([compute, C])
    # run 10 (or given number of) jobs
    pycos.Task(client_proc, 10 if len(sys.argv) < 2 else int(sys.argv[1]))
