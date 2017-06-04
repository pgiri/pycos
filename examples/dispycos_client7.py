# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This program is similar to 'dispycos_client1.py', but instead of using
# 'run_result' (which is simpler), it uses status message notifications from
# dispycos scheduler to run jobs at specific remote dispycos servers. This
# template can be used to implement cusomized scheduler to run remote tasks.

import pycos.netpycos as pycos
from pycos.dispycos import *

# this generator function is sent to remote server to run tasks there
def rtask_proc(n, task=None):
    yield task.sleep(n)
    raise StopIteration(n)


# Instead of using computation's 'run_result' method to get results (which is
# easier), in this example status messages from dispycos scheduler are used to
# start remote tasks and get their results
def status_proc(computation, njobs, task=None):
    # set computation's status_task to receive status messages from dispycos
    # scheduler (this should be done before httpd is created, in case it is
    # used).
    computation.status_task = task
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations wait for jobs to be created
    # and finished
    if (yield computation.schedule()):
        raise Exception('Failed to schedule computation')

    npending = njobs

    # in this example at most one task is submitted at a server; depending on
    # computation / needs, many tasks can be simlutaneously submitted / running
    # at a server (with 'computation.run_async').
    while True:
        msg = yield task.receive()
        if isinstance(msg, DispycosStatus):
            # print('Status: %s / %s' % (msg.info, msg.status))
            if msg.status == Scheduler.ServerInitialized and njobs > 0: # run a job
                n = random.uniform(5, 10)
                rtask = yield computation.run_at(msg.info, rtask_proc, n)
                if isinstance(rtask, pycos.Task):
                    print('  rtask_proc started at %s with %s' % (rtask.location, n))
                    njobs -= 1
        elif isinstance(msg, pycos.MonitorException):
            # previously submitted remote task finished
            rtask = msg.args[0]
            if msg.args[1][0] == StopIteration: # exit status type
                print('      rtask_proc at %s finished with %s' % (rtask.location, msg.args[1][1]))
            else:
                print('      rtask_proc at %s failed: %s / %s' %
                      (rtask.location, msg.args[1][0], msg.args[1][1]))

            npending -= 1
            if npending == 0:
                break
            if njobs > 0: # run another job
                n = random.uniform(5, 10)
                rtask = yield computation.run_at(rtask.location, rtask_proc, n)
                if isinstance(rtask, pycos.Task):
                    print('  rtask_proc started at %s with %s' % (rtask.location, n))
                    njobs -= 1

    yield computation.close()


if __name__ == '__main__':
    import random, sys
    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    # if scheduler is not already running (on a node as a program), start it
    # (private scheduler):
    Scheduler()
    njobs = 10 if len(sys.argv) < 2 else int(sys.argv[1])
    computation = Computation([rtask_proc])
    pycos.Task(status_proc, computation, njobs)
