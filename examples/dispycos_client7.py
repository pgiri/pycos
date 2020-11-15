# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This program is similar to 'dispycos_client1.py', but instead of using 'run' (which is simpler),
# it uses status message notifications from dispycos scheduler to run jobs at specific remote
# dispycos servers. This template can be used to implement cusomized scheduler to run remote
# tasks.

# this generator function is sent to remote server to run tasks there
def rtask_proc(n, task=None):
    yield task.sleep(n)
    raise StopIteration(n)


# -- code below is executed locally --

# Instead of using client's 'run' method to schedule tasks at any available server (which is
# easier), in this example status messages from dispycos scheduler are used to start remote tasks
# at specific servers and get their results
def client_proc(njobs, task=None):
    # set status_task to receive status messages from scheduler
    client.status_task = task
    # schedule client with the scheduler; scheduler accepts one client
    # at a time, so if scheduler is shared, the client is queued until it
    # is done with already scheduled clients wait for jobs to be created
    # and finished
    if (yield client.schedule()):
        raise Exception('Failed to schedule client')

    npending = njobs

    # in this example at most one task is submitted at a server; depending on
    # client / needs, many tasks can be simlutaneously submitted / running
    # at a server (with 'client.io_rtask').
    while True:
        msg = yield task.receive()  # from dispycos scheduler
        if isinstance(msg, DispycosStatus):
            # print('Status: %s / %s' % (msg.info, msg.status))
            if msg.status == Scheduler.ServerInitialized and njobs > 0:  # run a job
                n = random.uniform(5, 10)
                rtask = yield client.rtask_at(msg.info, rtask_proc, n)
                if isinstance(rtask, pycos.Task):
                    print('  rtask_proc started at %s with %s' % (rtask.location, n))
                    njobs -= 1
        elif isinstance(msg, pycos.MonitorStatus):
            # previously submitted remote task finished
            rtask = msg.info
            if msg.type == StopIteration:  # exit status type
                print('      rtask_proc at %s finished with %s' % (rtask.location, msg.value))
            else:
                print('      ** rtask_proc at %s failed: %s / %s' %
                      (rtask.location, msg.type, msg.value))

            npending -= 1
            if npending == 0:
                break
            if njobs > 0:  # run another job
                n = random.uniform(5, 10)
                rtask = yield client.rtask_at(rtask.location, rtask_proc, n)
                if isinstance(rtask, pycos.Task):
                    print('  rtask_proc started at %s with %s' % (rtask.location, n))
                    njobs -= 1

    yield client.close()


if __name__ == '__main__':
    import sys, random
    import pycos
    import pycos.netpycos
    from pycos.dispycos import *

    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    # PyPI / pip packaging adjusts assertion below for Python 3.7+
    if sys.version_info.major == 3:
        assert sys.version_info.minor < 7, \
            ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
             (__file__, sys.version_info.major, sys.version_info.minor))

    njobs = 10 if len(sys.argv) < 2 else int(sys.argv[1])
    client = Client([rtask_proc])
    pycos.Task(client_proc, njobs)
