# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# Distributed computing example where this client sends computation to remote
# dispycos process to run as remote tasks. At any time at most one computation
# task is scheduled at a process, as the computation is supposed to be CPU heavy
# (although in this example they are not). Status of remote tasks can be
# monitored in a web browser at http://127.0.0.1:8181


# this function is sent to remote dispycos servers to run as tasks.
def compute(n, task=None):
    yield task.sleep(n)  # computation is simluated here by suspending task


# -- code below is executed locally --

# this function runs locally to schedule remote tasks (with 'compute' function above).
def client_proc(njobs, task=None):
    # use httpd so computation progress can be monitored in a web browser
    # this is useful in identifying potential problems / evaluate performance of algorithms etc.
    httpd = pycos.httpd.HTTPServer(client)
    # cluster and progress of computations can be viewed in a web browser at this client's
    # network address and port 8181 (e.g., 'http://127.0.0.1:8181' at client)

    # schedule client with the scheduler; scheduler accepts one client
    # at a time, so if scheduler is shared, the client is queued until it
    # is done with already scheduled clients
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    # schedule tasks on remote dispycos servers
    for i in range(njobs):
        # computation is supposed to be CPU bound so 'rtask' is used so at most
        # one computations runs at a server at any time; for mostly idle
        # computations, 'io_rtask' may be used to run more than one task at a
        # server at the same time.
        rtask = yield client.rtask(compute, random.uniform(5, 10))
        if isinstance(rtask, pycos.Task):
            print('  job %s processed by %s' % (i, rtask.location))
        else:
            print('  ** rtask %s failed: %s' % (i, rtask))

    # wait for all jobs to be done and close client
    yield client.close()
    # shutdown httpd after client is closed
    httpd.shutdown()


if __name__ == '__main__':
    import random, sys, pycos.dispycos
    import pycos
    import pycos.netpycos
    from pycos.dispycos import *
    import pycos.httpd

    # pycos.logger.setLevel(pycos.Logger.DEBUG)  # enable it to see more details

    # send 'compute' generator function to run tasks at dispycos servers
    client = Client([compute])

    # run 10 (or given number of) jobs
    pycos.Task(client_proc, 10 if len(sys.argv) < 2 else int(sys.argv[1]))
