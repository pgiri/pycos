# Run 'dispycosnode.py' program to start processes to execute tasks sent
# by this client, along with this program.

# Distributed computing example where this client sends computations ('compute'
# function) to remote dispycos servers to run as remote tasks and obtain
# results. At any time at most one computation task is scheduled at a process,
# as the computation is supposed to be CPU heavy (although in this example they
# are not).

# this generator function is sent to remote dispycos servers to run tasks there
def compute(i, n, reply, task=None):
    import time
    # computation is simulated with waiting for given number of seconds
    yield task.sleep(n)
    # send job result to 'reply' task at client
    reply.send((i, task.location, time.asctime()))
    raise StopIteration(0)


# -- code below is executed locally --

# client (local) task submits computations
def client_proc(client, task=None):
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    # remote tasks send results to this process
    def reply_proc(task=None):
        task.set_daemon()
        while 1:
            msg = yield task.recv()
            print('      Received reply for %s from %s: %s' % (msg[0], msg[1], msg[2]))

    reply_task = pycos.Task(reply_proc)

    i = 0
    while True:
        n = yield task.receive()
        if n is None:
            break
        i += 1
        rtask = yield client.rtask(compute, i, n, reply_task)
        if isinstance(rtask, pycos.Task):
            print('  Task %s created for %s at %s' % (i, n, rtask.location))

    # wait for all jobs to be done and close client
    yield client.close()


if __name__ == '__main__':
    import sys, random, pycos.httpd
    import pycos
    import pycos.netpycos
    from pycos.dispycos import *

    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # PyPI / pip packaging adjusts assertion below for Python 3.7+
    if sys.version_info.major == 3:
        assert sys.version_info.minor < 7, \
            ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
             (__file__, sys.version_info.major, sys.version_info.minor))

    # in this example, dispycos scheduler is assumed to be running elsewhere on
    # the network, so unlike in other examples, scheduler is not started here

    # assume that we want to customize allocation of CPUs; in this case, we
    # leave one of the CPUs on node not used (default DispycosNodeAllocate uses all
    # CPUs)
    class NodeAllocate(DispycosNodeAllocate):
        # override 'allocate' method of DispycosNodeAllocate
        def allocate(self, ip_addr, name, platform, cpus, memory, disk):
            print('Allocate %s (%s): %s' % (ip_addr, platform, cpus))
            return cpus - 1

    nodes = [NodeAllocate(node='*')]
    # unlike in other example, here dispycos scheduler is assumed to be running on host 'foreman';
    # using a remote scheduler is useful when multiple clients can use nodes so each client
    # can use all available nodes exclusively and clients are scheduled one after another, using
    # nodes very effectively in a shared environment
    client = Client([compute], pulse_interval=10, zombie_period=51, nodes=nodes,
                    scheduler='foreman')
    # start httpd so client can be monitored with a browser
    http_server = pycos.httpd.HTTPServer(client)

    client_task = pycos.Task(client_proc, client)

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
        except Exception:
            break
        else:
            try:
                n = float(cmd)
            except Exception:
                # if input is not a number, generate random number
                n = random.uniform(10, 20)
            client_task.send(n)
    # indicate end to client_proc
    client_task.send(None)
    # wait for client_proc to be done
    client_task.value()
    http_server.shutdown()
