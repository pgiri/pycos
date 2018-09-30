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
def compute(i, n, reply, task=None):
    import time
    # computation is simulated with waiting for given number of seconds
    yield task.sleep(n)
    # send job result to 'reply' task at client
    reply.send((i, task.location, time.asctime()))
    raise StopIteration(0)

# client (local) task submits computations
def client_proc(computation, task=None):
    # schedule computation with the scheduler; if remote scheduler is not
    # automatically discovered (e.g., if it is on remote network, or UDP is
    # lossy), use 'peer' method to discover, e.g., with
    # yield pycos.Pycos.instance().peer('hostname_or_ip_of_scheduler')
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

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
        rtask = yield computation.run(compute, i, n, reply_task)
        if isinstance(rtask, pycos.Task):
            print('  Task %s created for %s at %s' % (i, n, rtask.location))

    # wait for all jobs to be done and close computation
    yield computation.close()


if __name__ == '__main__':
    import random, sys, pycos.httpd
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # in this example, dispycos scheduler is assumed to be running elsewhere on
    # the network, so unlike in other examples, scheduler is not started here

    # assume that we want to customize allocation of CPUs; in this case, we
    # leave one of the CPUs on node not used (DispycosNodeAllocate uses all
    # CPUs)
    class NodeAllocate(DispycosNodeAllocate):
        # override 'allocate' method of DispycosNodeAllocate
        def allocate(self, ip_addr, name, platform, cpus, memory, disk):
            print('Allocate %s (%s): %s' % (ip_addr, platform, cpus))
            return cpus - 1

    node_allocations = [NodeAllocate(node='*')]
    computation = Computation([compute], pulse_interval=10, zombie_period=51,
                              node_allocations=node_allocations)
    # start httpd so computation can be monitored with a browser
    http_server = pycos.httpd.HTTPServer(computation)

    client_task = pycos.Task(client_proc, computation)

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
        else:
            try:
                n = float(cmd)
            except:
                # if input is not a number, generate random number
                n = random.uniform(10, 20)
            client_task.send(n)
    # indicate end to client_proc
    client_task.send(None)
    # wait for client_proc to be done
    client_task.value()
    http_server.shutdown()
