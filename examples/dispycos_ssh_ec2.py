# Run 'dispycosnode.py' program on Amazon EC2 cloud computing and run this
# program on local computer.

# In this example ssh is used for port forwarding.  Make sure EC2 instance
# allows inbound TCP port 9706 and any additional ports, depending on how many
# CPUs are used by dispycosnode. Assume '54.204.242.185' is external IP address
# of EC2 instance. Login to that node with remote port forwarding:
# 'ssh -R # 9705:127.0.0.1:9705 54.204.242.185' (so port 9705, used by client is
# forwarded), then run dispycosnode on EC2 server at port (starting with) 9706 with:
# 'dispycosnode.py -d --ext_ip_addr 54.204.242.185 --tcp_ports 9706'

# Distributed computing example where this client sends computation to remote
# dispycos process to run as remote tasks. At any time at most one
# computation task is scheduled at a process (due to
# RemoteTaskScheduler). This example shows how to use 'execute' method of
# RemoteTaskScheduler to run comutations and get their results easily.

# This example can be combined with in-memory processing (see
# 'dispycos_client9_node.py') and streaming (see 'dispycos_client6.py') for
# efficient processing of data and communication.

import pycos
import pycos.netpycos
from pycos.dispycos import *


# this generator function is sent to remote dispycos servers to run
# tasks there
def compute(i, n, task=None):
    # 'i' is job number and 'n' is seconds to suspend task (to simulate
    # computation time)
    yield task.sleep(n)
    raise StopIteration((i, n))


def client_proc(computation, njobs, task=None):
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

    # pair EC2 node with this client with:
    yield pycos.Pycos().peer(pycos.Location('54.204.242.185', 9706))
    # if multiple nodes are used, 'relay' option can be used to pair with
    # all nodes with just one statement as:
    # yield pycos.Pycos().peer(pycos.Location('54.204.242.185', 9706), relay=True)

    # execute n jobs (tasks) and get their results. Note that number of
    # jobs created can be more than number of server processes available; the
    # scheduler will use as many processes as necessary/available, running one
    # job at a server process
    args = [(i, random.uniform(3, 10)) for i in range(njobs)]
    results = yield computation.run_results(compute, args)
    for result in results:
        print('job %s result: %s' % (result[0], result[1]))

    yield computation.close()


if __name__ == '__main__':
    import sys, random
    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    pycos.Pycos(node='127.0.0.1', tcp_port=9705, udp_port=9705)
    njobs = 10 if len(sys.argv) == 1 else int(sys.argv[1])
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    Scheduler()
    # use 'compute' for computation jobs
    computation = Computation([compute])
    pycos.Task(client_proc, computation, njobs)
