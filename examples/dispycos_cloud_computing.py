# Run 'dispycosnode.py' program on Amazon EC2 cloud computing and run this
# program on local computer.

#  Make sure EC2 instance allows inbound TCP port 9706 and any additional ports,
# depending on how many CPUs are used by dispycosnode. Assume '54.204.242.185'
# is external IP address of EC2 instance. Start dispycosnode on EC2 node with
# its external IP address as:

# dispycosnode.py -d --ext_ip_addr 54.204.242.185

import pycos
import pycos.netpycos
from pycos.dispycos import *


# this generator function is sent to remote dispycos servers to run
# tasks there
def compute(i, n, task=None):
    # 'i' is job number and 'n' is seconds to suspend task (to simulate
    # computation time)
    print('%s started job %s with %s' % (task.location, i, n))
    yield task.sleep(n)
    raise StopIteration((i, n))


def client_proc(computation, njobs, task=None):
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

    # establish communication with EC2 node with:
    yield pycos.Pycos().peer(pycos.Location('54.204.242.185', 9706))
    # if multiple nodes are used, 'relay' option can be used to pair with
    # all nodes with just one statement as:
    # yield pycos.Pycos().peer(pycos.Location('54.204.242.185', 9706), relay=True)

    # execute n jobs (tasks) and get their results. Note that number of
    # jobs created can be more than number of server processes available; the
    # scheduler will use as many processes as necessary/available, running one
    # job at a time at one server process
    args = [(i, random.uniform(3, 10)) for i in range(njobs)]
    results = yield computation.run_results(compute, args)
    for result in results:
        print('job %s result: %s' % (result[0], result[1]))

    yield computation.close()


if __name__ == '__main__':
    import sys, random
    # enable debug initially
    pycos.logger.setLevel(pycos.Logger.DEBUG)

    config = {} # add any additional parameters

    # if client is behind a router, configure router's firewall to forward port
    # 9705 to client's IP address and use router's external IP address (i.e.,
    # addressable from outside world) 
    config['ext_ip_addr'] = 'router.ext.ip'
    pycos.Pycos(**config)

    njobs = 4 if len(sys.argv) == 1 else int(sys.argv[1])
    # if scheduler is not already running (on a node as a program),
    # start private scheduler:
    Scheduler()
    # use 'compute' for computation jobs
    computation = Computation([compute])
    pycos.Task(client_proc, computation, njobs)
