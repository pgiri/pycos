# Run 'dispycosnode.py' program on Amazon EC2 cloud computing and run this
# program on local computer.

#  Make sure EC2 instance allows inbound TCP port 9706 and any additional ports,
# depending on how many CPUs are used by servers (e.g., if maximum CPUs in a
# server is 8, allow ports 9706 to 9714). For better protection, allow
# connection on these ports only from client IP address, or even use SSL. Start
# dispycosnode on EC2 node with its external IP address; e.g., on an EC2 node
# with external IP address '54.204.242.185', start dispycosnode as:

# dispycosnode.py -d --ext_ip_addr 54.204.242.185

# this generator function is sent to remote dispycos servers to run
# tasks there
def compute(i, n, task=None):
    # 'i' is job number and 'n' is seconds to suspend task (to simulate
    # computation time)
    print('%s started job %s with %s' % (task.location, i, n))
    yield task.sleep(n)
    raise StopIteration((i, n))


# -- code below is executed locally --

def client_proc(njobs, task=None):
    # schedule client with the scheduler; scheduler accepts one client
    # at a time, so if scheduler is shared, the client is queued until it
    # is done with already scheduled clients
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    # establish communication with EC2 node with:
    yield pycos.Pycos().peer(pycos.Location('54.204.242.185', 9706))
    # if multiple nodes are used, 'relay' option can be used to pair with
    # all nodes with just one statement as:
    # yield pycos.Pycos().peer(pycos.Location('54.204.242.185', 9706), relay=True)

    # schedule tasks on dispycos servers
    rtasks = []
    for i in range(njobs):
        rtask = yield client.rtask(compute, i, random.uniform(3, 10))
        if isinstance(rtask, pycos.Task):
            rtasks.append(rtask)
        else:
            print('  ** rtask failed for %s' % i)
    # wait for results
    for rtask in rtasks:
        result = yield rtask()
        if isinstance(result, tuple) and len(result) == 2:
            print('    result from %s: %s' % (rtask.location, str(result)))
        else:
            print('  ** rtask %s failed' % rtask)

    yield client.close()


if __name__ == '__main__':
    import sys, random
    import pycos
    import pycos.netpycos
    from pycos.dispycos import *

    # enable debug to see progress
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # PyPI / pip packaging adjusts assertion below for Python 3.7+
    if sys.version_info.major == 3:
        assert sys.version_info.minor < 7, \
            ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
             (__file__, sys.version_info.major, sys.version_info.minor))

    config = {}  # add any additional parameters

    # if client is behind a router, configure router's firewall to forward port
    # 9705 to client's IP address and use router's external IP address (i.e.,
    # addressable from outside world)
    config['ext_ip_addr'] = 'router.ext.ip'
    pycos.Pycos(**config)

    # send 'compute' to dispycos servers to run tasks when jobs are scheduled
    client = Client([compute])
    njobs = 4 if len(sys.argv) == 1 else int(sys.argv[1])
    pycos.Task(client_proc, njobs)
