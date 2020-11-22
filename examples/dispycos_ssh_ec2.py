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

# this generator function is sent to remote dispycos servers to run
# tasks there
def compute(i, n, task=None):
    # 'i' is job number and 'n' is seconds to suspend task (to simulate
    # computation time)
    yield task.sleep(n)
    raise StopIteration((i, n))


# -- code below is executed locally --

def client_proc(njobs, task=None):
    # schedule client with the scheduler; scheduler accepts one client
    # at a time, so if scheduler is shared, the client is queued until it
    # is done with already scheduled clients
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    # pair EC2 node with this client with:
    yield pycos.Pycos().peer(pycos.Location('54.204.242.185', 9706))
    # if multiple nodes are used in the network of 54.204.242.185, 'relay' option can be used to
    # pair with all nodes with just one statement as:
    # yield pycos.Pycos().peer(pycos.Location('54.204.242.185', 9706), relay=True)

    # schedule tasks on dispycos servers
    rtasks = []
    for i in range(njobs):
        rtask = yield client.rtask(compute, i, random.uniform(3, 10))
        if isinstance(rtask, pycos.Task):
            rtasks.append(rtask)
        else:
            print('  ** run failed for %s' % i)
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

    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    # PyPI / pip packaging adjusts assertion below for Python 3.7+
    if sys.version_info.major == 3:
        assert sys.version_info.minor < 7, \
            ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
             (__file__, sys.version_info.major, sys.version_info.minor))

    pycos.Pycos(host='127.0.0.1', tcp_port=9705, udp_port=9705)
    # send 'compute' to remote servers to run tasks when submitted
    client = Client([compute])
    njobs = 10 if len(sys.argv) == 1 else int(sys.argv[1])
    pycos.Task(client_proc, njobs)
