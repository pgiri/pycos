# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This example illustrates in-memory processing with 'node_setup' to read date
# in to memory. Remote tasks ('compute' in this case) then process data in
# memory. This example works with POSIX (Linux, OS X etc.), but not Windows. See
# 'dispycos_client9_server.py' which initializes each server to read data in to
# memory for processing in computations.

# 'compute' is executed at remote server process repeatedly to compute checksum
# of data in memory, initialized by 'node_setup'
def compute(alg, n, task=None):
    # note that files sent in 'node_available' would've been at parent of working directory, so to
    # access 'data_file' above (if not removed in 'node_setup' as done in this case), it can do so
    # with, e.g., as "open(os.path.join('..', data_file), 'rb')"
    global data, hashlib, file_name
    yield task.sleep(n)
    checksum = getattr(hashlib, alg)()
    checksum.update(data)
    raise StopIteration((file_name, alg, checksum.hexdigest()))


def node_setup(data_file, task=None):
    # 'node_setup' is executed on a node with the arguments returned by
    # 'node_available'. This task should return 0 to indicate successful
    # initialization.

    # variables declared as 'global' will be available (as read-only) in tasks.
    global os, hashlib, data, file_name
    import os, hashlib
    # note that files transferred to node are in parent directory of cwd where
    # each client is run (in case such files need to be accessed in
    # computation).
    file_name = data_file
    print('data_file: "%s"' % data_file)
    with open(data_file, 'rb') as fd:
        data = fd.read()
    os.remove(data_file)  # data_file is not needed anymore
    ret = yield 0  # task must have at least one 'yield' and 0 indicates success
    raise StopIteration(ret)


# -- code below is executed locally --

# 'node_available' is executed locally (at client) when a node is
# available. 'location' is Location instance of node. When this task is
# executed, 'depends' of client would've been transferred.
def node_available(avail_info, data_file, task=None):
    # data_file could've been sent with the client 'depends'; however, to
    # illustrate how files can be sent separately (e.g., to transfer different
    # files to different nodes), file is transferred with 'node_available'.

    print('  Sending %s to %s' % (data_file, avail_info.location.addr))
    sent = yield pycos.Pycos().send_file(avail_info.location, data_file,
                                         overwrite=True, timeout=5)
    if (sent < 0):
        print('Could not send data file "%s" to %s' % (data_file, avail_info.location))
        raise StopIteration(-1)

    # node_setup will be executed at dispynode with data_file as argument
    ret = yield client.enable_node(avail_info.location.addr, os.path.basename(data_file))
    raise StopIteration(ret)


# local task to process status messages from scheduler
def status_proc(task=None):
    task.set_daemon()
    i = 0
    while 1:
        msg = yield task.receive()
        if not isinstance(msg, DispycosStatus):
            continue
        if msg.status == Scheduler.NodeDiscovered:
            if i >= len(data_files):
                i = 0
            pycos.Task(node_available, msg.info.avail_info, data_files[i])
            i += 1


def client_proc(task=None):
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    # execute 10 jobs (tasks) and get their results. Note that number of jobs
    # created can be more than number of server processes available; the
    # scheduler will use as many processes as necessary/available, running one
    # job at a server process
    algorithms = ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512']
    rtasks = []
    for i in range(15):
        alg = algorithms[i % len(algorithms)]
        rtask = yield client.rtask(compute, alg, random.uniform(1, 3))
        if isinstance(rtask, pycos.Task):
            rtasks.append(rtask)
        else:
            pycos.logger.warning('  ** rtask failed for %s', alg)
    # wait for results
    for rtask in rtasks:
        result = yield rtask()
        if isinstance(result, tuple) and len(result) == 3:
            print('   %ssum for %s: %s' % (result[1], result[0], result[2]))

    yield client.close()


if __name__ == '__main__':
    import sys, os, random, glob
    import pycos
    import pycos.netpycos
    from pycos.dispycos import *

    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # PyPI / pip packaging adjusts assertion below for Python 3.7+
    if sys.version_info.major == 3:
        assert sys.version_info.minor < 7, \
            ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
             (__file__, sys.version_info.major, sys.version_info.minor))

    # use files in 'examples' directory
    data_files = glob.glob(os.path.join(os.path.dirname(pycos.__file__), 'examples', '*.py'))
    # optional argument must be integer indicating number of files to process
    if len(sys.argv) > 1:
        data_files = data_files[:min(len(data_files), int(sys.argv[1]))]

    # optional first argument must be a directory containing Python files
    if len(sys.argv) > 1 and os.path.isdir(sys.argv[1]):
        data_files = glob.glob(os.path.join(sys.argv[1], '*.py'))

    # Since this example doesn't work with Windows, 'nodes' feature is used to filter out nodes
    # running Windows.
    nodes = [DispycosNodeAllocate(node='*', platform='Windows', cpus=0)]
    client = Client([compute], nodes=nodes, node_setup=node_setup, disable_nodes=True,
                    status_task=pycos.Task(status_proc))
    pycos.Task(client_proc)
