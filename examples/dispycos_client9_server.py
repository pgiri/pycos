# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This example illustrates in-memory processing with 'server_available' to read
# date in to memory by each (remote) server process. Remote tasks ('compute' in
# this case) then process data in memory. This example works with POSIX (Linux,
# OS X etc.) and Windows. Note that, as data is read in to each server process,
# a node may have multiple copies of same data in memory of each process on that
# node, so this approach is not practical / efficient when data is large. See
# 'dispycos_client9_node.py' which uses 'node_available' and 'node_setup' to
# read data in to memory at node (and thus only one copy is in memory).

# In this example different files are sent to remote servers to compute checksum
# of their data (thus there is no duplicate data in servers at a node in this
# case).

import pycos
import pycos.netpycos
from pycos.dispycos import *


def server_available(location, data_file, task=None):
    # 'server_available' is executed locally (at client) when a server process
    # is available. 'location' is Location instance of server. When this task is
    # executed, 'depends' of client would've been transferred.  data_file
    # could've been sent with the client 'depends'; however, to illustrate
    # how files can be sent separately (to distribute data fragments among
    # servers), files are transferred to servers in this example

    print('  Sending %s to %s' % (data_file, location))
    if (yield pycos.Pycos().send_file(location, data_file, timeout=5, overwrite=True)) < 0:
        print('Could not send data file "%s" to %s' % (data_file, location))
        raise StopIteration(-1)

    # 'setup_server' is executed on remote server at 'location' with argument
    # data_file
    yield client.enable_server(location, os.path.basename(data_file))
    raise StopIteration(0)


# 'setup_server' is executed at remote server process to read the data in given
# file (transferred by client) in to memory (global variable). 'compute' then
# uses the data in memory instead of reading from file every time.
def setup_server(data_file, task=None):  # executed on remote server
    # variables declared as 'global' will be available in tasks for read/write
    # to all tasks on a server.
    global hashlib, data, file_name
    import os, hashlib
    file_name = data_file
    print('%s processing %s' % (task.location, data_file))
    # note that files transferred to server are in the directory where
    # tasks are executed (cf 'node_setup' in dispycos_client9_node.py)
    with open(data_file, 'rb') as fd:
        data = fd.read()
    os.remove(data_file)  # data_file is not needed anymore
    # generator functions must have at least one 'yield'
    ret = yield 0  # indicate successful initialization with exit value 0
    raise StopIteration(ret)


# 'compute' is executed at remote server process repeatedly to compute checksum
# of data in memory, initialized by 'setup_server'
def compute(alg, n, task=None):
    global data, hashlib, file_name
    yield task.sleep(n)
    checksum = getattr(hashlib, alg)()
    checksum.update(data)
    raise StopIteration((file_name, alg, checksum.hexdigest()))


# local task to process status messages from scheduler
def status_proc(task=None):
    task.set_daemon()
    i = 0
    while 1:
        msg = yield task.receive()
        if not isinstance(msg, DispycosStatus):
            continue
        if msg.status == Scheduler.ServerDiscovered:
            pycos.Task(server_available, msg.info, data_files[i])
            i += 1
            if i >= len(data_files):
                i = 0


def client_proc(client, task=None):
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    # execute 10 jobs (tasks) and get their results. Note that number of jobs
    # created can be more than number of server processes available; the
    # scheduler will use as many processes as necessary/available, running one
    # job at a server process
    algorithms = ['md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512']
    args = [(algorithms[i % len(algorithms)], random.uniform(5, 10)) for i in range(15)]
    results = yield client.run_results(compute, args)
    for i, result in enumerate(results):
        if isinstance(result, tuple) and len(result) == 3:
            print('    %ssum for %s: %s' % (result[1], result[0], result[2]))
        else:
            print('  rtask failed for %s: %s' % (args[i][0], str(result)))

    yield client.close()


if __name__ == '__main__':
    import sys, os, random, glob
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # PyPI / pip packaging adjusts assertion below for Python 3.7+
    if sys.version_info.major == 3:
        assert sys.version_info.minor < 7, \
            ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
             (__file__, sys.version_info.major, sys.version_info.minor))

    # optional first argument must be a directory containing Python files
    if len(sys.argv) > 1 and os.path.isdir(sys.argv[1]):
        data_files = glob.glob(os.path.join(sys.argv[1], '*.py'))
    else:
        # use files in 'examples' directory
        data_files = glob.glob(os.path.join(os.path.dirname(pycos.__file__), 'examples', '*.py'))

    # if scheduler is not already running (on a node as a program), start
    # private scheduler:
    Scheduler()

    # send 'compute' generator function; the client sends data files when server
    # is discovered (to illustrate how client can distribute data).
    client = Client([compute], status_task=pycos.Task(status_proc),
                    disable_servers=True, server_setup=setup_server)
    pycos.Task(client_proc, client)
