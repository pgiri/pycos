# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# In this example different files are sent to remote servers to compute checksum
# of their data.

import pycos.netpycos as pycos
from pycos.dispycos import *

# 'compute' is executed at remote server process to compute checksum of data for given file
def compute(data_file, alg, n, client_task, task=None):
    import hashlib
    checksum = getattr(hashlib, alg)()
    with open(data_file, 'rb') as fd:
        while 1:
            data = fd.read(8192)
            if data:
                checksum.update(data)
            else:
                break
    yield task.sleep(n)
    # send result to client
    client_task.send((data_file, alg, checksum.hexdigest()))
    # 'dispycos_close_server' will cause server process to terminate. The server can be restarted
    # with 'restart=True' in this call or in using 'restart_servers=True' parameter to
    # Client. Then a server is restarted (with new process) automatically so new computation
    # can be sent to it.
    dispycos_close_server(restart=True)


# 'server_available' is executed at client to send a data file to server that became available and
# run task at that server
def server_available(location, data_file, task=None):
    if (yield pycos.Pycos().send_file(location, data_file, timeout=5, overwrite=True)) < 0:
        print('Could not send data file "%s" to %s' % (data_file, location))
        raise StopIteration(-1)

    data_file = os.path.basename(data_file)
    pycos.logger.info('Running %s at %s' % (data_file, location))
    reply = yield client.run_at(location, compute, data_file, 'sha512', random.uniform(4, 5),
                                client_task)
    raise StopIteration(reply)


# local task to process status messages from scheduler
def status_proc(task=None):
    task.set_daemon()
    i = 0
    while 1:
        msg = yield task.receive()
        if isinstance(msg, DispycosStatus):
            # various status messages; in this case only ServerInitialized is useful
            if msg.status == Scheduler.ServerInitialized and i < len(data_files):
                # send data file and start computation in a separate task for concurrent
                # processing, useful in case many servers are available
                pycos.Task(server_available, msg.info, data_files[i])
                i += 1

def client_proc(client, task=None):
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    # receive results from servers
    for i in range(len(data_files)):
        result = yield task.recv()
        print('    %ssum for %s: %s' % (result[1], result[0], result[2]))

    yield client.close()


if __name__ == '__main__':
    import random, sys, os, glob
    pycos.logger.setLevel(pycos.Logger.DEBUG)

    # optional first argument must be a directory containing Python files
    if len(sys.argv) > 1 and os.path.isdir(sys.argv[1]):
        data_files = glob.glob(os.path.join(sys.argv[1], '*.py'))
    else:
        # use files in 'examples' directory
        data_files = glob.glob(os.path.join(os.path.dirname(pycos.__file__), 'examples', '*.py'))

    # if scheduler is not already running (on a node as a program), start private scheduler:
    Scheduler()

    # send 'compute' generator function; the client sends data files when server is discovered (to
    # illustrate how client can distribute data).

    # servers can be restarted with 'restart_servers=True' here, or with 'restart=True' to
    # 'dispycos_close_server'
    # client = Client([compute], status_task=pycos.Task(status_proc), restart_servers=True)
    client = Client([compute], status_task=pycos.Task(status_proc))
    client_task = pycos.Task(client_proc, client)
