# Run 'dispycosnode.py' program to start processes to execute computations sent by this client,
# along with this program.

# In this example different files are sent to remote servers to compute checksums of file data.

# 'compute' is executed at remote server process repeatedly to compute checksums of data in given
# file; results are sent to 'reply_task' (which is at client)
def compute(data_file, reply_task, task=None):
    import hashlib, random
    # load file into memory; this may not be suitable for large files, as it is likely all servers
    # on a node will also simultaneously hold data of different files in memory.
    with open(data_file, 'rb') as fd:
        data = fd.read()
    algs = getattr(hashlib, 'algorithms_guaranteed', None)
    if not algs:
        algs = hashlib.algorithms

    for alg in algs:
        try:
            csum = getattr(hashlib, alg)()
            csum.update(data)
            csum = csum.hexdigest()
        except Exception:  # some algorithms may fail with this simple method
            continue
        yield task.sleep(random.uniform(2, 5))  # to simulate computing in this example
        reply_task.send((data_file, alg, csum))
    reply_task.send(None)  # send 'None' to indicate this file is done
    os.remove(data_file)
    # 'dispycos_close_server' will cause server process to terminate. The server can be restarted
    # with 'restart=True' in this call or in using 'restart_servers=True' parameter to Client, as
    # done below. Then a server is restarted (with new process) automatically so new server
    # instance can be used.
    dispycos_close_server()


# -- code below is executed locally --

# 'use_server' is executed locally (at client) when a server process is available. 'location'
# is Location instance of server. 'data_file' is sent to this server to run 'compute' on this
# file. Thus each server (running on a CPU core / processor) processes different file.
def use_server(location, data_file, client, task=None):
    file_name = os.path.basename(data_file)
    pycos.logger.info('  Sending %s to %s' % (file_name, location))
    if (yield pycos.Pycos().send_file(location, data_file, timeout=5, overwrite=True)) < 0:
        print('Could not send data file "%s" to %s' % (data_file, location))
        raise StopIteration(-1)

    def recv_results(task=None):  # this task receives results from 'compute' on this server
        while 1:
            msg = yield task.recv()
            if isinstance(msg, tuple) and len(msg) == 3:  # check result is as expected
                fname, alg, csum = msg
                # assert fname == file_name
                pycos.logger.info('%ssum for "%s": %s', alg, fname, csum)
            elif not msg:  # end of computation
                break

    reply_task = pycos.Task(recv_results)
    yield client.rtask_at(location, compute, file_name, reply_task)
    yield reply_task()


# 'client_proc' is executed locally (at client) to schedule computation and to receive status
# messages from scheduler. When ServerInitialized message is received, it starts a new task with
# 'use_server' to use that server to process a file.
def client_proc(task=None):
    # this task receives status messages from scheduler which are processed below;
    client.status_task = task
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    # in this example it is not needed to keep track of server tasks, but may be useful in
    # other cases, e.g., to reschedule a file on failure of server
    server_tasks = {}
    i = 0
    while i < len(data_files):
        msg = yield task.receive()
        if not isinstance(msg, DispycosStatus):
            continue
        if msg.status == Scheduler.ServerInitialized:  # use this server
            server_tasks[msg.info] = pycos.Task(use_server, msg.info, data_files[i], client)
            i += 1
        elif msg.status == Scheduler.ServerClosed:
            server_tasks.pop(msg.info, None)

    for server_task in server_tasks.values():  # wait for results for data files
        yield server_task()
    yield client.close()


if __name__ == '__main__':
    import sys, os, glob
    import pycos
    import pycos.netpycos
    from pycos.dispycos import *

    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # PyPI / pip packaging adjusts assertion below for Python 3.7+
    if sys.version_info.major == 3:
        assert sys.version_info.minor < 7, \
            ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
             (__file__, sys.version_info.major, sys.version_info.minor))

    n = 0
    path = os.path.join(os.path.dirname(pycos.__file__), 'examples')
    if len(sys.argv) > 1:
        if os.path.isdir(sys.argv[1]):
            path = sys.argv[1]
            if len(sys.argv) > 2:
                n = int(sys.argv[2])
        else:
            n = int(sys.argv[1])  # assume n files will be processed

    data_files = glob.glob(os.path.join(path, '*.py'))
    if n:
        data_files = data_files[:n]

    # send 'compute' generator function;
    # 'restart_servers=True' causes servers to be started automatically when a server is closed;
    client = Client([compute], restart_servers=True)
    pycos.Task(client_proc)
