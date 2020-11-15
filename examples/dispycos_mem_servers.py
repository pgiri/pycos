# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# In this example, pycos, netpycos and dispycos's features are used to illustrate how each CPU in
# each node can be treated as independent, addressable computational unit with its own
# memory. With message passing, client and servers can communicate one-to-one in full-duplex or
# one-to-many (with channels). In addition, each server can run one or more computations with
# which client can communicate independently. These computations can share memory that can be
# updated as required.

# This example illustrates in-memory processing with 'init_mem' to read date in to memory by each
# (remote) server process. Remote tasks ('compute' in this case) then process data in memory. Note
# that, as data is read in each server process, a node should have enough memory to load data by
# each server (one per CPU).  If files are large, this approach is not practical / efficient. In
# practice, it may be required to use information in 'DispycosNodeInfo' to find how much memory is
# available at a node to use this approach.

# 'init_mem' is executed at remote server process to store data into memory so future tasks can
# use data in memory. This could've been done in 'compute' itself (without using 'init_mem'); here
# separate task is used to illustrate how tasks can share memory (e.g., to keep
# intermediate/partial results).
def init_mem(data_file, task=None):
    # prepare server by reading data to global variable 'data' and import 'hashlib' module in
    # global scope (so all computation tasks can use it without having to import in each task)
    global data, hashlib
    import os, hashlib
    # files transferred to server are in the directory where tasks are executed
    data_file = os.path.basename(data_file)
    with open(data_file, 'rb') as fd:
        data = fd.read()
    os.remove(data_file)  # data_file is not needed anymore
    result = yield 0  # generator function must have at least one 'yield'
    raise StopIteration(result)


# 'compute' is executed at remote server process to compute checksum of data in memory
# (initialized by 'init_mem'); in this example all servers run 'compute', but if necessary,
# different servers can run different functions or same server can run multiple functions etc.
def compute(reply_task, task=None):
    global data, hashlib  # use variables (in memory) setup in 'init_mem'
    import traceback
    while 1:
        msg = yield task.recv()
        if isinstance(msg, tuple) and len(msg) == 2:
            alg, n = msg
            csum = getattr(hashlib, alg)()
            csum.update(data)
            yield task.sleep(n)  # simulate computation
            try:
                result = csum.hexdigest()
            except Exception:
                # if alg is 'shake_*', hexdigest requires digest size argument, so above will fail;
                # instead of using it correctly, error message is sent
                result = traceback.format_exc()
            reply_task.send(result)
        elif msg is None:  # end of requests
            break
        else:
            reply_task.send('invalid message %s ignored' % msg)
    # close and restart server so new instance can be used to process another file; closing a
    # server discards all global memory used by that server
    dispycos_close_server(restart=True)
    raise StopIteration(0)


# -- code below is executed locally --

# this local function is executed when a server is available; this function runs 'init_mem'
# on that server first and then 'compute' to process data in memory
def use_server(client, location, data_file, task=None):
    pycos.logger.info('  sending %s to %s' % (data_file, location))
    if (yield pycos.Pycos().send_file(location, data_file, timeout=5, overwrite=True)) < 0:
        print('Could not send data file "%s" to %s' % (data_file, location))
        raise StopIteration(-1)

    data_file = os.path.basename(data_file)
    rtask = yield client.rtask(init_mem, data_file)
    if not isinstance(rtask, pycos.Task):
        raise StopIteration(-1)

    result = yield rtask()
    if isinstance(result, int) and result == 0:
        # start compute task at this server
        rtask = yield client.rtask(compute, task)
        if not isinstance(rtask, pycos.Task):
            raise StopIteration
        algorithms = list(hashlib.algorithms_guaranteed)
        # send 5 requests to compute (on in-memory data); here message passing is used for
        # exchanghing requests and replies
        for j in range(5):
            alg = random.choice(algorithms)
            rtask.send((alg, random.uniform(2, 5)))
            result = yield task.recv()
            if isinstance(result, str):
                pycos.logger.info('  server at %s computed %s for %s as "%s"',
                                  location, alg, data_file, result)
            else:
                pycos.logger.warning('  server at %s failed for %s with %s: %s',
                                     location, data_file, alg, result)
        rtask.send(None)  # indicate to compute that requests are done so it quits
        yield rtask()
    else:
        pycos.logger.warning('init_mem failed: %s', result)
    pycos.logger.debug('%s done', data_file)
    raise StopIteration(0)


def client_proc(data_files, task=None):
    client.status_task = task
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    i = 0
    while 1:
        msg = yield task.receive()  # status messages from scheduler
        if not isinstance(msg, DispycosStatus):
            continue
        if msg.status == Scheduler.ServerInitialized:
            # keep track of data files submitted for processing so in case of faults, files can be
            # submitted again to different servers
            data_file = data_files[i]
            servers[msg.info] = {'task': pycos.Task(use_server, client, msg.info, data_file),
                                 'file': data_file}
            i += 1
            if i >= len(data_files):
                break

    for server in servers.values():
        yield server['task']()
    yield client.close()


if __name__ == '__main__':
    import sys, os, random, glob, hashlib
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
    # optional first argument is number of files to process
    if len(sys.argv) > 1:
        data_files = data_files[:min(len(data_files), int(sys.argv[1]))]

    client = Client([init_mem, compute])
    servers = {}  # keep track of servers processing files (used in 'client_proc' and 'use_server')
    pycos.Task(client_proc, data_files)
