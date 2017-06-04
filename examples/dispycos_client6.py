# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This example uses status messages and message passing to run two remote tasks
# at two different servers to process streaming data for live/real-time
# analysis. This example requires numpy module available on servers. See
# dispycos_client6_channel.py that uses channels for communication and 'deque'
# for circular buffers (instead of numpy).

import pycos.netpycos as pycos
from pycos.dispycos import *


# This generator function is sent to remote dispycos to analyze data and
# generate apprporiate signals that are sent to a task running on client. The
# signal in this simple case is average of moving window of given size is below
# or above a threshold.
def rtask_avg_proc(threshold, trend_task, window_size, task=None):
    import numpy as np
    data = np.empty(window_size, dtype=float)
    data.fill(0.0)
    cumsum = 0.0
    while True:
        i, n = yield task.receive()
        if n is None:
            break
        cumsum += (n - data[0])
        avg = cumsum / window_size
        if avg > threshold:
            trend_task.send((i, 'high', float(avg)))
        elif avg < -threshold:
            trend_task.send((i, 'low', float(avg)))
        data = np.roll(data, -1)
        data[-1] = n
    raise StopIteration(0)


# This generator function is sent to remote dispycos process to save the
# received data in a file (on the remote peer).
def rtask_save_proc(task=None):
    import os
    import tempfile
    # save data in /tmp/tickdata
    with open(os.path.join(os.sep, tempfile.gettempdir(), 'tickdata'), 'w') as fd:
        while True:
            i, n = yield task.receive()
            if n is None:
                break
            fd.write('%s: %s\n' % (i, n))
    raise StopIteration(0)


# This task runs on client. It gets trend messages from remote task that
# computes moving window average.
def trend_proc(task=None):
    task.set_daemon()
    while True:
        trend = yield task.receive()
        print('trend signal at % 4d: %s / %.2f' % (trend[0], trend[1], trend[2]))


# this task is executed on a node to prepare for computation. In this case we
# want to make sure node has 'numpy' module used in rtask_avg_proc
def node_setup(task=None):
    try:
        import numpy
    except:
        # non-zero "return" indicates failure and this node won't be used
        yield -1
    else:
        yield 0


# This task runs locally. It creates two remote tasks at two dispycosnode server
# processes, two local tasks, one to receive trend signal from one of the remote
# tasks, and another to send data to two remote tasks
def client_proc(computation, task=None):
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

    trend_task = pycos.Task(trend_proc)

    # run average and save tasks at two different servers
    rtask_avg = yield computation.run(rtask_avg_proc, 0.4, trend_task, 10)
    assert isinstance(rtask_avg, pycos.Task)
    rtask_save = yield computation.run(rtask_save_proc)
    assert isinstance(rtask_save, pycos.Task)

    # if data is sent frequently (say, many times a second), enable streaming
    # data to remote peer; this is more efficient as connections are kept open
    # (so the cost of opening and closing connections is avoided), but keeping
    # too many connections open consumes system resources
    yield pycos.Pycos.instance().peer(rtask_avg.location, stream_send=True)
    yield pycos.Pycos.instance().peer(rtask_save.location, stream_send=True)

    # send 1000 items of random data to remote tasks
    for i in range(1000):
        n = random.uniform(-1, 1)
        item = (i, n)
        # data can be sent to remote tasks either with 'send' or 'deliver';
        # 'send' is more efficient but no guarantee data has been sent
        # successfully whereas 'deliver' indicates errors right away;
        # alternately, messages can be sent with a channel, which is more
        # convenient if there are multiple (unknown) recipients
        rtask_avg.send(item)
        rtask_save.send(item)
        yield task.sleep(0.01)
    item = (i, None)
    rtask_avg.send(item)
    rtask_save.send(item)

    yield computation.close()


if __name__ == '__main__':
    import random
    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    # if scheduler is shared (i.e., running as program), nothing needs to be
    # done (its location can optionally be given to 'schedule'); othrwise, start
    # private scheduler:
    Scheduler()
    # 'node_setup' is used to restrict to nodes that have 'numpy' module
    # available. However, 'node_setup' doesn't work with Windows, so this
    # example disables Windows nodes; alternately 'server_setup' (that works for
    # Windows nodes as well) can be used instead of 'node_setup'.
    node_allocations = [DispycosNodeAllocate(node='*', platform='Windows', cpus=0)]
    computation = Computation([], node_allocations=node_allocations, node_setup=node_setup)
    pycos.Task(client_proc, computation)
