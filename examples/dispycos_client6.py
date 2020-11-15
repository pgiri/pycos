# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This example uses status messages and message passing to run two remote tasks
# at two different servers to process streaming data for live/real-time
# analysis. This example requires numpy module available on servers. See
# dispycos_client6_channel.py that uses channels for communication and 'deque'
# for circular buffers (instead of numpy).

# This generator function is sent to remote dispycos to analyze data and
# generate apprporiate signals that are sent to a task running on client. The
# signal in this simple case is average of moving window of given size is below
# or above a threshold.
def rtask_avg_proc(threshold, trend_task, window_size, task=None):
    # numpy module is loaded in node_setup, so no need to load here
    global numpy  # no need to declare, but clarifies
    data = numpy.empty(window_size, dtype=float)
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
        data = numpy.roll(data, -1)
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


# -- code below is executed locally --

# This task runs on client. It gets trend messages from remote task that
# computes moving window average.
def trend_proc(task=None):
    task.set_daemon()
    while True:
        trend = yield task.receive()
        print('trend signal at % 4d: %s / %.2f' % (trend[0], trend[1], trend[2]))


# this task is executed on a node to prepare for client. In this case we
# want to make sure node has 'numpy' module used in rtask_avg_proc
def node_setup(task=None):
    # load numpy in global scope so it can be used in servers
    global numpy
    try:
        import numpy
    except Exception:
        # non-zero "return" indicates failure and this node won't be used
        ret = yield -1
    else:
        ret = yield 0
    raise StopIteration(ret)


# This task runs locally. It creates two remote tasks at two dispycosnode server
# processes, two local tasks, one to receive trend signal from one of the remote
# tasks, and another to send data to two remote tasks
def client_proc(task=None):
    # schedule client with the scheduler; scheduler accepts one client
    # at a time, so if scheduler is shared, the client is queued until it
    # is done with already scheduled clients
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    trend_task = pycos.Task(trend_proc)

    # run average and save tasks at two different servers
    rtask_avg = yield client.rtask(rtask_avg_proc, 0.4, trend_task, 10)
    assert isinstance(rtask_avg, pycos.Task)
    rtask_save = yield client.rtask(rtask_save_proc)
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

    # 'node_setup' is used to restrict to nodes that have 'numpy' module
    # available. However, 'node_setup' doesn't work with Windows, so this
    # example disables Windows nodes; alternately 'server_setup' (that works for
    # Windows nodes as well) can be used instead of 'node_setup'.
    nodes = [DispycosNodeAllocate(node='*', platform='Windows', cpus=0)]
    client = Client([], nodes=nodes, node_setup=node_setup)
    pycos.Task(client_proc)
