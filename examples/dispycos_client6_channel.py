# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This example is similar to 'dispycos_client6.py', except it uses broadcasting
# over Channel to send messages to remote tasks to process, and uses
# 'deque' module to implement circular buffer.

import pycos.netpycos as pycos
from pycos.dispycos import *


# This generator function is sent to remote dispycos to analyze data
# and generate apprporiate signals that are sent to a task
# running on client. The signal in this simple case is average of
# moving window of given size is below or above a threshold.
def rtask_avg_proc(channel, threshold, trend_task, window_size, task=None):
    import collections
    # subscribe to channel (at client)
    yield channel.subscribe(task)
    # create circular buffer
    data = collections.deque(maxlen=window_size)
    for i in range(window_size):
        data.append(0.0)
    cumsum = 0.0
    # first message is 'start' command; see 'client_proc'
    assert (yield task.receive()) == 'start'
    while True:
        i, n = yield task.receive()
        if n is None:
            break
        cumsum += (n - data[0])
        avg = (cumsum / window_size)
        if avg > threshold:
            trend_task.send((i, 'high', float(avg)))
        elif avg < -threshold:
            trend_task.send((i, 'low', float(avg)))
        data.append(n)
    raise StopIteration(0)


# This generator function is sent to remote dispycos process to save
# the received data in a file (on the remote peer).
def rtask_save_proc(channel, task=None):
    import os
    import tempfile
    # subscribe to channel (at client)
    yield channel.subscribe(task)
    # first message is 'start' command (to make sure all recipients started)
    assert (yield task.receive()) == 'start'
    # save data in 'tickdata' where computation files are saved (this will be
    # deleted when computation is done, so it must be copied elsewhere if
    # necessary)
    with open('tickdata', 'w') as fd:
        while True:
            i, n = yield task.receive()
            if n is None:
                break
            fd.write('%s: %s\n' % (i, n))
    raise StopIteration(0)


# This task runs on client. It gets trend messages from remote
# task that computes moving window average.
def trend_proc(task=None):
    task.set_daemon()
    while True:
        trend = yield task.receive()
        print('trend signal at % 4d: %s / %.2f' % (trend[0], trend[1], trend[2]))


# This process runs locally. It creates two remote tasks at two dispycosnode
# server processes, two local tasks, one to receive trend signal from one
# of the remote tasks, and another to send data to two remote tasks
def client_proc(computation, task=None):
    # schedule computation with the scheduler; scheduler accepts one computation
    # at a time, so if scheduler is shared, the computation is queued until it
    # is done with already scheduled computations
    if (yield computation.schedule()):
        raise Exception('Could not schedule computation')

    # in dispycos_client6.py, data is sent to each remote task; here, data
    # is broadcast over channel and remote tasks subscribe to it
    data_channel = pycos.Channel('data_channel')
    # not necessary to register channel in this case, as it is sent to remote
    # tasks; if they were to 'locate' it, it should be registered
    # data_channel.register()

    trend_task = pycos.Task(trend_proc)

    rtask_avg = yield computation.run(rtask_avg_proc, data_channel, 0.4, trend_task, 10)
    assert isinstance(rtask_avg, pycos.Task)
    rtask_save = yield computation.run(rtask_save_proc, data_channel)
    assert isinstance(rtask_save, pycos.Task)

    # make sure both remote tasks have subscribed to channel ('deliver'
    # should return 2 if they both are)
    assert (yield data_channel.deliver('start', n=2)) == 2

    # if data is sent frequently (say, many times a second), enable
    # streaming data to remote peer; this is more efficient as
    # connections are kept open (so the cost of opening and closing
    # connections is avoided), but keeping too many connections open
    # consumes system resources
    yield pycos.Pycos.instance().peer(rtask_avg.location, stream_send=True)
    yield pycos.Pycos.instance().peer(rtask_save.location, stream_send=True)

    # send 1000 items of random data to remote tasks
    for i in range(1000):
        n = random.uniform(-1, 1)
        item = (i, n)
        # data can be sent to remote tasks either with 'send' or
        # 'deliver'; 'send' is more efficient but no guarantee data
        # has been sent successfully whereas 'deliver' indicates
        # errors right away
        data_channel.send(item)
        yield task.sleep(0.02)
    item = (i, None)
    data_channel.send(item)

    yield computation.close()
    data_channel.close()


if __name__ == '__main__':
    import random
    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    # if scheduler is shared (i.e., running as program), nothing needs
    # to be done (its location can optionally be given to 'schedule');
    # othrwise, start private scheduler:
    Scheduler()
    computation = Computation([])
    pycos.Task(client_proc, computation)
