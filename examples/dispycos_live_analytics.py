# Run 'dispycosnode.py' program on nodes to start processes to execute computations sent by this
# client, along with this program.

# This example shows how to stream data to different remote tasks (a case of MISD - multiple
# instruction single data) to compute live analytics on data and send interesting signals
# (information in live data) back to client.

# This generator function is sent to remote dispycos to analyze data and generate apprporiate
# signals that are sent to a task running on client. The signal in this simple case is average of
# moving window of given size is below or above a threshold.
def rtask_avg_proc(threshold, trend_task, window_size, task=None):
    data = [0.0] * window_size
    cumsum = 0.0
    i = 0
    while True:
        pos, num = yield task.receive()
        if num is None:
            break
        cumsum += (num - data[i])
        if pos >= window_size:
            avg = (cumsum / window_size)
            if avg > threshold:
                trend_task.send((pos, 'high', float(avg)))
            elif avg < -threshold:
                trend_task.send((pos, 'low', float(avg)))
        data[i] = num
        i += 1
        if i == window_size:
            i = 0
    raise StopIteration(0)


# This generator function is sent to remote dispycos process to save the received data in a file
# (on the remote peer).
def rtask_save_proc(task=None):
    # save data in 'tickdata' where computation files are saved (this will be deleted when client
    # is done, so it must be saved elsewhere if necessary)
    with open('tickdata', 'w') as fd:
        while True:
            i, n = yield task.receive()
            if n is None:
                break
            fd.write('%s: %s\n' % (i, n))
    raise StopIteration(0)


# -- code below is executed locally --

# This task runs at client. It gets trend messages from rtask_avg.
def trend_proc(task=None):
    task.set_daemon()
    while True:
        trend = yield task.receive()
        print('trend signal at % 4d: %s / %.2f' % (trend[0], trend[1], trend[2]))


# This task runs locally. It sends computations to dispycos servers, creates two (remote) tasks on
# them, sends data to those tasks.
def client_proc(task=None):
    # schedule client with the scheduler; scheduler accepts one client at a time, so if scheduler
    # is shared, the client is queued until it is done with already scheduled clients
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    trend_task = pycos.Task(trend_proc)

    # run average and save tasks at two different servers
    rtask_avg = yield client.rtask(rtask_avg_proc, 0.4, trend_task, 10)
    assert isinstance(rtask_avg, pycos.Task)
    rtask_save = yield client.rtask(rtask_save_proc)
    assert isinstance(rtask_save, pycos.Task)

    # if data is sent frequently (say, many times a second), enable streaming data to remote peer;
    # this is more efficient as connections are kept open (so the cost of opening and closing
    # connections is avoided), but keeping too many connections open consumes system resources
    yield pycos.Pycos.instance().peer(rtask_avg.location, stream_send=True)
    yield pycos.Pycos.instance().peer(rtask_save.location, stream_send=True)

    # send 1000 items of random data to remote tasks
    for pos in range(1000):
        num = random.uniform(-1, 1)
        item = (pos, num)
        # data can be sent to remote tasks either with 'send' or 'deliver'; 'send' is more
        # efficient but no guarantee data has been sent successfully whereas 'deliver' indicates
        # errors right away
        if (rtask_avg.send(item) or rtask_save.send(item)):
            # TODO: deal with failure to send
            break
        yield task.sleep(0.02)
    item = (pos, None)
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

    # functions are not sent with 'depends' since two different rtasks are created with
    # two different functions
    client = Client([])
    pycos.Task(client_proc)
