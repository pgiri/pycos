#!/usr/bin/env python

# to be used with 'rps_monitor_server.py'
# client requests execution of tasks on (remote) server.

import random
import pycos
# import netpycos to use distributed version of Pycos
import pycos.netpycos


# this task gets MonitorStatus messages (notifications of exit status) for (remote) tasks created
# in rps_client (although monitor is not really necessary in this case - see below)
def monitor_proc(n, task=None):
    done = 0
    while done < n:
        msg = yield task.receive()
        if isinstance(msg, pycos.MonitorStatus):
            rtask = msg.info
            if msg.type == StopIteration:
                pycos.logger.debug('RPS %s finished with: %s', rtask, msg.value)
            else:
                pycos.logger.debug('RPS %s failed with: %s', rtask, msg.value)
            done += 1
        else:
            pycos.logger.warning('ignoring invalid message')


def rps_client(task=None):
    # if server is on remote network, automatic discovery won't work, so add it explicitly
    # yield scheduler.peer(pycos.Location('192.168.21.5', 9705))

    # get reference to RPS at server
    rps = yield pycos.RPS.locate('rps_server', timeout=5)
    if not isinstance(rps, pycos.RPS):
        print('Could not locate RPS: %s' % type(rps))
        raise StopIteration
    print('RPS is at %s' % rps.location)

    # 5 (remote) tasks are created with rps
    n = 5
    # set monitor (monitor_proc task) for tasks created for this RPS; even without monitor,
    # exceptions are shown
    rps.monitor(pycos.Task(monitor_proc, n))

    for i in range(n):
        # run task at server; calls with invalid positive number for 'b' will fail
        rtask = yield rps('test%s' % i, b=i)
        if not isinstance(rtask, pycos.Task):
            print('Could not create task for %s: %s' % (i, type(rtask)))
            continue
        pycos.logger.debug('rtask %s created', rtask)
        # messages can be exchanged with rtask; in this case, 'rps_server' expecpts to receive
        rtask.send('msg:%s' % i)
        yield task.sleep(random.uniform(0, 1))
    # it is easier to wait for each rtask with 'yield rtask()' here (without using monitor); using
    # monitor allows processing results as soon as they become available and using 'yield rtask()'
    # allows processing synchronously (potentially waiting till task is finished).


if __name__ == '__main__':
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # use 'test' secret so peers that use same secret are recognized
    scheduler = pycos.Pycos(name='client', secret='test')
    pycos.Task(rps_client)
