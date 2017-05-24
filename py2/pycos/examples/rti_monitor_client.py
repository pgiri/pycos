#!/usr/bin/env python

# to be used with 'rti_monitor_server.py'
# client requests execution of tasks on (remote) server.

import sys, random
# import netpycos to use distributed version of Pycos
import pycos.netpycos as pycos

def monitor_proc(n, task=None):
    # this task gets exceptions from (remote) tasks created in task1
    done = 0
    while True:
        msg = yield task.receive()
        if isinstance(msg, pycos.MonitorException):
            rtask = msg.args[0]
            value_type, value = msg.args[1]
            if value_type == StopIteration:
                pycos.logger.debug('RTI %s finished with: %s', rtask, value)
            else:
                pycos.logger.debug('RTI %s failed with: %s', rtask, value.args[0])
            done += 1
            if done == n:
                break
        else:
            pycos.logger.warning('ignoring invalid message')

def rti_test(task=None):
    # if server is on remote network, automatic discovery won't work,
    # so add it explicitly
    # yield scheduler.peer('192.168.21.5')

    # find where 'rti_1' is registered (with given name in any
    # known peer)
    rti1 = yield pycos.RTI.locate('rti_1')
    print('RTI is at %s' % rti1.location)
    # alternately, location can be explicitly created with
    # pycos.Location or obtained with 'locate' of pycos etc.
    loc = yield scheduler.locate('server')
    rti1 = yield pycos.RTI.locate('rti_1', loc)
    print('RTI is at %s' % rti1.location)

    n = 5
    monitor = pycos.Task(monitor_proc, n)
    for x in range(n):
        rtask = yield rti1('test%s' % x, b=x)
        pycos.logger.debug('RTI %s created' % rtask)
        # set 'monitor' as monitor for this task
        yield monitor.monitor(rtask)
        # send a message
        rtask.send('msg:%s' % x)
        yield task.sleep(random.uniform(0, 1))

pycos.logger.setLevel(pycos.Logger.DEBUG)
scheduler = pycos.Pycos(name='client', secret='test')
pycos.Task(rti_test)
