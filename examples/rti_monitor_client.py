#!/usr/bin/env python

# to be used with 'rti_monitor_server.py'
# client requests execution of tasks on (remote) server.

import random
import pycos
# import netpycos to use distributed version of Pycos
import pycos.netpycos


# this task gets MonitorStatus messages (notifications of exit status) for (remote) tasks created
# in rti_client
def monitor_proc(n, task=None):
    done = 0
    while done < n:
        msg = yield task.receive()
        if isinstance(msg, pycos.MonitorStatus):
            rtask = msg.info
            if msg.type == StopIteration:
                pycos.logger.debug('RTI %s finished with: %s', rtask, msg.value)
            else:
                pycos.logger.debug('RTI %s failed with: %s', rtask, msg.value)
            done += 1
        else:
            pycos.logger.warning('ignoring invalid message')


def rti_client(task=None):
    # if server is on remote network, automatic discovery won't work, so add it explicitly
    # yield scheduler.peer(pycos.Location('192.168.21.5', 9705))

    # get reference to RTI at server
    rti = yield pycos.RTI.locate('rti_server')
    print('RTI is at %s' % rti.location)

    # 5 (remote) tasks are created with rti
    n = 5
    # set monitor (monitor_proc task) for tasks created for this RTI; even without monitor,
    # exceptions are shown
    rti.monitor(pycos.Task(monitor_proc, n))

    for i in range(n):
        # run task at server; tasks with odd number for 'b' will fail
        rtask = yield rti('test%s' % i, b=i)
        pycos.logger.debug('rtask %s created', rtask)
        # messages can be exchanged with rtask; in this case, 'rti_server' expecpts to receive
        rtask.send('msg:%s' % i)
        yield task.sleep(random.uniform(0, 1))


if __name__ == '__main__':
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # use 'test' secret so peers that use same secret are recognized
    scheduler = pycos.Pycos(name='client', secret='test')
    pycos.Task(rti_client)
