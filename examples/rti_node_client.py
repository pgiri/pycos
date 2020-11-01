#!/usr/bin/env python

# to be used with 'rti_node_server.py'; client sends executable programs for execution on
# remote server using RTI feature.

import sys
import os
import hashlib
import random

# import netpycos to use networking
import pycos.netpycos


def monitor_proc(client, task=None):
    # this task gets exceptions from (remote) tasks created in client;
    # this is useful, e.g., in case remote task fails unexpectedly
    while True:
        msg = yield task.receive()
        if isinstance(msg, pycos.MonitorStatus):
            rtask = msg.info
            if msg.type == StopIteration and msg.value == 0:
                pycos.logger.debug('RTI %s finished with: %s', rtask, msg.value)
            else:
                pycos.logger.debug('RTI %s failed with: %s', rtask, msg.value)
                # deal with failure; in this case, an exception is thrown to client for
                # illustration
                client.throw(Exception(msg.value))
            break
        else:
            pycos.logger.warning('ignoring invalid message')


def client_proc(script, task=None):
    # if server is on remote network, automatic discovery won't work; or if Wifi is used or in
    # case of heavy traffic, discovery may not work due to loss of UDP packets, so add it
    # explicitly

    # yield scheduler.peer('192.168.21.5')

    # find where 'rti' is registered (with given name in any known peer)
    rti = yield pycos.RTI.locate('node_update_rti')
    # alternately, location can be explicitly created with pycos.Location or obtained with
    # 'locate' of pycos etc.
    # loc = yield scheduler.locate('node_update_server')
    # rti = yield pycos.RTI.locate('rti', loc)
    print('RTI server is at %s' % rti.location)

    sent = yield pycos.Pycos.instance().send_file(rti.location, script, overwrite=True, timeout=5)
    if sent < 0:
        print('Could not send file %s: %s' % (script, sent))
        raise StopIteration

    # it is simpler to have server send error directly to this client task, but a monitor is
    # used here to illustrate potential use cases
    monitor = pycos.Task(monitor_proc, task)
    yield rti.monitor(monitor)
    server = yield rti(task, script, random.uniform(2 ,5))
    req = yield task.recv(timeout=10)
    if not req:
        monitor.terminate()
        raise StopIteration
    auth.update(req)
    server.send(auth.hexdigest())

    outerr = yield task.recv()
    if outerr[0]:
        print('\nscript output: %s\n' % outerr[0])
    if outerr[1]:
        print('\nscript error: %s\n' % outerr[1])


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        script = sys.argv[1]
    else:
        script = os.path.join(os.path.dirname(sys.argv[0]), 'node_update_script.py')

    assert os.path.isfile(script)
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input
    auth = hashlib.sha512(read_input('Enter authentication string: ').strip().encode())

    # if this script is in shared environment, secret itself can be read from input
    scheduler = pycos.Pycos(name='client', secret='update')

    pycos.Task(client_proc, script)
