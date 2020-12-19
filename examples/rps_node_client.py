#!/usr/bin/env python

# to be used with 'rps_node_server.py'; client sends executable programs for execution on
# remote server using RPS feature.

import sys
import os
import hashlib
import random

# import netpycos to use networking
import pycos.netpycos


def client_proc(script, task=None):
    # if server is on remote network, automatic discovery won't work; or if Wifi is used or in
    # case of heavy traffic, discovery may not work due to loss of UDP packets, so add it
    # explicitly

    # yield scheduler.peer('192.168.21.5')

    # find where 'rps' is registered (with given name in any known peer)
    rps = yield pycos.RPS.locate('node_update_rps')
    # alternately, location can be explicitly created with pycos.Location or obtained with
    # 'locate' of pycos etc.
    # loc = yield scheduler.locate('node_update_server')
    # rps = yield pycos.RPS.locate('rps', loc)
    print('RPS server is at %s' % rps.location)

    sent = yield pycos.Pycos.instance().send_file(rps.location, script, overwrite=True, timeout=5)
    if sent < 0:
        print('Could not send file %s: %s' % (script, sent))
        raise StopIteration

    server = yield rps(task, script, random.uniform(2, 5))
    if not isinstance(server, pycos.Task):
        print('Could not get server task: %s' % type(server))
        raise StopIteration
    req = yield task.recv(timeout=10)
    if not req:
        raise StopIteration
    auth.update(req)
    server.send(auth.hexdigest())

    outerr = yield task.recv()
    if outerr[0]:
        print('\nscript output: %s\n' % outerr[0])
    if outerr[1]:
        print('\nscript error: %s\n' % outerr[1])
    ret = yield server()
    if ret:
        print('\n  ** script failed with %s' % ret)


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
