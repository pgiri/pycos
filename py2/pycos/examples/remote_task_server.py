#!/usr/bin/env python

# server program where client sends messages to this
# server using task send/receive

# run this program and then client either on same node or different
# node on local network. Server and client can also be run on two
# different networks but client must call 'scheduler.peer' method
# appropriately.

import sys
# import netpycos to use distributed version of Pycos
import pycos.netpycos as pycos

def receiver(task=None):
    task.set_daemon()
    task.register('server_task')
    while True:
        msg = yield task.receive()
        print('Received %s' % msg)

pycos.logger.setLevel(pycos.Logger.DEBUG)
# call with 'udp_port=0' to start network services
# scheduler = pycos.Pycos(secret='key')

pycos.Task(receiver)
if sys.version_info.major > 2:
    read_input = input
else:
    read_input = raw_input
while True:
    try:
        cmd = read_input().strip().lower()
        if cmd in ('quit', 'exit'):
            break
    except:
        break

