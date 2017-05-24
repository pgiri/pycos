#!/usr/bin/env python

# client sends messages to a remote task
# use with its server 'remote_task_server.py'

import sys
# import netpycos to use distributed version of Pycos
import pycos.netpycos as pycos

def sender(task=None):
    # if server is in remote network, add it; set 'stream_send' to
    # True for streaming messages to it
    # yield scheduler.peer('remote.peer.ip', stream_send=True)
    rtask = yield pycos.Task.locate('server_task')
    print('server is at %s' % rtask.location)
    for x in range(10):
        rtask.send('message %s' % x)

pycos.logger.setLevel(pycos.Logger.DEBUG)
# scheduler = pycos.Pycos(secret='key')
pycos.Task(sender)
