#!/usr/bin/env python

# client sends messages to a remote task
# use with its server 'remote_task_msg_server.py'

import sys, random
# import netpycos to use distributed version of Pycos
import pycos.netpycos as pycos

def sender(i, server_task, task=None):
    msg = '%d: ' % i + '-' * random.randint(100,300) + '/'
    yield server_task.send(msg)

def create_senders(n, task=None):
    # if server is in remote network, add it; set 'stream_send' to
    # True for streaming messages to it
    # yield scheduler.peer('remote.peer.ip', stream_send=True)
    server_task = yield pycos.Task.locate('server_task')
    print('server is at %s' % server_task.location)
    for i in range(n):
        pycos.Task(sender, i, server_task)

pycos.logger.setLevel(pycos.Logger.DEBUG)
# scheduler = pycos.Pycos(secret='key')
pycos.Task(create_senders, 10 if len(sys.argv) < 2 else int(sys.argv[1]))
