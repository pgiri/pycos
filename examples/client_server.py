#!/usr/bin/env python

# client and server tasks communicating with message passing
# (asynchronous concurrent programming);
# see https://pycos.sourceforge.io/pycos.html for details.

import random
import pycos


def server_proc(task=None):
    task.set_daemon()
    while True:
        msg = yield task.receive()
        print('Received %s' % (msg))


def client_proc(server, n, task=None):
    global msg_id
    for i in range(3):
        yield task.suspend(random.uniform(0.5, 3))
        # although multiple clients execute this method, locking is not
        # necessary, as a task not preempted (unlike Python threads) and runs
        # till 'yield'
        msg_id += 1
        server.send('msg_id %d: client %d, msg %d' % (msg_id, n, i))


msg_id = 0
# create server
server = pycos.Task(server_proc)
# create 10 clients
for i in range(10):
    pycos.Task(client_proc, server, i)
