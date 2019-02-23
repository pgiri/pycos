# client program for sending requests to remote server (msg_server.py) using
# message passing (asynchronous concurrent programming); see
# https://pycos.sourceforge.io for details.

import random
import pycos
# use netycos to start message passing with remote peers
import pycos.netpycos


def client_proc(n, task=None):
    global msg_id
    # get reference to remote server task
    server = yield pycos.Task.locate('server_task')
    for x in range(3):
        yield task.suspend(random.uniform(0.5, 2))
        # although multiple clients execute this method, locking is not
        # necessary, as a task not preempted (unlike Python threads) and runs
        # till 'yield'
        msg_id += 1
        server.send('msg_id %d: client %d, msg %d' % (msg_id, n, x))


# pycos.logger.setLevel(pycos.Logger.DEBUG)
msg_id = 0
# create 10 clients; each client sends 3 messages
for i in range(10):
    pycos.Task(client_proc, i)
