# client program for sending requests to remote server (tut_server.py)
# using message passing (asynchronous concurrent programming);
# see http://pycos.sourceforge.net/tutorial.html for details.

import sys, random
import pycos.netpycos as pycos

def client_proc(n, task=None):
    global msg_id
    server = yield pycos.Task.locate('server_task')
    for x in range(3):
        # yield task.suspend(random.uniform(0.5, 3))
        msg_id += 1
        server.send('%d: %d / %d' % (msg_id, n, x))
    # yield task.sleep(1)

msg_id = 0
pycos.logger.setLevel(pycos.Logger.DEBUG)
scheduler = pycos.Pycos(udp_port=0)
# create 10 clients; each client sends 3 messages
for i in range(4):
    pycos.Task(client_proc, i)
