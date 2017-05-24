# server program for processing requests received with message passing
# (asynchronous concurrent programming) from remote client
# (tut_client.py) on same network;
# see http://pycos.sourceforge.net/tutorial.html for details.

import sys
import pycos.netpycos as pycos

def server_proc(task=None):
    task.set_daemon()
    task.register('server_task')
    while True:
        msg = yield task.receive()
        print('processing %s' % (msg))

pycos.logger.setLevel(pycos.Logger.DEBUG)
scheduler = pycos.Pycos(udp_port=0)
server = pycos.Task(server_proc)
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
