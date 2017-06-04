# server program for processing requests received with message passing
# (asynchronous concurrent programming) from remote client (msg_client.py) on
# same network; see https://pycos.sourceforge.io for details.

import sys, random
# use netycos to start message passing with remote peers
import pycos.netpycos as pycos

# task to process a message from client
def process(msg, task=None):
    print('processing %s' % (msg))
    yield task.sleep(random.uniform(0, 1))
    print('  done with %s' % (msg))

# task receives messages from client and creates tasks to process each message
def server_task(task=None):
    task.set_daemon()
    task.register('server_task')
    while True:
        msg = yield task.receive()
        # create task to process message
        pycos.Task(process, msg)

# pycos.logger.setLevel(pycos.Logger.DEBUG)
# create server task
server = pycos.Task(server_task)

if sys.version_info.major > 2:
    read_input = input
else:
    read_input = raw_input
while True:
    try:
        cmd = read_input('Enter "quit" or "exit" to terminate: ').strip().lower()
        if cmd in ('quit', 'exit'):
            break
    except:
        break
