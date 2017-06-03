#!/usr/bin/env python

# server program where client sends messages to this server
# using channel send/receive

# run this program and then client either on same node or different
# node on local network. Server and client can also be run on two
# different networks but client must call 'scheduler.peer' method
# appropriately.

import sys, logging
import pycos.netpycos as pycos

def receiver_proc(task=None):
    task.set_daemon()
    # until subscribed, client's deliver will block
    yield task.sleep(5)
    yield channel.subscribe(task)
    pycos.logger.info(' receiver subscribed')
    while True:
        msg = yield task.receive()
        if msg:
            print('Received "%s" from %s at %s' % \
                  (msg['msg'], msg['sender'].name, msg['sender'].location))

pycos.logger.setLevel(logging.DEBUG)
pycos.logger.show_ms(True)
# scheduler = pycos.Pycos()
channel = pycos.Channel('2clients')
# register channel so client can get a reference to it
channel.register()

recv = pycos.Task(receiver_proc)
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
