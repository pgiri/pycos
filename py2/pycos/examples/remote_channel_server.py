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
    # until subscribed, 'deliver' in client will block
    msg = yield task.recv()
    yield channel.subscribe(task)
    print('receiver subbed: %s' % msg)
    while True:
        msg = yield task.receive()
        if msg:
            print('Received "%s" from %s at %s' % \
                  (msg['msg'], msg['sender'].name, msg['sender'].location))

def doctor(task=None):
    task.register()
    task.set_daemon()
    while 1:
        n = yield task.recv()
        print('pulse: %s' % n)

pycos.logger.setLevel(logging.DEBUG)
pycos.logger.show_ms(True)
# scheduler = pycos.Pycos()
channel = pycos.Channel('2clients')
# register channel so client can get a reference to it
channel.register()

recv = pycos.Task(receiver_proc)
pycos.Task(doctor)
if sys.version_info.major > 2:
    read_input = input
else:
    read_input = raw_input
while True:
    try:
        cmd = read_input().strip().lower()
        if cmd in ('quit', 'exit'):
            break
        if cmd.startswith('go'):
            recv.send(cmd)
    except:
        break
