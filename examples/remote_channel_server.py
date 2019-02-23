#!/usr/bin/env python

# server program where client sends messages to this server using channel
# send/receive

# run this program and then client either on same node or different node on
# local network.

import sys
import pycos
# import netpycos to use networked version of Pycos
import pycos.netpycos


def receiver_proc(task=None):
    task.set_daemon()
    # until subscribed, client's deliver will block
    yield task.sleep(5)
    # subscribe to channel to get messages
    yield channel.subscribe(task)
    print(' receiver subscribed')
    while True:
        msg = yield task.receive()
        if msg:
            print('Received "%s" from %s at %s' %
                  (msg['msg'], msg['sender'].name, msg['sender'].location))


if __name__ == '__main__':
    # pycos.logger.setLevel(pycos.logger.DEBUG)
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
            cmd = read_input('Enter "quit" or "exit" to terminate: ').strip().lower()
            if cmd in ('quit', 'exit'):
                break
        except:
            break
