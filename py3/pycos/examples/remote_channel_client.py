#!/usr/bin/env python

# example where client and server communicate through a channel.
# use with its server 'remote_channel_server.py'

import sys, logging, random
import pycos.netpycos as pycos

def sender_proc(rchannel, task=None):
    # send messages to channel; 'deliver' is used with n=2, so
    # messages will not be sent until both receivers subscribe to
    # channel
    for x in range(10):
        msg = {'msg': 'message %s' % x, 'sender': task}
        n = yield rchannel.deliver(msg, n=2, timeout=10)
        pycos.logger.info('  Delivered to: %s' % n)
        yield task.sleep(random.uniform(0, 0.5))
    rchannel.send({'msg': None, 'sender': task})

def receiver_proc2(task=None):
    # if server is in remote network, add it explicitly
    # yield scheduler.peer('remote.ip')
    # yield scheduler.peer(pycos.Location('remote.ip', port))
    rchannel = yield pycos.Channel.locate('2clients')
    # this task subscribes to the channel, so gets messages to server
    # channel
    pycos.logger.debug('server is at %s' % rchannel.location)
    if (yield rchannel.subscribe(task)) != 0:
        raise Exception('subscription failed')
    sender = pycos.Task(sender_proc, rchannel)
    while True:
        msg = yield task.receive()
        pycos.logger.debug('Received "%s" from %s at %s',
                           msg['msg'], msg['sender'].name, msg['sender'].location)
        if msg['msg'] is None and msg['sender'] == sender:
            break
    yield rchannel.unsubscribe(task)

pycos.logger.setLevel(logging.DEBUG)
pycos.logger.show_ms(True)
scheduler = pycos.Pycos()
pycos.Task(receiver_proc2)
