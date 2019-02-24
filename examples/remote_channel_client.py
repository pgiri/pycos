#!/usr/bin/env python

# example where client and server communicate through a channel.
# use with its server 'remote_channel_server.py'

import sys, random
import pycos
# import netpycos to use networked version of Pycos
import pycos.netpycos


def sender_proc(rchannel, task=None):
    # send messages to channel; 'deliver' is used with n=2, so messages will not
    # be sent until both receivers subscribe to channel
    for x in range(10):
        msg = {'msg': 'message %s' % x, 'sender': task}
        n = yield rchannel.deliver(msg, n=2, timeout=10)
        print('  Delivered to: %s' % n)
        yield task.sleep(random.uniform(0, 0.5))
    rchannel.send({'msg': None, 'sender': task})


def receiver_proc2(task=None):
    # if server is in remote network, add it explicitly
    # scheduler = pycos.Pycos.instance()
    # yield scheduler.peer(pycos.Location('remote.ip', tcp_port))
    rchannel = yield pycos.Channel.locate('2clients', timeout=5)
    if not rchannel:
        print('Could not locate server!')
        raise StopIteration
    # this task subscribes to the channel to get messages to server channel
    print('server is at %s' % rchannel.location)
    if (yield rchannel.subscribe(task)) != 0:
        raise Exception('subscription failed')
    sender = pycos.Task(sender_proc, rchannel)
    while True:
        msg = yield task.receive()
        print('Received "%s" from %s at %s' %
              (msg['msg'], msg['sender'].name, msg['sender'].location))
        if msg['msg'] is None and msg['sender'] == sender:
            break
    yield rchannel.unsubscribe(task)


if __name__ == '__main__':
    # pycos.logger.setLevel(pycos.logger.DEBUG)
    # PyPI / pip packaging adjusts assertion below for Python 3.7+
    if sys.version_info.major == 3:
        assert sys.version_info.minor < 7, \
            ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
             (__file__, sys.version_info.major, sys.version_info.minor))
    pycos.Task(receiver_proc2)
