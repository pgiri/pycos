#!/usr/bin/env python

# server program for client sending requests to execute tasks

# run this program and then client either on same node or different
# node on local network. Server and client can also be run on two
# different networks but client must call 'scheduler.peer' method
# appropriately.

import sys
# import netpycos to use distributed version of Pycos
import pycos.netpycos as pycos

def rti_1(a, b=1, task=None):
    pycos.logger.debug('running %s/%s with %s, %s', task.name, id(task), a, b)
    msg = yield task.receive()
    if b % 2 == 0:
        yield task.sleep(b)
        pycos.logger.debug('%s/%s done', task.name, id(task))
        # (remote) monitor (if any) gets this exception (to be
        # interpreted as normal termination)
        raise StopIteration(msg)
    else:
        # (remote) monitor (if any) gets this exception, too
        raise Exception('invalid invocation: %s' % b)

pycos.logger.setLevel(pycos.Logger.DEBUG)
# 'secret' is set so only peers that use same secret can communicate
scheduler = pycos.Pycos(name='server', secret='test')
# register rti_1 so remote clients can request execution
rti1 = pycos.RTI(rti_1)
rti1.register()

if sys.version_info.major > 2:
    read_input = input
else:
    read_input = raw_input
while True:
    try:
        line = read_input().strip().lower()
        if line in ('quit', 'exit'):
            break
    except:
        break
