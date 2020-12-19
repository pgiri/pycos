#!/usr/bin/env python

# server program for client sending requests to execute tasks

# run this program and then client (rps_monitor_client.py) either on same node or different node
# on local network. Server and client can also be run on two different networks but client must
# call 'scheduler.peer' method appropriately.

import sys
import pycos
# import netpycos to use distributed version of Pycos
import pycos.netpycos

# PyPI / pip packaging adjusts assertion below for Python 3.7+
if sys.version_info.major == 3:
    assert sys.version_info.minor < 7, \
        ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
         (__file__, sys.version_info.major, sys.version_info.minor))


# when client invokes RPS in this program, function below is used to start a task
def rps_server(a, b=1, task=None):
    pycos.logger.debug('running %s with %s, %s', task, a, b)
    # receive message from client
    msg = yield task.receive(timeout=2)
    # to illustrate how client's monitor can receive exit values or exceptions, exception is
    # raised if given b is not a positve number, otherwise task sleeps for b seconds and exits
    # with msg
    if isinstance(b, (int, float)) and b > 0 and isinstance(msg, str):
        yield task.sleep(b)
        # (remote) monitor (if any) gets back msg (to be interpreted as normal termination)
        raise StopIteration(msg)
    else:
        # (remote) monitor (if any) gets this exception
        raise Exception('invalid invocation: %s' % b)


if __name__ == '__main__':
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # 'secret' is set so only peers that use same secret can communicate
    scheduler = pycos.Pycos(name='server', secret='test')
    # register rps_server so remote clients can request execution
    rps = pycos.RPS(rps_server)
    rps.register()

    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input
    while True:
        try:
            line = read_input().strip().lower()
            if line in ('quit', 'exit'):
                break
        except Exception:
            break
