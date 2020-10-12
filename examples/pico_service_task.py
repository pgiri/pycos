#!/usr/bin/env python

# to be used with 'pico_client_task.py'

# Example shows how to run tiny (pico) services, such as reading a sensor or controlling a device;
# here, time at server is requested. In this example service task receives requests and sends
# replies. Compare this to 'pico_service_rti.py' where each request is served by a different
# task. This example can be modified so pico_service task creates a new task to serve a request
# instead (as done in 'remote_task_server.py').

import sys
import time
import pycos
# import netpycos to add networking to pycos
import pycos.netpycos

# PyPI / pip packaging adjusts assertion below for Python 3.7+
if sys.version_info.major == 3:
    assert sys.version_info.minor < 7, \
        ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
         (__file__, sys.version_info.major, sys.version_info.minor))


def pico_service(task=None):
    task.set_daemon()
    task.register()
    while 1:
        req = yield task.recv()
        if not isinstance(req, dict):
            continue
        reply = req.get('reply', None)
        delay = req.get('delay', None)
        if (req.get('req', None) == 'time' and isinstance(reply, pycos.Task) and
            isinstance(delay, (int, float))):

            yield task.sleep(delay)  # simulate delay in getting result
            reply.send({'result': time.asctime(), 'from': task})

if __name__ == '__main__':
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # 'secret' is set so only peers that use same secret can communicate;
    # SSL can be used for encryption if required; see 'rti_node_*.py' for authentication of peers
    scheduler = pycos.Pycos(name='pico_server', secret='PycOS')
    server = pycos.Task(pico_service)

    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input

    while 1:
        try:
            line = read_input('Enter "quit" or "exit" to terminate pico_service: ').strip().lower()
            if line in ('quit', 'exit'):
                break
        except:
            break
