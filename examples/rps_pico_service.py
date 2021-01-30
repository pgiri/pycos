#!/usr/bin/env python

# to be used with 'rps_pico_client.py'

# Example to illustrate RPS feature to run tiny (pico) services, such as reading sensor data or
# controlling a device; here, time at server is requested. In this example a new service task is
# created to serve a request. Compare this to 'task_pico_service.py' where requests are processed
# by the same server task.

import sys
import time
import random
import pycos
# import netpycos to add networking to pycos
import pycos.netpycos

# PyPI / pip packaging adjusts assertion below for Python 3.7+
if sys.version_info.major == 3:
    assert sys.version_info.minor < 7, \
        ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
         (__file__, sys.version_info.major, sys.version_info.minor))


def pico_service(req, task=None):
    if not isinstance(req, dict):
        raise Exception('request must be a dictionary')

    client = req.get('client', None)
    if req.get('name', None) != 'time':
        raise Exception('request should have "name" set to "time"')
    if not isinstance(client, pycos.Task):
        raise Exception('request should have "client" set to task of requester')
    delay = random.uniform(0.5, 2)
    # simulate delay in getting result (e.g., reading a sensor or computation)
    yield task.sleep(delay)
    raise StopIteration({'result': time.asctime(), 'server': task})


if __name__ == '__main__':
    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    # 'secret' is set so only peers that use same secret can communicate;
    # SSL can be used for encryption if required; see 'rps_node_*.py' for authentication of peers
    scheduler = pycos.Pycos(name='pico_server', secret='PycOS')
    # create RPS and register it so remote clients can request execution
    pycos.RPS(pico_service).register()

    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input

    while 1:
        try:
            line = read_input('Enter "quit" or "exit" to terminate pico_service: ').strip().lower()
            if line in ('quit', 'exit'):
                break
        except Exception:
            break
