#!/usr/bin/env python

# to be used with 'pico_service_rti.py'
# client requests execution of tasks on (remote) server.

import sys
import random
import pycos
# import netpycos to add networking to pycos
import pycos.netpycos


def client_proc(n, task=None):
    # if server is on remote network, automatic discovery won't work,
    # so add it explicitly
    # yield scheduler.peer(pycos.Location('192.168.21.5', 9705))

    # get reference to RTI at server
    service_rti = yield pycos.RTI.locate('pico_service')
    pycos.logger.info('pico service RTI is at %s' % service_rti.location)

    for i in range(n):
        req = {'req': 'time', 'reply': task, 'delay': random.uniform(2, 5)}
        service_task = yield service_rti(req)
        if isinstance(service_task, pycos.Task):
            pycos.logger.info('service task: %s', service_task)

    for i in range(n):
        msg = yield task.recv(timeout=10)  # use 'timeout' in case request/reply fails
        if not msg:
            break
        if isinstance(msg, dict):
            pycos.logger.info('result: %s, from: %s', msg.get('result', None), msg.get('from', None))

if __name__ == '__main__':
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    if len(sys.argv) > 1:
        n = int(sys.argv[1])
    else:
        n = 10
    # use 'PycOS' secret so peers that use same secret are recognized
    scheduler = pycos.Pycos(secret='PycOS')
    pycos.Task(client_proc, n)
