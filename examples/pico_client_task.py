#!/usr/bin/env python

# to be used with 'picos_service_task.py'


import sys
import random
import pycos
# import netpycos to add networking to pycos
import pycos.netpycos


def client_proc(task=None):
    # if server is on remote network, automatic discovery won't work,
    # so add it explicitly
    # yield scheduler.peer(pycos.Location('192.168.21.5', 9705))

    # get reference to server
    service_task = yield pycos.Task.locate('pico_service', timeout=10)
    if not isinstance(service_task, pycos.Task):
        pycos.logger.warning('could not find pico service task')
        raise StopIteration
    pycos.logger.info('pico service task is at %s' % service_task.location)
    req = {'req': 'time', 'reply': task, 'delay': random.uniform(2, 5)}
    service_task.send(req)
    msg = yield task.recv(timeout=10)  # use 'timeout' in case request/reply fails
    if isinstance(msg, dict):
        pycos.logger.info('result: %s, from: %s', msg.get('result', None), msg.get('from', None))
    else:
        pycos.logger.warning('failed to get reply')

if __name__ == '__main__':
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # use 'PycOS' secret so peers that use same secret are recognized
    scheduler = pycos.Pycos(secret='PycOS')
    pycos.Task(client_proc)
