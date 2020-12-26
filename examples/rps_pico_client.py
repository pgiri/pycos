#!/usr/bin/env python

# to be used with 'rps_pico_service.py'
# client requests execution of tasks on (remote) server.

import sys
import pycos
# import netpycos to add networking to pycos
import pycos.netpycos


def client_proc(n, task=None):
    # if server is on remote network, automatic discovery won't work,
    # so add it explicitly
    # yield scheduler.peer(pycos.Location('192.168.21.5', 9705))

    # get reference to RPS at server
    service_rps = yield pycos.RPS.locate('pico_service')
    pycos.logger.info('pico service RPS is at %s' % service_rps.location)

    service_tasks = []
    for i in range(n):
        req = {'req': 'time', 'client': task}
        rtask = yield service_rps(req)
        if isinstance(rtask, pycos.Task):
            pycos.logger.info('service task: %s', rtask)
            service_tasks.append(rtask)

    for rtask in service_tasks:
        reply = yield rtask.finish(timeout=5)  # use timeout in case of failures
        if isinstance(reply, dict):
            assert rtask == reply.get('server')
            pycos.logger.info('result: %s, from: %s',
                              reply.get('result', None), reply.get('server', None))


if __name__ == '__main__':
    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    if len(sys.argv) > 1:
        n = int(sys.argv[1])
    else:
        n = 5
    # use 'PycOS' secret so peers that use same secret are recognized
    scheduler = pycos.Pycos(secret='PycOS')
    pycos.Task(client_proc, n)
