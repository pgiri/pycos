#!/usr/bin/env python

# server program for client sending requests to execute tasks

# run this program and then client either on same node or different node on local network.

import sys
import subprocess
import hashlib

import pycos.asyncfile
# import netpycos to use networking
import pycos.netpycos


def node_update_rti(client, script, n, task=None):
    client_auth = yield task.recv()
    check_auth = auth.copy()
    check_auth.update(repr(client).encode())
    if check_auth.hexdigest() != client_auth:
        # it is simpler to send appropriate error to client; instead, for illustration, client's
        # monitor is used indicate failure
        raise StopIteration('Invalid authentication')

    print('RTI %s running %s for client: %s' % (task, script, client))
    if script.endswith('.py'):
        cmd = [sys.executable, script]
    else:
        cmd = [script]
    cmd.append(str(n))
    pipe = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    async_pipe = pycos.asyncfile.AsyncPipe(pipe)
    outerr = yield async_pipe.communicate()
    client.send(outerr)
    raise StopIteration(pipe.returncode)


if __name__ == '__main__':
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input

    # use authentication to prevent unauthorized clients from running scripts
    auth = hashlib.sha512(read_input('Enter authentication string: ').strip().encode())

    # use SSL for security in shared environment
    scheduler = pycos.Pycos(name='node_update_server')
    rti = pycos.RTI(node_update_rti)
    rti.register()

    while True:
        try:
            line = read_input().strip().lower()
            if line in ('quit', 'exit'):
                break
        except:
            break
