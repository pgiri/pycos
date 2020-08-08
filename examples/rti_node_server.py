#!/usr/bin/env python

# server program for client sending requests to execute tasks

# run this program and then 'rti_node_client.py' either on same node or different node on local
# network.

import sys
import os
import subprocess
import hashlib

import pycos.asyncfile
# import netpycos to use networking
import pycos.netpycos


def node_update_rti(client, script, n, task=None):
    rand = os.urandom(20)
    client.send(rand)
    resp = yield task.recv(timeout=5)
    if not resp:
        raise StopIteration(None)
    check_auth = auth.copy()
    check_auth.update(rand)
    if resp != check_auth.hexdigest():
        raise StopIteration('Invalid authentication')

    script = os.path.join(script_path, os.path.basename(script))
    if not os.path.isfile(script):
        raise StopIteration('Invalid script %s' % os.path.basename(script))
    print('RTI %s running %s for client: %s' % (task, script, client))
    # print('script: %s / %s' % (script, os.path.join(script_path, os.path.basename(script))))

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

    # use 'secret' so (only) valid clients can save scripts with 'send_file' (which is done before
    # server authenticates clients)
    # use SSL for security in shared environment
    scheduler = pycos.Pycos(name='node_update_server', secret='update')
    script_path = os.path.join(scheduler.dest_path, 'rti_update_scripts')
    if not os.path.isdir(script_path):
        os.mkdir(script_path)
    # set Pycos dest_path so files sent by clients are saved in script_path
    scheduler.dest_path = script_path
    # alternately, (and safer to) put all scripts in this path and not allow clients to send
    # (arbitrary) files to server, but send just the script names

    # another option is for client to send contents of script to server which after validating
    # authentication, can save the data to file and run that script
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input

    # use authentication to prevent unauthorized clients from running scripts
    auth = hashlib.sha512(read_input('Enter authentication string: ').strip().encode())

    rti = pycos.RTI(node_update_rti)
    rti.register()

    while True:
        try:
            cmd = read_input('\nEnter "quit" or "exit" to terminate: ')
            if cmd.strip().lower() in ('quit', 'exit'):
                break
        except:
            break
