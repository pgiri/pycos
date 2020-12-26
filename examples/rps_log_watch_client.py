#!/usr/bin/env python

# to be used with 'rps_log_watch_server.py'
# client requests execution of tasks on (remote) server.

import sys
import pycos
# import netpycos to use distributed version of Pycos
import pycos.netpycos


# this task receives log messages from servers (running 'rps_monitor_server')
def rps_client(task=None):
    task.set_daemon()
    while 1:
        msg = yield task.receive()
        if isinstance(msg, tuple) and len(msg) == 2:
            pass
        else:
            pycos.logger.warning('Ignoring message: %s', type(msg))
            continue
        # each message should be tuple of server task and log line
        who, line = msg
        if not isinstance(who, pycos.Task) or not isinstance(line, (str, bytes)):
            pycos.logger.warning('Ignoring message: %s / %s', type(who), type(line))
            continue
        # TODO: check if 'who' in 'servers'
        print('%s: %s' % (who.location.addr, line.decode()))


# this task gets peer status (online / off-line) notification
def peer_status(task=None):
    client = pycos.Task(rps_client)
    rpss = {}
    while 1:
        status = yield task.receive()
        if not isinstance(status, pycos.PeerStatus):
            if status == 'quit':
                break
            pycos.logger.warning('Invalid peer status %s ignored', type(status))
            continue
        if status.status == pycos.PeerStatus.Online:
            # if peer has rps_log_monitor, run RPS there
            def discover_rps(location, task=None):
                rps = yield pycos.RPS.locate('rps_log_monitor', location=location, timeout=5)
                if isinstance(rps, pycos.RPS):
                    rpss[rps] = rps
                    server = yield rps(client)
                    if isinstance(server, pycos.Task):
                        servers[location] = server
            pycos.Task(discover_rps, status.location)
        else:  # status.status == pycos.PeerStatus.Offline
            servers.pop(status.location, None)

    for rps in rpss.values():
        rps.close()


if __name__ == '__main__':
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # use 'secret' as used by server
    scheduler = pycos.Pycos(name='client', secret='LogMon')
    servers = {}
    # set peer_status notification
    status_monitor = pycos.Task(peer_status)
    scheduler.peer_status(status_monitor)

    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input
    while True:
        try:
            line = read_input('Enter "quit" or "exit" to quit: ').strip().lower()
            if line in ('quit', 'exit'):
                break
        except Exception:
            break
    status_monitor.send('quit')
