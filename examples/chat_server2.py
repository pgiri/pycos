#!/usr/bin/env python

# chat server using sockets

# Run this program and at least two clients (chat_client.py) on same computer.
# To use clients on other computers, give network IP address as argument (and
# port as additional argument if necessary). This version maintains
# 'client_conns' in one task so avoids copying during broadcast.

import pycos, socket, sys


def client_conn_proc(conn, task=None):
    task.set_daemon()

    msg_bcast_task.send((conn, None))
    while True:
        try:
            line = yield conn.recv_msg()
        except:
            break
        if not line:
            break
        msg_bcast_task.send((conn, line))
    msg_bcast_task.send((conn, 0))


def msg_bcast_proc(task=None):
    task.set_daemon()

    client_conns = set()
    while True:
        try:
            conn, msg = yield task.recv()
        except GeneratorExit:
            break
        if msg:
            msg = '%s says: %s' % (conn.fileno(), msg)
            msg = msg.encode()
            for client_conn in client_conns:
                if client_conn != conn:
                    yield client_conn.send_msg(msg)
        elif msg is None:
            client_conns.add(conn)
        elif msg == 0:
            client_conns.discard(conn)
        else:
            pycos.logger.warning('  invalid message "%s" ignored', msg)
    for client_conn in client_conns:
        client_conn.close()


def server_proc(host, port, task=None):
    task.set_daemon()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # convert sock to asynchronous
    sock = pycos.AsyncSocket(sock)
    sock.bind((host, port))
    sock.listen(128)
    print('server at %s' % str(sock.getsockname()))

    try:
        while True:
            conn, addr = yield sock.accept()
            pycos.Task(client_conn_proc, conn)
    except:
        msg_bcast_task.terminate()


if __name__ == '__main__':
    # optional arg 1 is host IP address and arg 2 is port to use
    host, port = '', 3456
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input

    msg_bcast_task = pycos.Task(msg_bcast_proc)
    pycos.Task(server_proc, host, port)

    while True:
        try:
            cmd = read_input('Enter "quit" or "exit" to terminate: ').strip().lower()
            if cmd in ('quit', 'exit'):
                break
        except:
            break
    msg_bcast_task.terminate()
