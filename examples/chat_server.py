#!/usr/bin/env python

# chat server using sockets

# Run this program and at least two clients (chat_client.py) on same computer.
# To use clients on other computers, give network IP address as argument (and
# port as additional argument if necessary)

import pycos, socket, sys


def client_conn_proc(conn, task=None):
    task.set_daemon()
    clients.add(conn)
    while True:
        try:
            line = yield conn.recv_msg()
        except:
            break
        if not line:
            break
        msg = '%s says: %s' % (conn.fileno(), line)
        msg = msg.encode()
        # Iterate through copy of 'clients', as it may be changed in 'chat'
        # while iterating (since 'yield' is used, this task and other
        # client_conn tasks may be interleaved by task scheduler). See
        # 'chat_server2.py' for an alternate implementation.
        for client in list(clients):
            if client != conn:
                yield client.send_msg(msg)
    clients.discard(conn)


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
        for client in clients:
            client.close()
        raise


if __name__ == '__main__':
    # optional arg 1 is host IP address and arg 2 is port to use
    host, port = '', 3456
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    pycos.Task(server_proc, host, port)
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input

    clients = set()
    while True:
        try:
            cmd = read_input('Enter "quit" or "exit" to terminate: ').strip().lower()
            if cmd in ('quit', 'exit'):
                break
        except:
            break
    for client in clients:
        client.close()
