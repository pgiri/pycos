#!/usr/bin/env python

# Alternative implementation of chat server (see 'chat_server.py'); must be used
# with 'chat_sock_client.py'.  To use clients on other computers, give network
# IP address as argument (and port as additional argument if necessary)

import sys, socket
import pycos

# task to deal with specific client
def client_proc(conn, addr, server, task=None):
    # given conn is synchronous, convert it to asynchronous
    conn = pycos.AsyncSocket(conn)
    server.send(('joined'.encode(), (conn, addr)))

    while True:
        try:
            line = yield conn.recv_msg()
        except:
            line = None
        if not line:
            server.send(('left'.encode(), (conn, addr)))
            break
        server.send(('broadcast'.encode(), (conn, line)))

# server task to deal with messages from clients
def server_proc(task=None):
    clients = set()
    while True:
        cmd, item = yield task.receive()
        cmd = cmd.decode()
        if cmd == 'broadcast':
            conn, msg = item
            msg = ('%s says: %s' % (conn.fileno(), msg.decode())).encode()
            for client in clients:
                if conn != client:
                    yield client.send_msg(msg)
        elif cmd == 'joined':
            conn, addr = item
            yield conn.send_msg(('id: %s' % conn.fileno()).encode())
            msg = ('%s joined' % conn.fileno()).encode()
            for client in clients:
                yield client.send_msg(msg)
            clients.add(conn)
        elif cmd == 'left':
            conn, addr = item
            clients.discard(conn)
            msg = ('%s left' % conn.fileno()).encode()
            conn.close()
            for client in clients:
                yield client.send_msg(msg)
        elif cmd == 'terminate':
            break

if __name__ == '__main__':
    # pycos.logger.setLevel(pycos.logger.DEBUG)
    # optional arg 1 is host IP address and arg 2 is port to use
    host, port = '', 3456
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])

    server = pycos.Task(server_proc)
    # in this case, synchronous socket is used for accepting connections and
    # each client is created with pycos to handle messages
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(128)

    print('server at %s' % str(sock.getsockname()))
    print('Ctrl-C to terminate server')
    try:
        while True:
            conn, addr = sock.accept()
            # each connection is handled by a task
            pycos.Task(client_proc, conn, addr, server)
    except:
        pass
    server.send(('terminate'.encode(), None))
