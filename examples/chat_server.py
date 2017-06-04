#!/usr/bin/env python

# chat server using sockets

# Run this program and at least two clients (chat_client.py) on same computer.
# To use clients on other computers, give network IP address as argument (and
# port as additional argument if necessary)

import pycos, socket, sys, time

def client_send(clients, conn, task=None):
    task.set_daemon()

    while True:
        try:
            line = yield conn.recv_msg()
        except:
            break
        if not line:
            clients.discard(conn)
            break
        for client in clients:
            if client != conn:
                yield client.send_msg(line)

def chat(host, port, task=None):
    task.set_daemon()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock = pycos.AsyncSocket(sock)
    sock.bind((host, port))
    sock.listen(128)
    print('server at %s' % str(sock.getsockname()))

    clients = set()

    try:
        while True:
            conn, addr = yield sock.accept()
            clients.add(conn)
            pycos.Task(client_send, clients, conn)
    except:
        for client in clients:
            client.shutdown(socket.SHUT_RDWR)
            client.close()
        raise

if __name__ == '__main__':
    # optional arg 1 is host IP address and arg 2 is port to use
    host, port = '', 3456
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    pycos.Task(chat, host, port)
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input
    while True:
        try:
            cmd = read_input('Enter "quit" or "exit" to terminate: ').strip().lower()
            if cmd.strip().lower() in ('quit', 'exit'):
                break
        except:
            break
