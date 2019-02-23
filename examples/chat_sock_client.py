#!/usr/bin/env python

# run at least two instances of this program on either same node or multiple
# nodes on local network, along with 'chat_sock_server.py'; text typed in a
# client is sent to the all other clients.  To use server on another computer,
# give network IP address as argument (and port as additional argument if
# necessary)

import sys, socket
import pycos


def client_recv(conn, task=None):
    task.set_daemon()
    while True:
        line = yield conn.recv_msg()
        if not line:
            break
        print(line.decode())


def client_send(conn, task=None):
    task.set_daemon()
    while True:
        msg = yield task.recv()
        yield conn.send_msg(msg)


if __name__ == '__main__':
    # pycos.logger.setLevel(pycos.logger.DEBUG)
    # optional arg 1 is host IP address and arg 2 is port to use
    host, port = '', 3456
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock = pycos.AsyncSocket(sock)
    # same connection is used to receive messages in one task and to send
    # messages in another task
    pycos.Task(client_recv, sock)
    sender = pycos.Task(client_send, sock)

    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input
    while True:
        try:
            line = read_input().strip()
            if line.lower() in ('quit', 'exit'):
                break
            if not line:
                continue
        except:
            break
        # use message passing to send message to sender which will use
        # connection's asynchronous socket method to transmit it
        sender.send(line.encode())
    sock.close()
