#!/usr/bin/env python

# run at least two instances of this program on either same node or multiple
# nodes on local network, along with 'chat_sock_server.py'; text typed in a
# client is sent to the all other clients.  To use server on another computer,
# give network IP address as argument (and port as additional argument if
# necessary)

import sys, socket, time
import pycos

def client_recv(conn, task=None):
    conn = pycos.AsyncSocket(conn)
    while True:
        line = yield conn.recv_msg()
        if not line:
            break
        print(line.decode())

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
    pycos.Task(client_recv, sock)
    # wrap it with pycos's synchronous (with 'blocking=True') socket so
    # 'send_msg' can be used
    conn = pycos.AsyncSocket(sock, blocking=True)

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
        conn.send_msg(line.encode())
    conn.shutdown(socket.SHUT_WR)
