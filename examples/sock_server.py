#!/usr/bin/env python

# server program for client (sock_client.py) sending requests with sockets
# (asynchronous network programming); see https://pycos.sourceforge.io for
# details.

# run this program and then client on same node. If they are on different
# computers, 'host' address must be changed appropriately.

import sys, socket
import pycos

# deal with a specific client
def process(conn, task=None):
    global recvd
    data = ''.encode()
    while True:
        # receive chunk (may be smaller than message)
        chunk = yield conn.recv(128)
        if not chunk:
            break
        data += chunk
    conn.close()
    print('recieved: %s' % data)
    recvd += 1

# server accepts connections and creates tasks to deal with them
def server(host, port, task=None):
    task.set_daemon()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # setup socket for asynchronous I/O with pycos
    sock = pycos.AsyncSocket(sock)
    sock.bind((host, port))
    sock.listen(128)

    while True:
        conn, addr = yield sock.accept()
        # create a task to process connection
        pycos.Task(process, conn)

# pycos.logger.setLevel(pycos.Logger.DEBUG)
recvd = 0
pycos.Task(server, '', 8010)

if sys.version_info.major > 2:
    read_input = input
else:
    read_input = raw_input
while True:
    cmd = read_input('Enter "quit" or "exit" to terminate: ').strip().lower()
    if cmd == 'exit' or cmd == 'quit':
        break
print('Received %d messages' % recvd)
