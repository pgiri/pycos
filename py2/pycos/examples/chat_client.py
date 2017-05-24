#!/usr/bin/env python

import pycos, socket, sys

def client_recv(conn, sender, task=None):
    while True:
        line = yield conn.recv_msg()
        if not line:
            sender.terminate()
            break
        print(line.decode())

def client_send(conn, task=None):
    thread_pool = pycos.AsyncThreadPool(1)
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input
    while True:
        try:
            line = yield thread_pool.async_task(read_input)
            line = line.strip()
            if line in ('quit', 'exit'):
                break
        except:
            break
        yield conn.send_msg(line.encode())

if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 1234))
    conn = pycos.AsyncSocket(sock)
    sender = pycos.Task(client_send, conn)
    recvr = pycos.Task(client_recv, conn, sender)
    sender.value()
    recvr.terminate()
