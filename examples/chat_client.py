#!/usr/bin/env python

# chat client using sockets, to be used with chat_server.py. To use server on
# another computer, give network IP address as argument (and port as additional
# argument if necessary)

import pycos, socket, sys


def client_recv(sock, task=None):
    while True:
        try:
            msg = yield sock.recv_msg()
        except:
            break
        if not msg:
            break
        print('  %s' % msg.decode())
    sender.terminate()


def client_send(sock, task=None):
    # since readline is synchronous (blocking) call, use async thread;
    # alternately, input can be read in 'main' and sent to this task (with
    # message passing)
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
            yield sock.send_msg(line.encode())
        except:
            break


if __name__ == '__main__':
    # optional arg 1 is host IP address and arg 2 is port to use
    host, port = '', 3456
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # in other programs socket is converted to asynchronous and 'connect' is
    # used with 'yield' for async I/O. Here, for illustration, socket is first
    # connected synchronously (since 'yield' can not be used in 'main') and then
    # it is setup for asynchronous I/O
    sock.connect((host, port))
    sock = pycos.AsyncSocket(sock)
    sender = pycos.Task(client_send, sock)
    recvr = pycos.Task(client_recv, sock)
    sender.value()
    recvr.terminate()
