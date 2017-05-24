#!/usr/bin/env python

import pycos, socket, logging, sys, time

def client_send(clients, conn, task=None):
    task.set_daemon()
    logging.debug('%s/%s started with %s', task.name, id(task), conn._fileno)

    while True:
        try:
            line = yield conn.recv_msg()
        except:
            break
        if not line:
            logging.debug('removing %s', conn._fileno)
            clients.discard(conn)
            break
        # logging.debug('got line "%s"', line)
        for client in clients:
            if client != conn:
                # logging.debug('sending "%s" to %s', line, client._fileno)
                yield client.send_msg(line)

def chat(host='localhost', port=1234, task=None):
    task.set_daemon()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock = pycos.AsyncSocket(sock)
    sock.bind((host, port))
    sock.listen(128)
    logging.debug('server at %s', str(sock.getsockname()))

    clients = set()

    try:
        while True:
            conn, addr = yield sock.accept()
            clients.add(conn)
            pycos.Task(client_send, clients, conn)
    except:
        for client in clients:
            logging.debug('closing %s', client._fileno)
            client.shutdown(socket.SHUT_RDWR)
            client.close()
        raise

if __name__ == '__main__':
    pycos.Task(chat)
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input
    while True:
        try:
            cmd = read_input()
            if cmd.strip().lower() in ('quit', 'exit'):
                break
        except:
            break
