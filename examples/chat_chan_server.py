#!/usr/bin/env python

# chat server; must be used with 'chat_chan_client.py'

import sys, logging
# import netpycos to use distributed version of Pycos
import pycos.netpycos as pycos

# pycos will disconnect if MaxConnectionErrors number of networking errors
# (e.g., conection / send timeout) occur; default is 10
pycos.MaxConnectionErrors = 3

def server_proc(task=None):
    # to illustrate 'transform' function of channel, messages are modified
    def txfm_msgs(name, msg_cid):
        msg, client_id = msg_cid
        # assert name == 'chat_channel'
        # e.g., drop shoutings
        if msg.isupper():
            return None
        if msg == 'joined':
            msg += ' :-)'
        elif msg == 'bye':
            msg = 'left :-('
        else:
            msg = 'says: %s' % msg
        return (msg, client_id)

    channel = pycos.Channel('chat_channel', transform=txfm_msgs)
    channel.register()
    task.set_daemon()
    task.register('chat_server')
    client_id = 1
    while True:
        # each message is a 2-tuple
        cmd, who = yield task.receive()
        # join/quit messages can be sent by clients themselves, but
        # for illustration server sends them instead
        if cmd == 'join':
            channel.send(('joined', client_id))
            who.send(client_id)
            client_id += 1
        elif cmd == 'quit':
            channel.send(('bye', who))
        elif cmd == 'terminate':
            break
    channel.unregister()
    task.unregister()

if __name__ == '__main__':
    # pycos.logger.setLevel(logging.DEBUG)
    server = pycos.Task(server_proc)
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
    server.send(('terminate', None))
    server.value() # wait for server to finish
