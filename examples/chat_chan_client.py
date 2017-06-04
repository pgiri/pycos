#!/usr/bin/env python

# run at least two instances of this program on either same node or multiple
# nodes on local network, along with 'chat_chan_server.py'; text typed in a
# client is broadcast over a channel to all clients

import sys, logging
import pycos.netpycos as pycos

# this task receives messages from server
def recv_proc(client_id, task=None):
    task.set_daemon()
    while True:
        msg, who = yield task.receive()
        # ignore messages from self (sent by local 'send_proc')
        if who == client_id:
            continue
        print('    %s %s' % (who, msg))

# this task sends messages to channel (to broadcast to all clients)
def send_proc(task=None):
    # if server is in a remote network, use 'peer' as (optionally enabling
    # streaming for efficiency):
    # yield pycos.Pycos.instance().peer('server node/ip')
    server = yield pycos.Task.locate('chat_server')
    server.send(('join', task))
    client_id = yield task.receive()
    
    # channel is at same location as server task
    channel = yield pycos.Channel.locate('chat_channel', server.location)
    recv_task = pycos.Task(recv_proc, client_id)
    yield channel.subscribe(recv_task)
    # since readline is synchronous (blocking) call, use async thread
    async_threads = pycos.AsyncThreadPool(1)
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input
    while True:
        try:
            line = yield async_threads.async_task(read_input)
            line = line.strip()
            if line.lower() in ('quit', 'exit'):
                break
        except:
            break
        # send message to channel
        channel.send((line, client_id))
    server.send(('quit', client_id))
    yield channel.unsubscribe(recv_task)

if __name__ == '__main__':
    # pycos.logger.setLevel(logging.DEBUG)
    pycos.Task(send_proc)
