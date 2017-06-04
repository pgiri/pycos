# program for broadcasting messages over channel; see
# http://pycos.sourceforge.io/tutorial.html for details.

import sys, random
import pycos

def seqsum(task=None):
    # compute sum of numbers received (from channel)
    result = 0
    while True:
        msg = yield task.receive()
        if msg is None:
            break
        result += msg
    print('sum: %f' % result)

def seqprod(task=None):
    # compute product of numbers received (from channel)
    result = 1
    while True:
        msg = yield task.receive()
        if msg is None:
            break
        result *= msg
    print('prod: %f' % result)

def client_proc(task=None):
    # create channel
    channel = pycos.Channel('sum_prod')
    # create tasks to compute sum and product of numbers sent
    sum_task = pycos.Task(seqsum)
    prod_task = pycos.Task(seqprod)
    # subscribe tasks to channel so they receive messages
    yield channel.subscribe(sum_task)
    yield channel.subscribe(prod_task)
    # send 4 numbers to channel
    for _ in range(4):
        r = random.uniform(0.5, 3)
        channel.send(r)
        print('sent %f' % r)
    # send None to indicate end of data
    channel.send(None)
    yield channel.unsubscribe(sum_task)
    yield channel.unsubscribe(prod_task)

if __name__ == '__main__':
    # create pycos Task
    pycos.Task(client_proc)
