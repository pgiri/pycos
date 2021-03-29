# Run 'dispycosnode.py' program on one or more nodes (to start servers to execute tasks
# sent by this client), and this program on local computer(s).

# this is a simple example to show how KeyboardInterrupt exception can be used in remote tasks

# this generator function is sent to remote dispycos servers to run tasks there
def compute_proc(client_task, task=None):
    import random, time

    best = 0
    last_update = 0
    # this task never terminates on its own, so client / scheduler / node must terminate it; in
    # this case, client closes computation (causing scheduler to terminate) after a while.
    while 1:
        v = random.uniform(0, 1)
        if v > best:
            best = v
            now = time.time()
            if (now - last_update) > 10:
                # it is safer to send replies as objects of private class (passed with 'depeneds'
                # to avoid confusion with any other messages processed in 'status_proc')
                client_task.send([task.location, best])
                last_update = now
        try:
            # simulate computation
            yield task.sleep(3)
        except KeyboardInterrupt:
            # this task is about to be terminated (in about 5 seconds); in real world use cases
            # this can be used to send appropriate reply, save state, send files to client etc.
            break

    # for illustration, we will manipulate 'best' result to send better value; note that this task
    # has about 5 seconds to finish, after which it will be terminated
    low = best
    for i in range(5):
        v = random.uniform(low, 1)
        if v > best:
            best = v
        yield task.sleep(random.uniform(0.25, 0.5))
    client_task.send([task.location, best])


# -- code below is executed locally --

# status messages indicating nodes, servers and remote tasks finish status are sent to this local
# task; in this case we process only servers initialized and closed
def status_proc(task=None):
    task.set_daemon()  # set as daemon so this task is terminated automatically when exiting
    while 1:
        msg = yield task.recv()
        if isinstance(msg, DispycosStatus):
            if msg.status == Scheduler.ServerInitialized:
                pycos.logger.debug('server at %s is available', msg.info)
                # start new computation task at this server
                rtask = yield client.rtask_at(msg.info, compute_proc, client_task)
            elif msg.status == Scheduler.ServerClosed or msg.status == Scheduler.ServerAbandoned:
                pycos.logger.debug('server at %s is closed', msg.info)


# this local task submits client to dispycos scheduler, shows latest updates
def client_proc(task=None):
    # set status_task separately as status_proc needs 'client' argument
    client.status_task = pycos.Task(status_proc)
    # schedule client with the scheduler
    if (yield client.schedule()):
        raise Exception('schedule failed')

    # process results from 'compute_proc' rtasks
    best = 0
    while 1:
        # as best value approaches 1, it may take long time to receive next best updates;
        # here, the computations are stopped if no update received for 60 seconds
        msg = yield task.recv(timeout=60)
        if isinstance(msg, list):  # safer approach is to use special class instead of list
            if len(msg) == 2 and isinstance(msg[0], pycos.Location):
                # from a compute task with latest best value
                if msg[1] > best:
                    pycos.logger.info('update from %s: %s', msg[0], msg[1])
                    best = msg[1]
        elif not msg:  # no message (update) received for 60 seconds
            pycos.logger.info('stopping computations; current best value is %s', best)
            break
        elif msg == 'quit':
            break

    yield client.close(terminate=True)
    if msg is None:
        # closing due to no updates in 60 seconds; need to terminate loop in __main__
        if os.name == 'nt':
            signum = signal.CTRL_BREAK
        else:
            signum = signal.SIGINT
        os.kill(os.getpid(), signum)


if __name__ == '__main__':
    import pycos.dispycos, sys, os, signal
    import pycos.netpycos as pycos
    from pycos.dispycos import *

    pycos.logger.setLevel(pycos.Logger.DEBUG)

    client = Client([compute_proc])
    client_task = pycos.Task(client_proc)

    print('   Enter "quit" or "exit" to end the program ')
    if sys.version_info.major < 3:
        input = raw_input

    while True:
        try:
            inp = input().strip().lower()
            if inp == 'quit' or inp == 'exit':
                break
        except KeyboardInterrupt:
            break
    client_task.send('quit')
