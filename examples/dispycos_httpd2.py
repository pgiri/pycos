# Run 'dispycosnode.py' program to start servers to execute computations sent by
# this client, along with this program.

# This example is similar to 'dispycos_httpd1.py'. It sets up 'status_task' of
# client to get status messages from dispycos scheduler (which are actally
# relayed by httpd's status_task) to show when remote task is created and
# finished.

# objects of C are exchanged between client and servers
class C(object):
    def __init__(self, i):
        self.i = i
        self.n = None

    def __repr__(self):
        return '%d: %s' % (self.i, self.n)


# this generator function is sent to remote dispycos servers to run
# tasks there
def compute(obj, client, task=None):
    # obj is an instance of C
    yield task.sleep(obj.n)


# -- code below is executed locally --

# status messages indicating nodes, servers and remote tasks
# finish status are sent to this task
def status_proc(task=None):
    task.set_daemon()
    while True:
        msg = yield task.receive()
        # (re)send all status messages to http server
        if isinstance(msg, pycos.MonitorStatus):
            if msg.type == StopIteration:
                print('    rtask %s done' % (msg.info))
            else:
                print('  rtask %s failed: %s / %s' % (msg.info, msg.type, msg.value))
        elif isinstance(msg, DispycosStatus):
            if msg.status == Scheduler.TaskStarted:
                print('rtask %s started' % msg.info.task)
            # else:
            #     print('Status: %s / %s' % (msg.status, msg.info))

        elif isinstance(msg, DispycosNodeAvailInfo):
            pass
        else:
            print('status msg ignored: %s' % type(msg))


def client_proc(task=None):
    # create http server to monitor nodes, servers, tasks
    http_server = pycos.httpd.HTTPServer(client)
    # schedule client with the scheduler
    if (yield client.schedule()):
        raise Exception('schedule failed')

    i = 0
    while True:
        cmd = yield task.receive()
        if cmd is None:
            break
        i += 1
        c = C(i)
        c.n = random.uniform(20, 50)
        # unlike in dispycos_client*.py, here 'io_rtask' is used to run as
        # many tasks as given on servers (i.e., possibly more than one
        # task on a server at any time).
        rtask = yield client.io_rtask(compute, c, task)
        if isinstance(rtask, pycos.Task):
            print('  %s: rtask %s created' % (i, rtask))
        else:
            print('  %s: rtask failed: %s' % (i, rtask))

    # unlike in dispycos_httpd1.py, here 'await_async' is not used, so any
    # running async tasks are just terminated.
    yield client.close()
    http_server.shutdown()


if __name__ == '__main__':
    import pycos.dispycos, pycos.httpd, sys, random
    import pycos
    import pycos.netpycos
    from pycos.dispycos import *

    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # send generator function and class C (as the client uses objects of C)
    # use MinPulseInterval so node status updates are sent more frequently
    # (instead of default 2*MinPulseInterval)
    client = Client([compute, C], status_task=pycos.Task(status_proc),
                    pulse_interval=pycos.dispycos.MinPulseInterval)

    task = pycos.Task(client_proc)

    print('   Enter "quit" or "exit" to end the program, or ')
    print('   Enter anything else to schedule a task on one of the servers')
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input

    while True:
        try:
            cmd = read_input().strip().lower()
            if cmd in ('quit', 'exit'):
                break
        except Exception:
            break
        task.send(cmd)
    task.send(None)
