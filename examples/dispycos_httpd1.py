# Run 'dispycosnode.py' program to start servers to execute computations sent by
# this client, along with this program.

# This example shows how to use 'httpd' module to start HTTP server so cluster /
# node / server / remote task status can be monitored in a web browser at
# http://127.0.0.1:8181

# objects of C are exchanged between client and servers
class C(object):
    def __init__(self, i):
        self.i = i
        self.n = None

    def __repr__(self):
        return '%d: %s' % (self.i, self.n)


# this generator function is sent to remote dispycos servers to run tasks there
def compute(obj, client, task=None):
    # obj is an instance of C
    yield task.sleep(obj.n)


# -- code below is executed locally --

def client_proc(task=None):
    # create http server to monitor nodes, servers, tasks
    http_server = pycos.httpd.HTTPServer(client)
    if (yield client.schedule()):
        raise Exception('schedule failed')

    i = 0
    while True:
        cmd = yield task.receive()
        if cmd is None:
            break
        i += 1
        c = C(i)
        try:
            c.n = float(cmd)
        except Exception:
            print('  "%s" is not a number' % cmd)
            continue
        else:
            # unlike in dispycos_client*.py, here 'io_rtask' is used to run as
            # many tasks as given on servers (i.e., possibly more than one task
            # on a server at any time).
            yield client.io_rtask(compute, c, task)

    # close client with 'await_io=True' to wait until all running I/O
    # tasks to finish before closing client
    yield client.close(await_io=True)
    http_server.shutdown()


if __name__ == '__main__':
    import pycos.dispycos, pycos.httpd, sys
    import pycos
    import pycos.netpycos
    from pycos.dispycos import *

    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    # send generator function and class C (as the client uses objects of C)
    # use MinPulseInterval so node status updates are sent more frequently
    # (instead of default 2*MinPulseInterval)
    client = Client([compute, C], pulse_interval=pycos.dispycos.MinPulseInterval)
    task = pycos.Task(client_proc)

    print('   Enter "quit" or "exit" to end the program, or ')
    print('   Enter a number to schedule a task on one of the servers')
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
