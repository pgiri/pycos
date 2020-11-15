# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This example runs a program (dispycos_client5_proc.py) on a remote server. The
# program reads from standard input and writes to standard output. Subprocess
# and asynchronous pipes are used to write / read data from the program, and
# message passing is used to send data from client to the program and to send
# output from the program back to the client.

# rtask_proc is sent to remote server to execute dispycos_client5_proc.py
# program. It uses message passing to get data from client that is sent to the
# program's stdin using pipe and read output from program that is sent back to
# client.
def rtask_proc(client, program, task=None):
    import sys
    import os
    import subprocess
    import pycos.asyncfile

    if program.endswith('.py'):
        # computation dependencies are saved in parent directory
        program = [sys.executable, os.path.join('..', program)]
    # start program as a subprocess (to read from and write to pipe)
    if os.name == 'nt':  # create pipe with asyncfile under Windows
        pipe = pycos.asyncfile.Popen(program, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    else:
        pipe = subprocess.Popen(program, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    # convert to asynchronous pipe; see 'pipe_csum.py' and 'pipe_grep.py' for
    # chaining pipes
    pipe = pycos.asyncfile.AsyncPipe(pipe)

    # reader reads (output) from pipe and sends to client as messages
    def reader_proc(task=None):
        while True:
            line = yield pipe.readline()
            if not line:
                break
            # send output to client
            client.send(line)
        pipe.stdout.close()
        if os.name == 'nt':
            pipe.close()
        client.send(None)

    reader = pycos.Task(reader_proc)

    # writer gets messages from client and writes them (input) to pipe
    while True:
        data = yield task.receive()
        if not data:
            break
        # write data as lines to program
        yield pipe.write(data + '\n'.encode(), full=True)
    pipe.stdin.close()
    # wait for all data to be read (subprocess to end)
    yield reader()
    raise StopIteration(pipe.poll())


# -- code below is executed locally --

# client (local) task submits tasks for remote execution at dispycos servers
def client_proc(program, n, task=None):
    # schedule client with the scheduler; scheduler accepts one client
    # at a time, so if scheduler is shared, the client is queued until it
    # is done with already scheduled clients
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    # send 10 random numbers to remote process (rtask_proc)
    def send_input(rtask, task=None):
        for i in range(10):
            # encode strings so works with both Python 2.7 and 3
            rtask.send(('%.2f' % random.uniform(0, 5)).encode())
            # assume delay in input availability
            yield task.sleep(random.uniform(0, 2))
        # end of input is indicated with None
        rtask.send(None)

    # read output (messages sent by 'reader_proc' on remote process)
    def get_output(i, task=None):
        while True:
            line = yield task.receive()
            if not line:  # end of output
                break
            print('      job %s output: %s' % (i, line.strip().decode()))

    def create_job(i, task=None):
        # create reader and send to rtask so it can send messages to reader
        client_reader = pycos.Task(get_output, i)
        # schedule rtask on (available) remote server
        rtask = yield client.rtask(rtask_proc, client_reader, os.path.basename(program))
        if isinstance(rtask, pycos.Task):
            print('  job %s processed by %s' % (i, rtask.location))
            # sender sends input data to rtask
            pycos.Task(send_input, rtask)
            # wait for all data to be received
            yield client_reader()
            print('  job %s done' % i)
        else:  # failed to schedule
            print('  ** job %s failed: %s' % (i, rtask))
            client_reader.terminate()

    # create n jobs (that run concurrently)
    job_tasks = []
    for i in range(1, n+1):
        job_tasks.append(pycos.Task(create_job, i))
    # wait for jobs to finish
    for job_task in job_tasks:
        yield job_task()

    yield client.close()


if __name__ == '__main__':
    import sys, random, os
    import pycos
    import pycos.netpycos
    from pycos.dispycos import *

    pycos.logger.setLevel(pycos.Logger.DEBUG)
    # PyPI / pip packaging adjusts assertion below for Python 3.7+
    if sys.version_info.major == 3:
        assert sys.version_info.minor < 7, \
            ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
             (__file__, sys.version_info.major, sys.version_info.minor))

    # dispycos saves depedency files in node's directory. If the file at client
    # is at or below current directory (in directory hierarchy), then the file
    # will be saved with same relative path at client (e.g., if file is
    # './file1', then file is saved in node directory, and if file is
    # './dir/file2' then file is saved in 'dir' directory under node's
    # directory). However, if the file is not under current directory, then file
    # is saved in node's directory itself (without any path).

    # program to distribute and execute
    program = os.path.join(os.path.dirname(sys.argv[0]), 'dispycos_client5_proc.py')

    # Or (see 'Either' above), get the path as dispycos would: If current
    # directory is a parent of program's path, get relative path to it, or just
    # the file name otherwise.
    # if program.startswith(os.getcwd()):
    #     program = program[len(os.getcwd()+os.sep):]
    # else:
    #     program = os.path.basename(program)

    # send rtask_proc and program
    client = Client([rtask_proc, program])
    # run n (defailt 5) instances of program
    pycos.Task(client_proc, program, 5 if len(sys.argv) < 2 else int(sys.argv[1]))
