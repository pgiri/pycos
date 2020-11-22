# Run 'dispycosnode.py' program to start processes to execute computations sent
# by this client, along with this program.

# This example shows how to use message passing, transferring files.  The input
# files are assumed to be in current working directory as 'input0.dat',
# 'input1.dat', ..., 'input9.dat' (10 input files).  'run_jobs_proc' creates
# jobs as and when a dispycos process is found, or a process finishes a
# job. Each job is processed by a remote task (rtask) and a corresponding local
# task (with 'client_proc') interacts with that rtask to send the input data to
# where rtask is, an instance of class C to give the information needed to
# process the file. Client then waits for rtask to finish processing job, send
# the resulting output file in the form 'outputX.dat' and result (in this case,
# it is the return value of 'send_file').

# Note that the objective is to illustrate features, so implementation is not
# ideal. Error checking is skipped at a few places for brevity.

# objects of C are sent by a client to remote task
class C(object):
    def __init__(self, i, data_file, n, client):
        self.i = i
        self.data_file = data_file
        self.result_file = None
        self.n = n
        self.client = client


# this generator function is sent to remote server to run tasks there
def rtask_proc(task=None):
    import os
    # receive object from client_proc task
    cobj = yield task.receive()
    if not cobj:
        raise StopIteration
    # Input file is already copied at where this rtask is running (by client).
    # For given input file, create an output file with each line in the output
    # file computed as length of corresponding line in input file
    cobj.result_file = 'result-%s' % cobj.data_file
    with open(cobj.data_file, 'r') as data_fd:
        with open(cobj.result_file, 'w') as result_fd:
            for lineno, line in enumerate(data_fd, start=1):
                result_fd.write('%d: %d\n' % (lineno, len(line)-1))
    # 'sleep' to simulate computing
    yield task.sleep(cobj.n)
    # transfer the result file to client
    status = yield pycos.Pycos().send_file(cobj.client.location, cobj.result_file,
                                           overwrite=True, timeout=30)
    if status:
        print('Could not send %s to %s' % (cobj.result_file, cobj.client.location))
        cobj.result_file = None
    cobj.client.send(cobj)
    os.remove(cobj.data_file)
    os.remove(cobj.result_file)


# -- code below is executed locally --

# this generator function is used to create local task (at the client) to
# communicate with a remote task
def use_rtask(job_id, data_file, rtask, task=None):
    # send input file to rtask.location; this will be saved to dispycos process's
    # working directory
    if (yield pycos.Pycos().send_file(rtask.location, data_file, timeout=10)) < 0:
        print('Could not send input data to %s' % rtask.location)
        # terminate remote task
        rtask.send(None)
        raise StopIteration(-1)
    # send data to process
    obj = C(job_id, data_file, random.uniform(5, 8), task)
    if (yield rtask.deliver(obj)) != 1:
        print('Could not send input to %s' % rtask.location)
        raise StopIteration(-1)
    # rtask sends result to this task as message
    result = yield task.receive()
    if not result.result_file:
        print('Processing %s failed' % obj.i)
        raise StopIteration(-1)
    # rtask saves results file at this client, which is saved in pycos's
    # dest_path, not current working directory; alternately, pycos.Pycos can be started with
    # appropriate 'dest_path' parameter
    result_file = os.path.join(pycos.Pycos().dest_path, result.result_file)
    # move file to cwd
    target = os.path.join(os.getcwd(), os.path.basename(result_file))
    os.rename(result_file, target)
    print('    job %s output is in %s' % (obj.i, target))


# local process schedules computation, create remote tasks at dispycos servers and corresponding
# local tasks (with use_rtask) to communicate with rtasks
def client_proc(data_files, task=None):
    # schedule client with the scheduler; scheduler accepts one client
    # at a time, so if scheduler is shared, the client is queued until it
    # is done with already scheduled clients
    if (yield client.schedule()):
        raise Exception('Could not schedule client')

    for i in range(len(data_files)):
        data_file = os.path.basename(data_files[i])
        # create remote task
        rtask = yield client.rtask(rtask_proc)
        if isinstance(rtask, pycos.Task):
            # start local task to send input file and data to rtask (due to concurrency, many
            # use_rtask processes can be simultaneously sending data to different servers,
            # improving efficiency)
            pycos.Task(use_rtask, i, data_file, rtask)
        else:
            print('  ** job %s failed: %s' % (i, rtask))

    yield client.close()


if __name__ == '__main__':
    import sys, random, os, glob
    import pycos
    import pycos.netpycos
    from pycos.dispycos import *

    # pycos.logger.setLevel(pycos.Logger.DEBUG)
    # PyPI / pip packaging adjusts assertion below for Python 3.7+
    if sys.version_info.major == 3:
        assert sys.version_info.minor < 7, \
            ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
             (__file__, sys.version_info.major, sys.version_info.minor))

    data_files = glob.glob(os.path.join(os.path.dirname(sys.argv[0]), 'dispycos_client*.py'))
    if not data_files:
        raise Exception('No data files to process')
    if len(sys.argv) > 1:
        data_files = data_files[:int(sys.argv[1])]
    print(data_files)

    # unlike in earlier examples, rtask_proc is not sent with client (as it
    # is not included in 'components'; instead, it is sent each time a job is
    # submitted, which is a bit inefficient, but useful when using many functions for tasks
    client = Client([C])
    pycos.Task(client_proc, data_files)
