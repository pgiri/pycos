# Asynchronous pipe example with "communicate" method that is similar
# to Popen's "communicate". Same example is used to show how custom
# write/read processes can be provided to feed / read from the
# asynchronous pipe

# argv[1] must be a text file

import sys, os, subprocess, platform
import pycos
import pycos.asyncfile

# PyPI / pip packaging adjusts assertion below for Python 3.7+
if sys.version_info.major == 3:
    assert sys.version_info.minor < 7, \
        ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
         (__file__, sys.version_info.major, sys.version_info.minor))


def communicate(input, task=None):
    if platform.system() == 'Windows':
        # asyncfile.Popen must be used instead of subprocess.Popen
        pipe = pycos.asyncfile.Popen([r'\cygwin64\bin\sha1sum.exe'],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    else:
        pipe = subprocess.Popen(['sha1sum'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    # convert pipe to asynchronous version
    async_pipe = pycos.asyncfile.AsyncPipe(pipe)
    # 'communicate' takes either the data or file descriptor with data
    # (if file is too large to read in full) as input
    input = open(input)
    stdout, stderr = yield async_pipe.communicate(input)
    print('communicate sha1sum: %s' % stdout)


def custom_feeder(input, task=None):
    def write_proc(fin, pipe, task=None):
        while True:
            data = yield os.read(fin.fileno(), 8*1024)
            if not data:
                break
            n = yield pipe.write(data, full=True)
            assert n == len(data)
        fin.close()
        pipe.stdin.close()

    def read_proc(pipe, task=None):
        # output from sha1sum is small, so read until EOF
        data = yield pipe.stdout.read()
        pipe.stdout.close()
        raise StopIteration(data)

    if platform.system() == 'Windows':
        # asyncfile.Popen must be used instead of subprocess.Popen
        pipe = pycos.asyncfile.Popen([r'\cygwin64\bin\sha1sum.exe'],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    else:
        pipe = subprocess.Popen(['sha1sum'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    async_pipe = pycos.asyncfile.AsyncPipe(pipe)
    reader = pycos.Task(read_proc, async_pipe)
    writer = pycos.Task(write_proc, open(input), async_pipe)
    stdout = yield reader.finish()
    print('     feeder sha1sum: %s' % stdout)


# pycos.logger.setLevel(pycos.Logger.DEBUG)
# simpler version using 'communicate'
task = pycos.Task(communicate, sys.argv[1] if len(sys.argv) > 1 else sys.argv[0])
task.value()  # wait for it to finish

# alternate version with custom read and write processes
pycos.Task(custom_feeder, sys.argv[1] if len(sys.argv) > 1 else sys.argv[0])
