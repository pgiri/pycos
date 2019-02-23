# Asynchronous pipe example using chained Popen

import sys, subprocess, traceback, platform
import pycos
import pycos.asyncfile

# PyPI / pip packaging adjusts assertion below for Python 3.7+
if sys.version_info.major == 3:
    assert sys.version_info.minor < 7, \
        ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
         (__file__, sys.version_info.major, sys.version_info.minor))


def writer(apipe, inp, task=None):
    fd = open(inp)
    while True:
        line = fd.readline()
        if not line:
            break
        yield apipe.stdin.write(line.encode())
    apipe.stdin.close()


def line_reader(apipe, task=None):
    nlines = 0
    while True:
        try:
            line = yield apipe.readline()
        except:
            pycos.logger.debug('read failed')
            pycos.logger.debug(traceback.format_exc())
            break
        nlines += 1
        if not line:
            break
        print(line.decode())
    raise StopIteration(nlines)


# pycos.logger.setLevel(pycos.Logger.DEBUG)
if platform.system() == 'Windows':
    # asyncfile.Popen must be used instead of subprocess.Popen
    p1 = pycos.asyncfile.Popen([r'\cygwin64\bin\grep.exe', '-i', 'error'],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    p2 = pycos.asyncfile.Popen([r'\cygwin64\bin\wc.exe'], stdin=p1.stdout, stdout=subprocess.PIPE)
    async_pipe = pycos.asyncfile.AsyncPipe(p1, p2)
    pycos.Task(writer, async_pipe, r'\tmp\grep.inp' if len(sys.argv) == 1 else sys.argv[1])
    pycos.Task(line_reader, async_pipe)
else:
    p1 = subprocess.Popen(['grep', '-i', 'error'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(['wc'], stdin=p1.stdout, stdout=subprocess.PIPE)
    async_pipe = pycos.asyncfile.AsyncPipe(p1, p2)
    pycos.Task(writer, async_pipe, '/var/log/syslog' if len(sys.argv) == 1 else sys.argv[1])
    pycos.Task(line_reader, async_pipe)

    # alternate example:

    # p1 = subprocess.Popen(['tail', '-f', '/var/log/kern.log'], stdin=None, stdout=subprocess.PIPE)
    # p2 = subprocess.Popen(['grep', '--line-buffered', '-i', 'error'],
    #                       stdin=p1.stdout, stdout=subprocess.PIPE)
    # async_pipe = pycos.asyncfile.AsyncPipe(p2)
    # pycos.Task(line_reader, async_pipe)
