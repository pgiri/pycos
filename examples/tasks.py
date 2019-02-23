#!/usr/bin/env python

# program for creating tasks (asynchronous concurrent programming); see
# http://pycos.sourceforge.io/tutorial.html for details.

import random, time
import pycos


def pycos_proc(n, task=None):
    s = random.uniform(0.5, 3)
    print('%f: process %d sleeping for %f seconds' % (time.time(), n, s))
    yield task.sleep(s)
    print('%f: process %d terminating' % (time.time(), n))


# create 10 clients
for i in range(10):
    pycos.Task(pycos_proc, i)
