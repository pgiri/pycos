.. _intro:

************
Introduction 
************

Traditional methods of threads and synchronous I/O have some drawbacks,
especially with Python due to global interpreter lock (GIL). This approach is
not suited for large number of concurrent connections, known as `C10K problem
<https://en.wikipedia.org/wiki/C10k_problem>`_. In addition, threads in Python
have both memory and time overheads, especially on multi-core systems due to
context switches; see `Inside the Python GIL
<http://www.dabeaz.com/python/GIL.pdf>`_.

There are now many asynchronous frameworks that address these problems, that
usually provide event loop mechanism and callbacks. Programming with such
framework requires careful retooling of the application logic, similar to `using
GOTO statements <http://tirania.org/blog/archive/2013/Aug-15.html>`_.


Unlike with those frameworks, using pycos is very similar to the thread based
programming so there is almost no learning curve (as far as asynchronous
programming is concerned) - existing thread implementations can be converted to
pycos almost mechanically (although it cannot be automated). In fact, it is
easier to use pycos than threads, as locking is not required with pycos.

.. While not required to program with pycos, `Curious Course on Tasks and
   Concurrency <http://www.dabeaz.com/tasks/index.html>`_ offers details on
   generator functions and tasks.

For example, a simple client program to send messages using sockets with pycos
is::

   import sys, socket, pycos

   def client(host, port, n, task=None):
       sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
       # convert 'sock' to asynchronous socket
       sock = pycos.AsyncSocket(sock)
       yield sock.connect((host, port))
       data = 'client id: %d' % n
       yield sock.sendall(data)
       sock.close()

   # run 10 client tasks
   for i in range(10):
       pycos.Task(client, sys.argv[1], int(sys.argv[2]), i)

The pgrogram creates 10 tasks; each task process converts socket to asynchronous
socket with :ref:`AsyncSocket`, connects to server and sends a message. In the
tasks, socket I/O operations are called with *yield* (with *connect* and
*sendall* here). With these statements, the I/O operation is initiated, the task
is suspended and control goes to pycos's scheduler for scheduling other tasks as
well as processing I/O events. When an I/O operation is complete, the scheduler
resumes the suspended task with the results of the I/O operation. During the
time it takes to complete an I/O operation, the scheduler executes other tasks,
so many requests can be concurrently processed. Thus, with pycos *yield* is
similar to `system calls <https://en.wikipedia.org/wiki/System_call>`_ in
Unix-like kernels.

Note that the above program is similar to regular Python program, except for
using *yield* and creating processes with :ref:`Task` (instead of
:class:`threading.Thread`). Unlike with other asynchronous frameworks, the I/O
event loop in pycos is transparent - pycos's scheduler handles I/O events
automatically. If task method has ``task=None`` default argument, the task
constructor :ref:`Task` will set this ``task`` argument with the Task instance,
which can be used for calling methods in :ref:`Task` class (e.g., ``yield
task.sleep(2)`` to suspend execution for 2 seconds).

pycos package conists of following modules:

* [pycos](/pycos.rst) module provides API for tasks and asynchronous network
  programming. It includes following classes:

  * :ref:`Task` to create tasks, which are counterpart to threads in regular
    (synchronous) programs. Programming with tasks is very similar to
    programming with threads, except for a few differences. Tasks also support
    message passing for local or remote tasks to exchange information.

  * :py:class:`~pycos.Lock`, :py:class:`~pycos.RLock`, :py:class:`~pycos.Event`,
    :py:class:`~pycos.Semaphore`, :py:class:`~pycos.Condition` primitives
    provide asynchronous API similar to counterparts in threading
    module. Blokcing operations in these primitives should be used with *yield*.

  * :ref:`AsyncSocket` should be used to convert regular (synchronous) socket to
    asynchronous socket, as done in the example above. Blocking operations, such
    as *connect*, *send*, *recv* etc. should be used with *yield*.

  * :ref:`Channel` provides broadcasting (one-to-many, subscription based) API
    for message passing.

* :mod:`netpycos` module extends :ref:`Pycos` and :ref:`Task`, :ref:`Channel`
  etc. so the API works for remote tasks, channels etc. In addition, it provides
  :ref:`Location` used to refer to resource location and :ref:`RTI` to execute
  (predefined) tasks.

* :mod:`dispycos` module provides :ref:`Computation` to package computation
  fragments (code) and data to be scheduled for executing at remote server
  processes with :ref:`netpycos`. The client program can then schedule (remote)
  tasks to be executed. These tasks and client can use message passing to
  exchange data. The remote servers should be started with ``dispycosnode.py``
  program.

* :mod:`asyncfile` module provides :ref:`AsyncFile` and :ref:`AsyncPipe` for
  converting files and pipes to asynchronous API.  Blocking operations on these
  should be used with *yield*, as with sockets.

pycos has been tested with Python versions 2.7 and 3.2 under Linux, OS X and
Windows. Under Windows pycos uses IOCP only if `Python for Windows Extensions
<https://sourceforge.net/projects/pywin32/files/pywin32/>`_ (pywin32) is
installed. pywin32 works with Windows 32-bit and 64-bit.

.. include:: piwik.rst
