pycos
######

    .. note:: Full documentation for pycos is now available at `pycos.org
              <https://pycos.org>`_.

`pycos <https://pycos.org>`_ is a Python framework for asynchronous, concurrent, network /
distributed programming, distributed computing with *very* light weight tasks and message passing.

Unlike with other asynchronous frameworks, programs developed with pycos have
**same logic and structure** as programs with threads, except for a few
syntactic changes - mostly using *yield* with asynchronous completions that give
control to pycos's scheduler, which interleaves executions of tasks, similar to
the way an operating system executes multiple processes. In addition, pycos has
many additional features, including message passing for communication,
distributed computing/programming etc.

Unlike threads, creating tasks with pycos is very efficient. Moreover, with
pycos context switch occurs only when tasks use *yield* (typically with an
asychronous call), so there is no need for locking and there is no overhead of
unnecessary context switches.

pycos works with Python versions 2.7+ and 3.1+ on Linux, Mac OS X and Windows; it may
work on other platforms (e.g., FreeBSD and other BSD variants) too.

Features
--------

* No callbacks or event loops! No need to lock critical sections either,

* Efficient polling mechanisms epoll, kqueue, /dev/poll, Windows
  I/O Completion Ports (IOCP) for high performance and
  scalability,

* Asynchronous (non-blocking) sockets and pipes, for concurrent
  processing of I/O,

* SSL for security,

* Asynchronous locking primitives similar to Python threading module,

* Asynchronous timers and timeouts,

* `Message passing <http://en.wikipedia.org/wiki/Message_passing>`_
  for (local and remote) tasks to exchange messages one-to-one
  with `Message Queue Pattern
  <http://en.wikipedia.org/wiki/Message_queue>`_ or through
  broadcasting channels with `Publish-Subscribe Pattern
  <http://en.wikipedia.org/wiki/Publish/subscribe>`_,

* `Location transparency
  <http://en.wikipedia.org/wiki/Location_transparency>`_ with naming
  and locating (local and remote) resources,

* Remote Pico Service (RPS) for defining services that remote clients can run as tasks
  (with possibly message passing to communicate).

* Distributing computation components (code and data) for execution of
  distributed communicating processes, for wide range of use cases, covering
  `SIMD, MISD, MIMD <https://en.wikipedia.org/wiki/Flynn%27s_taxonomy>`_ system
  architectures at the process level, `web interface
  <https://pycos.org/dispycos.html#client-browser-interface>`_ to
  monitor cluster/application status/performance; `in-memory processing
  <https://en.wikipedia.org/wiki/In-memory_processing>`_, data streaming,
  real-time (live) analytics and cloud computing are supported as well,

* Monitoring and restarting of (local or remote) tasks, for fault detection
  and fault-tolerance,

* Hot-swapping of task functions, for dynamic system reconfiguration.

* Thread pools with asynchronous task completions, for executing (external)
  synchronous tasks, such as reading standard input.

Dependencies
------------

pycos is implemented with standard modules in Python.

If `psutil <https://pypi.python.org/pypi/psutil>`_ is available on nodes, node
availability status (CPU, memory and disk) is sent in status messages, and shown
in web browser so node/application performance can be monitored.

Under Windows efficient polling notifier I/O Completion Ports (IOCP) is
supported only if `pywin32 <https://github.com/mhammond/pywin32>`_ is available;
otherwise, inefficient *select* notifier is used.

Installation
------------
To install pycos, run::

   python -m pip install pycos

Authors
-------
* Giridhar Pemmasani

Links
-----
* Documentation is at `pycos.org`_.
* `GitHub (Code Repository) <https://github.com/pgiri/pycos>`_.
