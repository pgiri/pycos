pycos
######

pycos is a Python framework for asynchronous, :doc:`concurrent </pycos>`,
:doc:`network </network>`, :doc:`distributed programming </netpycos>` and
:doc:`distributed computing </dispycos>`, using generator functions,
asynchronous completions and message passing. pycos can be used to create tasks
with generator functions, similar to the way threads are created with functions
with Python's `threading module
<https://docs.python.org/2.7/library/threading.html>`_. Programs developed with
pycos have **same logic and structure** as programs with threads, except for a
few syntactic changes - mostly using *yield* with asynchronous completions that
give control to pycos's scheduler, which interleaves executions of generators,
similar to the way an operating system executes multiple processes.

Unlike threads, creating processes (tasks) with pycos is very
efficient. Moreover, with pycos context switch occurs only when tasks use
*yield* (typically with an asychronous call), so there is no need for locking
and there is no overhead of unnecessary context switches.

pycos works with Python versions 2.7+ and 3.1+. It has been tested with Linux,
Mac OS X and Windows; it may work on other platforms, too.

Features
--------

* No callbacks or event loops! No need to lock critical sections either,

* Efficient polling mechanisms epoll, kqueue, /dev/poll, Windows I/O Completion
  Ports (IOCP) for high performance and scalability,

* Asynchronous (non-blocking) :doc:`sockets </network>` and :doc:`pipes
  </asyncfile>`, for concurrent processing of I/O,

* SSL for security,

* Asynchronous locking primitives similar to Python threading module,

* Asynchronous timers and timeouts,

* `Message passing <http://en.wikipedia.org/wiki/Message_passing>`_ for (local
  and remote) tasks to exchange messages one-to-one with `Message Queue Pattern
  <http://en.wikipedia.org/wiki/Message_queue>`_ or through broadcasting
  channels with `Publish-Subscribe Pattern
  <http://en.wikipedia.org/wiki/Publish/subscribe>`_,

* `Location transparency <http://en.wikipedia.org/wiki/Location_transparency>`_
  with naming and locating (local and remote) resources,

* Monitoring and restarting of (local or remote) tasks, for fault detection and
  fault-tolerance,

* Distributing computation components (code and data) to execute
  :doc:`dispycos`, for wide range of use cases, covering `SIMD, MISD, MIMD
  <https://en.wikipedia.org/wiki/Flynn%27s_taxonomy>`_ system architectures at
  the process level, and `web interface
  <http://pycos.sourceforge.net/dispycos.html#client-browser-interface>`_ to
  monitor cluster/application status/performance; `in-memory processing
  <https://en.wikipedia.org/wiki/In-memory_processing>`_, data streaming,
  real-time (live) analytics and :ref:`Cloud` are supported as well,

* Remote execution of tasks for distributed programming with Remote Task
  Invocation :ref:`RCI` and message passing,

* Hot-swapping of task functions, for dynamic system reconfiguration,

* Thread pools with asynchronous task completions, for executing synchronous
  tasks, e.g., external library calls, such as reading standard input.


Dependencies
------------

pycos is implemented with standard modules in Python.

If `psutil <https://pypi.python.org/pypi/psutil>`_ is available on nodes, node
availability status (CPU, memory and disk) is sent in status messages, and shown
in web browser so node/application performance can be monitored.

Under Windows efficient polling notifier I/O Completion Ports (IOCP) is
supported only if `pywin32
<http://sourceforge.net/projects/pywin32/files/pywin32/>`_ is available;
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
* `Project page <http://pycos.sourceforge.io>`_.
* `GitHub (Code Repository) <https://github.com/pgiri/pycos>`_.
