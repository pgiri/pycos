***********************************************
Asynchronous Concurrenct Programming (pycos)
***********************************************
.. module:: pycos
   :synopsis: asynchronous, concurrent programming

pycos provides API for `asynchronous
<http://en.wikipedia.org/wiki/Asynchronous_I/O>`_ and `concurrent
<http://en.wikipedia.org/wiki/Deterministic_concurrency>`_ programming with
`tasks <http://en.wikipedia.org/wiki/Task>`_ using Python's `generator
functions <http://www.python.org/dev/peps/pep-0342/>`_. Tasks are like
light weight threads - creating and running tasks is very
efficient. Moreover, unlike in the case of thread programming, a task
continues to run until it voluntarily gives up control (when *yield* is used),
so locking is not needed to protect critical sections.

Programs developed with pycos have same logic and structure as programs with
threads, except for a few syntactic changes. Although the API below has many
methods, most of them are for additional features of pycos (such as message
passing, hot swapping, monitoring etc.), and not needed for simple programs that
are similar to thread based programs. The differences compared to threaded
programming are:

   * Instead of creating threads, tasks should be created with
     :class:`Task`. The task function (first argument to :class:`Task`) should
     be a generator function (i.e., function with *yield* statements),

   * Sockets, pipes etc, should be converted to asynchronous versions with
     :ref:`AsyncSocket`, :ref:`AsyncPipe` etc.,

   * I/O operations, such as AsyncSocket's :meth:`~AsyncSocket.send`,
     :meth:`~AsyncSocket.receive`, :meth:`~AsyncSocket.accept`, blocking
     operations, such as task's :func:`sleep`, Event's :func:`wait`, etc., are
     implemented with generator methods; these should be used with *yield*
     (e.g., as ``data = yield async_sock.receve(1024)``),

   * pycos's locking primitives (:class:`pycos.Event`, :class:`pycos.Condition`,
     etc.) should be used in place of Python's threading counterparts with
     *yield* on blocking operations (e.g., as ``yield async_event.wait()``),

   * Task's :meth:`~Task.sleep` should be used in place of :meth:`time.sleep`
     (e.g., as ``yield task.sleep(2)``).

`Tasks <http://en.wikipedia.org/wiki/Task>`_ in pycos are essentially generator
functions that suspend execution when *yield* is used and are resumed by pycos's
scheduler (Pycos) after the asynchronous operation is complete. Usually *yield*
is used with an asynchronous call, such as socket's
:meth:`~AsyncSocket.connect`, :meth:`~AsyncSocket.send` or pipe's
:meth:`~AsyncPipe.read`, :meth:`~AsyncPipe.communicate`, waiting for a message
etc. With such statements, the asynchronous call is initiated and control goes
to scheduler which schedules another task, if one is ready to execute. When the
asynchronous operation is complete, the task that called the opreation becomes
ready to execute. Thus, the tasks in pycos are not strictly cooperative tasks
that pass control to each other, but each *yield* statement transfers control to
pycos's scheduler, which manages them. However, pycos supports message passing,
suspend/resume calls etc., so that tasks can cooperate in a way that is easier
to program and understand.

Unlike with threads, there is no forced preemption with tasks - at any time at
most one task is executing and it continues to execute until *yield* is
called. Thus, there is no need for locking critical sections with pycos.

pycos framework consists of :class:`Pycos` scheduler, :class:`Task` to create
tasks from generator functions, :class:`Channel` to broadcast messages,
:ref:`AsyncSocket` to convert regular synchronous sockets to asynchronous
(non-blocking) sockets, :ref:`AsyncPipe` for pipes, :ref:`AsyncFile` for files,
:ref:`Computation` and :meth:`dispycos_server` for distributed / parallel
computing, :class:`Lock` and :class:`RLock` for locking (although locking is not
required with pycos), :class:`Condition`, :class:`Event`, :class:`Semaphore`
primitives very similar to thread primitives (except for a few syntactic changes
noted above).

Examples
========

See :ref:`tut-tasks`, :ref:`tut-channels` and :ref:`tut-message-passing` in
tutorial for examples. There are many illustrative use cases in 'examples'
directory under where pycos module is installed.

Following is a brief description of the examples included relevant to this
section:

* :download:`examples/tasks.py` creates a number of tasks that each suspend
  execution for a brief period. The number of tasks created can be increased to
  thousands or tens of thousands to show pycos can scale well.

* :download:`examples/client_server.py` shows message passing (:meth:`send`
  and :meth:`receive` methods of tasks) between local client and server
  tasks. The remote version and local version are similar, except that remote
  versions register/locate tasks.

* :download:`examples/channel.py` uses broadcasting :ref:`Channel` to
  exchange messages in local tasks.

.. _Pycos:

Pycos scheduler
==================

:class:`Pycos` is a (singleton) scheduler that runs tasks similar to the way
operating system's scheduler runs multiple processes. It is initialized
automatically (for example, when a task is created), so for most purposes the
scheduler is transparent. The scheduler in :mod:`pycos` manages tasks, message
passing, I/O events, timeouts, wait/resume events etc., in a single concurrent
program; it doesn't provide distributed programming for message passing over
network. :mod:`netpycos` extends :class:`Pycos` with features supporting
distributed programming, remote execution of tasks etc.  If the scheduler
instance is needed, it can be obtained with either :meth:`Pycos` or
:meth:`Pycos.instance`.

Unlike in other asychronous frameworks, in pycos there is no explicit event loop
- the I/O events are processed by the scheduler and methods in
:ref:`AsyncSocket`, :ref:`AsyncPipe` etc. For example, :meth:`~AsyncSocket.recv`
method (which must be used with *yield*) sets up an internal function to execute
when the socket has data to read and suspends the caller task. The scheduler can
execute any other tasks that are ready while the I/O operation is pending. When
the data has been read, the suspended task is resumed with the data read so that
:ref:`AsyncSocket`\'s :meth:`~AsyncSocket.recv` works just as
:meth:`socket.recv`, except for using *yield*. Thus, programming with pycos is
very similar to that with threads, except for using *yield* with certain
methods.

.. class:: Pycos()

   Creates and returns singleton scheduler. If a scheduler instance has already
   been created (such as when a task was created), a new instance won't be
   created. :mod:`netpycos` extends ``Pycos`` for distributed programming and
   the constructor there has various options to customize.

   The scheduler following methods:
      
   .. method:: instance()

      This static method returns instance of ``Pycos`` scheduler; use it as
      ``scheduler = Pycos.instance()``. If the instance has not been started
      (yet), it creates one and returns it.

   .. method:: cur_task()

      This static method returns task (instance of :ref:`Task`) being executed;
      use it as ``task = Pycos.cur_task()``. As mentioned below, if task's
      generator function has ``task=None`` parameter, :ref:`Task` constructor
      initializes it to the task instance (which is a way to document that
      method is used for creating tasks).

   .. method:: join()

       .. note:: This method must be called from (main) thread only - calling
          from a task will deadlock entire task framework.

     Waits for all scheduled non-daemon tasks to finish. After join returns,
     more tasks can be created (which are then added to scheduler).

   .. method:: terminate()

       .. note:: This method must be called from (main) thread only - calling
          from a task will deadlock entire task framework.

     Terminates all scheduled tasks and then the scheduler itself. If necessary,
     a new scheduler instance may be created with :meth:`Pycos` or
     :meth:`Pycos.instance`.

The scheduler runs in a separate thread from user program. The scheduler
terminates when all non-daemon tasks are terminated, similar to Python's
threading module.

.. _Task:

Task
=========

pycos's ``Task`` class creates tasks (light weight processes). Tasks are similar
to threads in regular Python programs, except for a few differences as noted
above.

.. class:: Task(target[, arg1, arg2, ...])

   Creates a task, where *target* is a generator function (a function with
   *yield* statements), *arg1*, *arg2* etc. are arguments or keyword arguments
   to *target*. If *target* generator function has ``task=None`` keyword
   argument, Task constructor replaces ``None`` with the instance of Task
   created, so task can use this to invoke methods in Task class (see
   below). Alternately, the instance can be obtained with the static method
   ``task = Pycos.cur_task()``.

   Consider the generator function (where *sock* is asynchronous socket and all
   statements are asynchronous, so all are used with *yield*)::

      def get_reply(sock, msg, task=None):
          yield sock.sendall(msg)
          yield task.sleep(1)
          reply = yield sock.recv(1024)

   A task for processing above function can be created with, for example,
   ``Task(get_reply, conn, "ping")``. **Task** constructor creates task with the
   method *get_reply* with parameters ``sock=conn``, ``msg="ping"`` and *task*
   set to the just created task instance. (If *task=None* argument is not used,
   the task instance can be obtained with ``task = Pycos.cur_task()``.) The task
   is then added to Pycos scheduler so it executes concurrently with other tasks
   - there is no need to start it explicitly, as done with threads. Note that
   generator functions cannot use ``return`` statement. With pycos a return
   statement such as ``return v`` can be replaced with ``raise
   StopIteration(v)``. If a generator/task does not use :exc:`StopIteration`,
   then the last value yielded in the generator becomes the return value. Thus,
   in the example above *get_reply* does not use :exc:`StopIteration`, so buffer
   received (in the last *yield*) is equivalent to return value of *get_reply*.

   Blocking operations, such as :meth:`socket.recv`, :meth:`socket.connect`, are
   implemented as generator functions in asynchronous implementation of socket
   :ref:`AsyncSocket`. These functions simply initiate the operation; *yield*
   should be used with them (as in the example above) so scheduler can run other
   eligible tasks while the operation is pending. Calling these methods without
   *yield* simply returns generator function itself, instead of result of the
   method call. So care must be taken to use *yield* when calling generator
   functions. Using *yield* where it is not needed is not an error; e.g.,
   :meth:`resume` method of tasks can be used without *yield*, but when used
   with *yield*, the caller gives control to scheduler which may execute resumed
   task right away. In rest of the documentation, methods that need to be called
   with *yield* are noted so.

   In rest of the documentation we follow the convention of using ``task=None``
   keyword argument in generator methods and use *task* variable to refer to the
   task, i.e., instance of **Task**, executing the generator function. This
   variable can be used to invoke methods of **Task**, use it in other tasks,
   for example, to send messages to it, or wake up from sleep etc. A task has
   following methods:

   .. method:: suspend(timeout=None, alarm_value=None)
               sleep(timeout=None, alarm_value=None)

      .. note:: This method must always be used with *yield* as
	 ``yield task.sleep()``.

      Suspends task *task* until *timeout*. If *timeout* is a positive number
      (float or int), the scheduler suspends execution of task until that many
      seconds (or fractions of second). If *timeout* is ``None``, the task is
      not woken up by the scheduler - some other task needs to resume it. The
      value yielded by this method is the value it is resumed with or
      *alarm_value* if resumed by the scheduler due to timeout.  If *timeout=0*,
      this method returns *alarm_value* without suspending the task.

      For example, if a task executes ``v = yield task.sleep(2.9)``, it is
      suspended for 2.9 seconds. If before timeout, another task wakes up this
      task (with :meth:`resume` method) with a value, *v* is set to that
      value. Otherwise, after 2.9 seconds, this task is resumed with ``None``
      (default *alarm_value*) so *v* is set to ``None`` and task continues
      execution. During the time task is suspended, scheduler executes other
      scheduled tasks. Within a task ``task.sleep`` (or ``task.suspend``) must
      be used (with *yield*) instead of ``time.sleep``; calling ``time.sleep``
      will deadlock entire task framework.

   .. method:: resume(update=None)
               wakeup(update=None)

      Wakes up (suspended) task ``task``. As explained above, the suspended task
      gets *update* (any Python object) as the value of yield statement that
      caused it to suspend. If sleep/resume synchronization is needed (so that
      resume waits until specific suspend is ready to receive), Event locking
      primitive can be used so that resuming task waits on an event variable and
      suspending task sets the event before going to sleep.

   .. method:: send(msg)

      Sends message *msg* (any Python object) to the task on which this method
      is invoked. If the task is currently waiting for messages (with
      :meth:`receive`), then it is resumed with *msg*. If it is not currently
      waiting for messages, the message is queued so that next :meth:`receive`
      returns the message without suspending.

      Message can be any Python object when sender and recipients are in same
      program/pycos (i.e., messages are not sent over network). However, when
      sender and reecipient are on different pycos instances (over network), the
      messages must be serializable at the sender and unserializable at the
      receiver. If message includes any objects that have unserializable
      attributes, then their classes have to provide :func:`__getstate__` method
      to serialize the objects, and the remote program should have
      :func:`__setstate__` for those classes; see `Pickle protocol
      <https://docs.python.org/2/library/pickle.html#the-pickle-protocol>`_.

      If the recipient is in a remote pycos, :meth:`send` simply queues messages
      for transfer over network. A daemon task in pycos transfers the messages
      in the order they are queued. This task, by default, may transfer each
      message with a new connection. As creating sockets and making connections
      is expensive, it may be rather inefficient, especially if messages are
      sent frequently. See :meth:`peer` method in :doc:`netpycos` for specifying
      that messages to peers should be sent as stream, using same connection.

   .. method:: deliver(msg, timeout=None)

      .. note:: This method must always be used with *yield* as
         ``recvd = yield rtask.deliver(msg)``.

      Similar to :meth:`send` except that this method must be used with *yield*
      and it returns status of delivering the message to the recipient. If it is
      1, the message has been successfully placed in recipient task's message
      queue (when recipient calls :meth:`receive`, it gets the queued messages
      in the order they are received). If *timeout* is given and message
      couldn't be delivered before timeout, the return value is 0. If *timeout*
      is ``None``, delivery will not timeout. For local tasks (i.e., tasks
      executing in the same program) *timeout* has no effect - if the recipient
      is valid, message will be delivered successfully. However, if the
      recipient is a remote task (see :doc:`netpycos`), network delays /
      failures may cause delivery to be delayed or delivery may fail (i.e.,
      there is a possibility of delivery waiting forever); to avoid such issues,
      appropriate *timeout* may be used.

   .. method:: receive(timeout=None, alarm_value=None)
               recv(timeout=None, alarm_value=None)

      .. note:: This method must always be used with *yield* as
	 ``msg = yield task.receive()``.

      Returns earliest queued message if there are pending messages, or suspends
      ``task`` until either a message is sent to it or *timeout* seconds
      elapse. If called with *timeout=0*, this method will not suspend the task
      - it will return either earliest queued message if available or
      *alarm_value*.

      *recv* is synonym for *receive*.

   .. method:: set_daemon(flag=True)

      Marks the task a daemon (task that never terminates) if *flag* is
      ``True``. Similar to threading module, Pycos scheduler waits for all
      non-daemon tasks to terminate before exiting. The daemon status can be
      toggled by calling :meth:`set_daemon` with *flag* set to ``True`` or
      ``False``.

   .. method:: hot_swappable(flag)

      Marks if the task's generator function can be replaced. This method can be
      used to set (with *flag=True*) or clear (with *flag=False*) the flag. With
      hot swapping, a task's code can be updated (to new functionality) while
      the application is running.

   .. method:: hot_swap(target [, arg1, arg2, ...])

      Requests Pycos to replace task's generator function with *target([arg1,
      arg2, ...])*. Pycos then throws :exc:`HotSwapException` in the task when:

	 * task indicated it can handle hot swap (i.e., last called ``hot_swappable``
	   with *flag=True*),
	 * it is currently executing at top-level in the call stack (i.e., has
	   not called other generator functions), and
	 * has no pending asynchronous operations (socket I/O, tasks scheduled with
	   AsyncThreadPool, etc.).

      The new generator is set as args[0] of :exc:`HotSwapException`, so the
      task can inspect new generator, if necessary, and can do any preparation
      for hot swapping, e.g., saving state (perhaps by sending state as a
      message to itself which can be retrieved in the new generator with
      :meth:`receive`), or even ignore hot swap request. If/when it is ready for
      swap, it must re-raise the same :exc:`HotSwapException` (with the new
      generator as args[0]). This causes Pycos to close current generator
      function, replace it with the new generator function and schedule new
      generator for execution (from the beginning). Any messages (i.e., resume
      updates) queued in the previous generator are not reset, so new generator
      can process queued messages (e.g., use :meth:`receive` in a loop with
      *timeout=0* until :meth:`receive` returns *alarm_value*). Note that
      :meth:`hot_swap` changes generator function of a particular task for which
      it is called. If there are many tasks using that generator function,
      :meth:`hot_swap` may be called for each such task.

   .. method:: monitor(observe)

      .. note:: This method must always be used with *yield* as
	 ``v = yield task.monitor(observe)``.

      Sets ``task`` as the monitor of task *observe*. Then, when the task
      *observe* is finished (either because task's generator function finished
      exceution or was terminated by Pycos because of an uncaught exception),
      Pycos sends the status as message with :exc:`MonitorException` to
      ``task``. :exc:`MonitorException` args[0] is set to the affected task
      *observe* and args[1] is set to the exception tuple: If *observe* finished
      execution, the tuple is a pair, with first element set to (type)
      :exc:`StopIteration` and second element instance of :exc:`StopIteration`
      with the last value yielded by *observe*, and if *observe* was terminated
      due to uncaught exception, the tuple will have either 2 or 3 elements,
      with first element set to the type of exception, second element set to the
      uncaught exception, and third element set to trace, if available. The
      monitor task can inspect :exc:`MonitorException` and possibly restart the
      affected task (see below). A task can be monitored by more than one
      monitor, and a monitor can monitor more than one task. This method must
      always be used with *yield*.

   .. method:: throw(*args)

      Throws exception *\*args* to task (at the point where it is currently
      executing).

   .. method:: terminate()

      Terminates the task. This is useful, for example, to terminate server
      tasks that otherwise never terminate.

   .. method:: value()

      .. note:: This method must be called from a thread, not a task.

      Returns the last value yielded by the task, possibly waiting until task
      terminates. This method should not be called from a task - this will cause
      entire task framework to deadlock.  This method is meant for main thread
      in the user program to wait for (main) task(s) it creates.

   .. method:: finish()

      .. note:: This method must always be used in a task with
	 *yield* as ``v = yield other.finish()``.

      Returns the last value yielded by the task ``other``, possibly waiting
      until it terminates.

   Faults in (local or remote) tasks can be detected with :meth:`monitor`, and
   fault-toerant tasks can be developed with :meth:`hot_swap`.

Locking Primitives
==================

.. class:: Lock
.. class:: RLock
.. class:: Semaphore
.. class:: Event
.. class:: Condition

.. note:: With pycos locking is not needed, as there is no forced preemption -
   at any time at most one task is executing and the control is transfered to
   the scheduler only when *yield* statement is encountered. (In fact, the
   implementation of asynchronous locking primitives in pycos updates lists and
   counters without locking.) So with pycos :class:`Lock` and :class:`RLock` are
   optional.

pycos provides asynchronous implementations of :class:`Lock`, :class:`RLock`,
:class:`Semaphore`, :class:`Event` and :class:`Condition` primitives. They are
similar to versions in threading module. Any operation that would block in
threading module must be called with *yield* appropriately. For example,
acquiring a lock is a blocking operation, so it should be invoked as ``yield
lock.acquire()``. Similarly, Event's wait method or Condition's wait method must
be used as ``yield event.wait()`` or ``yield condition.wait()``. For example,
Condition variable cv in a client should be used as (compare to example at
`threading module
<http://docs.python.org/library/threading.html#condition-objects>`_)::

   while True:
     yield cv.acquire()
     while not an_item_is_available():
         yield cv.wait()
     get_an_available_item()
     cv.release()

See documentation strings in ``pycos`` module for more details on which methods
should be used with *yield* and which methods need not be.

.. _Channel:

Channel
=======

Channel is a broadcast mechanism with which tasks can exchange
messages. Messages sent to Channel are sent to its subscribers
(recipients). While a message can be sent one-to-one with task's
:meth:`~Task.send` or :meth:`~Task.deliver` methods on the receiving task,
channels can be used to broadcast a message so all its subscribers get that
message.

.. class:: Channel(name, transform=None)

   Creates channel with *name*, which must be unique. If *transform*, is given,
   it must a function that is called before a message is sent to
   subscribers. The function is called with name of the channel and the
   message. It should return transformed message or ``None``. If ``None`` is
   returned, the message is dropped - subscribers will not receive the
   message. Otherwise, transformed message is sent to subscribers.

   A channel has following methods.

   .. method:: subsribe(subscriber, timeout=None)

      .. note:: This method must be used with *yield* as
	 ``yield channel.subscribe(task)``

      Subscribes *subscriber* (a task or even another channel) to the
      channel. Any messages sent to the channel are then sent to each
      subscriber; i.e., messages are broadcast to all subscribers. It is
      possible to chain or create hierarchical channels with channels
      subscribing to other channels. If *timeout* is a positive number, the call
      fails if subscription is not successfull (e.g., the channel couldn't be
      located) before that many seconds.

   .. method:: send(message)

      Calls *transform* function of the channel (see above) if it has one. If
      the function returns ``None``, the message is ignored. Otherwise the
      message is sent to current subscribers. Messages sent over a channel are
      queued (buffered) at receiving tasks.  A task *task*, for example, that
      has subscribed to the channel can receive messages with ``msg = yield
      task.receive()``.

   .. method:: deliver(message, timeout=None, n=0)

      .. note:: This method must be used with *yield* as
	 ``recvd = yield channel.deliver(msg)``

      Similar to :meth:`send`, except that it waits until at least *n*
      subscribers are subscribed. It returns total number of end-point
      recipients (tasks) the message has been delivered to - in case of
      heirarchical channels, it is the sum of recipients of all the
      channels. This may be less than *n* (e.g., delivering message to a
      subscriber may fail, due to network error), or more (e.g., there are more
      subscribers, or some subscribers are channels with more than one
      subscriber). If *n* is 0, then the message will be delivered to all
      current subscribers. In any case, *timeout* is maximum amount of time in
      seconds (or fraction of second) for delivering the message. Thus, for
      example, if *timeout* occurs before *n* subscribers are subscribed to the
      channel, the method returns 0.

   .. method:: unsubsribe(subscriber, timeout=None)

      .. note:: This method must be used with *yield* as
	 ``yield channel.unsubscribe(task)``

      Unsubscribes the subscriber (task or another channel), so future messages
      to the channel are not sent to that subscriber. If *timeout* is a positive
      number, it is the number of seconds for unsubscribe request to complete.

   .. method:: close()

      Close the channel. The channel can't be used for message passing after
      closing.

   .. method:: set_transform(transform)

      Set/change *transform* as the method to call when message is sent to this
      channel. See :class:`Channel` constructor and :meth:`~Channel.send`.

Message Passing
===============

Task's :meth:`~Task.send`, :meth:`~Task.receive` and :meth:`~Task.deliver` offer
one-to-one message passing and Channel's :meth:`~Channel.send` and
:meth:`~Channel.deliver` offer one-to-many / broadcast message passing.

pycos delivers messages in the order they have been sent with either one-to-one
or broadcast message passing (i.e., with either *send* or *deliver* methods of
tasks or channels); i.e., pycos guarantees temporal order of messages.

AsyncThreadPool
===============

pycos framework and all tasks run in a single thread. It implements concurrency
(running more than one task) by interleaving tasks - suspending a task that
is waiting for some event and running a task that is ready to execute. All the
blocking operations, such as sending/receiving data (sockets, message passing),
or sleeping, are implemented with generator funtions that schedule the operation
and suspend the task. However, pycos framework doesn't implement every blocking
operation. Sometimes, it is necessary to use functions in other modules that
block the thread until the operation is complete. For example, reading standard
input will block the thread until the read method is complete. If such functions
are used in a task, entire pycos framework and all tasks are blocked; i.e.,
pycos scheduler itself is blocked, so even if there are other tasks eligible to
run, they won't be executed. AsyncThreadPool class can be used to run such
blocking functions in separate threads so pycos itself is not affected by them.


.. class:: AsyncThreadPool(num_threads)

   Creates a pool with given number of threads. When a blocking function is
   scheduled, an available thread in the pool is used to execute that
   function. More threads will allow more blocking functions to be running
   simultaneously, but take more system resources.

   .. method:: async_task(target, \*args, \*\*kwargs)

      .. note:: This method must be used with *yield* as
	 ``val = yield pool.async_task(target, args, kwargs)``

      Schedules given *target* function with arguments *\*args* and keyword
      arguments *\*\*kwargs* for execution with a thread in the pool. If all
      threads are currently executing other functions, then the function will be
      executed when a thread becomes available (i.e., done with currently
      executing function).

      The value returned by this method is the value returned by the function.

   .. method:: join()

      Waits for all scheduled blocking functions to finish. This method should
      be called from main thread, not from any task, as this method is a
      blocking operation.

   .. method:: terminate()

      Waits for all scheduled blocking functions to finish and then terminate
      the threads; the pool can no longer be used for scheduling tasks. This
      method should be called from main thread, not from any task, as this
      method is a blocking operation.


See :download:`examples/chat_client.py` which uses thread pool (with 1 thread)
to execute ``sys.stdin.readline`` (a bloking function).

.. include:: piwik.rst
