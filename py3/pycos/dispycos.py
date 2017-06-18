#!/usr/bin/python3

"""
This file is part of pycos project. See https://pycos.sourceforge.io for details.

This module provides API for creating distributed communicating
processes. 'Computation' class should be used to package computation components
(Python generator functions, Python functions, files, classes, modules) and then
schedule runs that create remote tasks at remote server processes running
'dispycosnode.py'.

See 'dispycos_client*.py' files in 'examples' directory for various use cases.
"""

import os
import sys
import inspect
import hashlib
import collections
import time
import socket
import shutil
import operator
import functools
import re

import pycos.netpycos as pycos
from pycos import Task, SysTask, logger
import pycos.dispycos

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2014-2015 Giridhar Pemmasani"
__license__ = "Apache 2.0"
__url__ = "https://pycos.sourceforge.io"

__all__ = ['Scheduler', 'Computation', 'DispycosStatus', 'DispycosTaskInfo',
           'DispycosNodeInfo', 'DispycosNodeAvailInfo', 'DispycosNodeAllocate']

MsgTimeout = pycos.MsgTimeout
MinPulseInterval = 10
MaxPulseInterval = 10 * MinPulseInterval

# status about nodes / servers are sent with this structure
DispycosStatus = collections.namedtuple('DispycosStatus', ['status', 'info'])
DispycosTaskInfo = collections.namedtuple('DispycosTaskInfo', ['task', 'args', 'kwargs',
                                                               'start_time'])
DispycosNodeInfo = collections.namedtuple('DispycosNodeInfo', ['name', 'addr', 'cpus', 'platform',
                                                               'avail_info'])


class DispycosNodeAvailInfo(object):
    """Node availability status is indicated with this class.  'cpu' is
    available CPU in percent in the range 0 to 100. 0 indicates node is busy
    executing tasks on all CPUs and 100 indicates node is not busy at all.
    """

    def __init__(self, location, cpu, memory, disk, swap):
        self.location = location
        self.cpu = cpu
        self.memory = memory
        self.disk = disk
        self.swap = swap


class DispycosNodeAllocate(object):
    """Allocation of nodes can be customized by specifying 'node_allocations' of
    Computation with DispycosNodeAllocate instances.

    'ip_addr' must be a regular expression, e.g., "192\.168\.1\..*" (note that
    "." without escape stands for "any" character), 'cpus' must be minimum
    number of servers running on that node, 'memory' is available memory in
    bytes, 'disk' is available disk space (on the partition where dispycosnode
    servers are running from), and 'platform' is regular expression to match
    output of"platform.platform()" on that node, e.g., "Linux.*x86_64" to accept
    only nodes that run 64-bit Linux.
    """

    def __init__(self, node, platform='', cpus=0, memory=0, disk=0):
        if node.find('*') < 0:
            try:
                info = socket.getaddrinfo(node, None)[0]
                ip_addr = info[4][0]
                if info[0] == socket.AF_INET6:
                    ip_addr = re.sub(r'^0*', '', ip_addr)
                    ip_addr = re.sub(r':0*', ':', ip_addr)
                    ip_addr = re.sub(r'::+', '::', ip_addr)
                node = ip_addr
            except:
                node = ''

        if node:
            self.ip_rex = node.replace('.', '\\.').replace('*', '.*')
        else:
            logger.warning('node "%s" is invalid', node)
            self.ip_rex = ''
        self.platform = platform.lower()
        self.cpus = cpus
        self.memory = memory
        self.disk = disk

    def allocate(self, ip_addr, name, platform, cpus, memory, disk):
        """When a node is found, scheduler calls this method with IP address,
        name, CPUs, memory and disk available on that node. This method should
        return a number indicating number of CPUs to use. If return value is 0,
        the node is not used; if the return value is < 0, this allocation is
        ignored (next allocation in the 'node_allocations' list, if any, is
        applied).
        """
        if not re.match(self.ip_rex, ip_addr):
            return -1
        if (self.platform and not re.search(self.platform, platform)):
            return -1
        if ((self.memory and memory and self.memory > memory) or
            (self.disk and disk and self.disk > disk)):
            return 0
        if self.cpus > 0:
            if self.cpus > cpus:
                return 0
            return self.cpus
        elif self.cpus == 0:
            return 0
        else:
            cpus += self.cpus
            if cpus < 0:
                return 0
            return cpus


class Computation(object):
    """Packages components to distribute to remote pycos schedulers to create
    (remote) tasks.
    """

    def __init__(self, components, pulse_interval=(5*MinPulseInterval), node_allocations=[],
                 status_task=None, node_setup=None, server_setup=None,
                 disable_nodes=False, disable_servers=False, peers_communicate=False):
        """'components' should be a list, each element of which is either a
        module, a (generator or normal) function, path name of a file, a class
        or an object (in which case the code for its class is sent).

        'pulse_interval' is interval (number of seconds) used for heart beat
        messages to check if client / scheduler / server is alive. If the other
        side doesn't reply to 5 heart beat messages, it is treated as dead.
        """

        if pulse_interval < MinPulseInterval or pulse_interval > MaxPulseInterval:
            raise Exception('"pulse_interval" must be at least %s and at most %s' %
                            (MinPulseInterval, MaxPulseInterval))
        if ((not isinstance(node_allocations, list)) or
            any(not isinstance(_, (DispycosNodeAllocate, str)) for _ in node_allocations)):
            raise Exception('"node_allocations" must be list of DispycosNodeAllocate instances')
        if status_task and not isinstance(status_task, Task):
            raise Exception('status_task must be Task instance')
        if node_setup and not inspect.isgeneratorfunction(node_setup):
            raise Exception('"node_setup" must be a task (generator function)')
        if server_setup and not inspect.isgeneratorfunction(server_setup):
            raise Exception('"server_setup" must be a task (generator function)')
        if (disable_nodes or disable_servers) and not status_task:
            raise Exception('status_task must be given when nodes or servers are disabled')

        if not isinstance(components, list):
            components = [components]

        self._code = ''
        self._xfer_funcs = set()
        self._xfer_files = []
        self._auth = None
        self.scheduler = None
        self._pulse_task = None
        self._pulse_interval = pulse_interval
        self._node_allocations = [node if isinstance(node, DispycosNodeAllocate)
                                  else DispycosNodeAllocate(node) for node in node_allocations]
        self.status_task = status_task
        if node_setup:
            components.append(node_setup)
            self._node_setup = node_setup.__name__
        else:
            self._node_setup = None
        if server_setup:
            components.append(server_setup)
            self._server_setup = server_setup.__name__
        else:
            self._server_setup = None
        self._peers_communicate = bool(peers_communicate)
        self._disable_nodes = bool(disable_nodes)
        self._disable_servers = bool(disable_servers)

        depends = set()
        cwd = os.getcwd()
        for dep in components:
            if isinstance(dep, str) or inspect.ismodule(dep):
                if inspect.ismodule(dep):
                    name = dep.__file__
                    if name.endswith('.pyc'):
                        name = name[:-1]
                    if not name.endswith('.py'):
                        raise Exception('Invalid module "%s" - must be python source.' % dep)
                    if name.startswith(cwd):
                        dst = os.path.dirname(name[len(cwd):].lstrip(os.sep))
                    elif dep.__package__:
                        dst = dep.__package__.replace('.', os.sep)
                    else:
                        dst = os.path.dirname(dep.__name__.replace('.', os.sep))
                else:
                    name = os.path.abspath(dep)
                    if name.startswith(cwd):
                        dst = os.path.dirname(name[len(cwd):].lstrip(os.sep))
                    else:
                        dst = '.'
                if name in depends:
                    continue
                try:
                    with open(name, 'rb') as fd:
                        pass
                except:
                    raise Exception('File "%s" is not valid' % name)
                self._xfer_files.append((name, dst, os.sep))
                depends.add(name)
            elif (inspect.isgeneratorfunction(dep) or inspect.isfunction(dep) or
                  inspect.isclass(dep) or hasattr(dep, '__class__')):
                if inspect.isgeneratorfunction(dep) or inspect.isfunction(dep):
                    name = dep.__name__
                elif inspect.isclass(dep):
                    name = dep.__name__
                elif hasattr(dep, '__class__') and inspect.isclass(dep.__class__):
                    dep = dep.__class__
                    name = dep.__name__
                if name in depends:
                    continue
                depends.add(name)
                self._xfer_funcs.add(name)
                self._code += '\n' + inspect.getsource(dep).lstrip()
            else:
                raise Exception('Invalid computation: %s' % dep)
        # check code can be compiled
        compile(self._code, '<string>', 'exec')
        # Under Windows dispycos server may send objects with '__mp_main__'
        # scope, so make an alias to '__main__'.  Do so even if scheduler is not
        # running on Windows; it is possible the client is not Windows, but a
        # node is.
        if os.name == 'nt' and '__mp_main__' not in sys.modules:
            sys.modules['__mp_main__'] = sys.modules['__main__']

    def schedule(self, location=None, timeout=None):
        """Schedule computation for execution. Must be used with 'yield' as
        'result = yield compute.schedule()'. If scheduler is executing other
        computations, this will block until scheduler processes them
        (computations are processed in the order submitted).
        """

        if self._auth is not None:
            raise StopIteration(0)
        self._auth = ''
        if self.status_task is not None and not isinstance(self.status_task, Task):
            raise StopIteration(-1)

        self.scheduler = yield SysTask.locate('dispycos_scheduler', location=location,
                                              timeout=MsgTimeout)
        if not isinstance(self.scheduler, Task):
            raise StopIteration(-1)

        def _schedule(self, task=None):
            self._pulse_task = SysTask(self._pulse_proc)
            msg = {'req': 'schedule', 'computation': pycos.serialize(self), 'client': task}
            self.scheduler.send(msg)
            self._auth = yield task.receive(timeout=MsgTimeout)
            if not isinstance(self._auth, str):
                logger.debug('Could not send computation to scheduler %s: %s',
                             self.scheduler, self._auth)
                raise StopIteration(-1)
            SysTask.scheduler().atexit(10, lambda: SysTask(self.close))
            if task.location != self.scheduler.location:
                for xf, dst, sep in self._xfer_files:
                    drive, xf = os.path.splitdrive(xf)
                    if xf.startswith(sep):
                        xf = os.path.join(os.sep, *(xf.split(sep)))
                    else:
                        xf = os.path.join(*(xf.split(sep)))
                    xf = drive + xf
                    dst = os.path.join(self._auth, os.path.join(*(dst.split(sep))))
                    if (yield pycos.Pycos.instance().send_file(
                       self.scheduler.location, xf, dir=dst, timeout=MsgTimeout)) < 0:
                        logger.warning('Could not send file "%s" to scheduler', xf)
                        yield self.close()
                        raise StopIteration(-1)
            msg = {'req': 'await', 'auth': self._auth, 'client': task}
            self.scheduler.send(msg)
            resp = yield task.receive()
            if (isinstance(resp, dict) and resp.get('auth') == self._auth and
               resp.get('resp') == 'scheduled'):
                raise StopIteration(0)
            else:
                yield self.close()
                raise StopIteration(-1)

        yield Task(_schedule, self).finish()

    def nodes(self):
        """Get list of addresses of nodes initialized for this computation. Must
        be used with 'yield' as 'yield compute.nodes()'.
        """

        def _nodes(self, task=None):
            msg = {'req': 'nodes', 'auth': self._auth, 'client': task}
            if (yield self.scheduler.deliver(msg, timeout=MsgTimeout)) == 1:
                yield task.receive(MsgTimeout)
            else:
                raise StopIteration([])

        yield Task(_nodes, self).finish()

    def servers(self):
        """Get list of Location instances of servers initialized for this
        computation. Must be used with 'yield' as 'yield compute.servers()'.
        """

        def _servers(self, task=None):
            msg = {'req': 'servers', 'auth': self._auth, 'client': task}
            if (yield self.scheduler.deliver(msg, timeout=MsgTimeout)) == 1:
                yield task.receive(MsgTimeout)
            else:
                raise StopIteration([])

        yield Task(_servers, self).finish()

    def close(self, await_async=False):
        """Close computation. Must be used with 'yield' as 'yield
        compute.close()'.
        """

        def _close(self, done, task=None):
            msg = {'req': 'close_computation', 'auth': self._auth, 'client': task,
                   'await_async': bool(await_async)}
            yield self.scheduler.deliver(msg, timeout=MsgTimeout)
            msg = yield task.receive()
            if msg != 'closed':
                logger.warning('%s: closing computation failed?', self._auth)
            self._auth = None
            if self._pulse_task:
                yield self._pulse_task.send('quit')
                self._pulse_task = None
            done.set()

        if self._auth:
            done = pycos.Event()
            SysTask(_close, self, done)
            yield done.wait()

    def run_at(self, where, gen, *args, **kwargs):
        """Must be used with 'yield' as

        'rtask = yield computation.run_at(where, gen, ...)'

        Run given generator function 'gen' with arguments 'args' and 'kwargs' at
        remote server 'where'.  If the request is successful, 'rtask' will be a
        (remote) task; check result with 'isinstance(rtask,
        pycos.Task)'. The generator is expected to be (mostly) CPU bound and
        until this is finished, another CPU bound task will not be
        submitted at same server.

        If 'where' is a string, it is assumed to be IP address of a node, in
        which case the task is scheduled at that node on a server at that
        node. If 'where' is a Location instance, it is assumed to be server
        location in which case the task is scheduled at that server.

        'gen' must be generator function, as it is used to run task at
        remote location.
        """
        yield self._run_request('run_async', where, 1, gen, *args, **kwargs)

    def run(self, gen, *args, **kwargs):
        """Run CPU bound task at any remote server; see 'run_at'
        above.
        """
        yield self._run_request('run_async', None, 1, gen, *args, **kwargs)

    def run_result_at(self, where, gen, *args, **kwargs):
        """Must be used with 'yield' as

        'rtask = yield computation.run_result_at(where, gen, ...)'

        Whereas 'run_at' and 'run' return remote task instance,
        'run_result_at' and 'run_result' wait until remote task is
        finished and return the result of that remote task (i.e., either
        the value of 'StopIteration' or the last value 'yield'ed).

        'where', 'gen', 'args', 'kwargs' are as explained in 'run_at'.
        """
        yield self._run_request('run_result', where, 1, gen, *args, **kwargs)

    def run_result(self, gen, *args, **kwargs):
        """Run CPU bound task at any remote server and return result of
        that task; see 'run_result_at' above.
        """
        yield self._run_request('run_result', None, 1, gen, *args, **kwargs)

    def run_async_at(self, where, gen, *args, **kwargs):
        """Must be used with 'yield' as

        'rtask = yield computation.run_async_at(where, gen, ...)'

        Run given generator function 'gen' with arguments 'args' and 'kwargs' at
        remote server 'where'.  If the request is successful, 'rtask' will be a
        (remote) task; check result with 'isinstance(rtask,
        pycos.Task)'. The generator is supposed to be (mostly) I/O bound and
        not consume CPU time. Unlike other 'run' variants, tasks created
        with 'async' are not "tracked" by scheduler (see online documentation for
        more details).

        If 'where' is a string, it is assumed to be IP address of a node, in
        which case the task is scheduled at that node on a server at that
        node. If 'where' is a Location instance, it is assumed to be server
        location in which case the task is scheduled at that server.

        'gen' must be generator function, as it is used to run task at
        remote location.
        """
        yield self._run_request('run_async', where, 0, gen, *args, **kwargs)

    def run_async(self, gen, *args, **kwargs):
        """Run I/O bound task at any server; see 'run_async_at'
        above.
        """
        yield self._run_request('run_async', None, 0, gen, *args, **kwargs)

    def run_results(self, gen, iter):
        """Must be used with 'yield', as for example,
        'results = yield scheduler.map_results(generator, list_of_tuples)'.

        Execute generator 'gen' with arguments from given iterable. The return
        value is list of results that correspond to executing 'gen' with
        arguments in iterable in the same order.
        """
        tasks = []
        append_task = tasks.append
        for params in iter:
            if not isinstance(params, tuple):
                if hasattr(params, '__iter__'):
                    params = tuple(params)
                else:
                    params = (params,)
            append_task(Task(self.run_result, gen, *params))
        results = [None] * len(tasks)
        for i, task in enumerate(tasks):
            results[i] = yield task.finish()
        raise StopIteration(results)

    def enable_node(self, ip_addr, *setup_args):
        """If computation disabled nodes (with 'disabled_nodes=True' when
        Computation is constructed), nodes are not automatically used by the
        scheduler until nodes are enabled with 'enable_node'.

        'ip_addr' must be either IP address or host name of the node to be
        enabled.

        'setup_args' is arguments passed to 'node_setup' function specific to
        that node. If 'node_setup' succeeds (i.e., finishes with value 0), the
        node is used for computations.
        """
        if self.scheduler:
            if isinstance(ip_addr, pycos.Location):
                ip_addr = ip_addr.addr
            self.scheduler.send({'req': 'enable_node', 'auth': self._auth, 'addr': ip_addr,
                                 'setup_args': setup_args})

    def enable_server(self, location, *setup_args):
        """If computation disabled servers (with 'disabled_servers=True' when
        Computation is constructed), servers are not automatically used by the
        scheduler until they are enabled with 'enable_server'.

        'location' must be Location instance of the server to be enabled.

        'setup_args' is arguments passed to 'server_setup' function specific to
        that server. If 'server_setup' succeeds (i.e., finishes with value 0), the
        server is used for computations.
        """
        if self.scheduler:
            self.scheduler.send({'req': 'enable_server', 'auth': self._auth, 'server': location,
                                 'setup_args': setup_args})

    def _run_request(self, request, where, cpu, gen, *args, **kwargs):
        """Internal use only.
        """
        if isinstance(gen, str):
            name = gen
        else:
            name = gen.__name__

        if name in self._xfer_funcs:
            code = None
        else:
            # if not inspect.isgeneratorfunction(gen):
            #     logger.warning('"%s" is not a valid generator function', name)
            #     raise StopIteration([])
            code = inspect.getsource(gen).lstrip()

        def _run_req(task=None):
            msg = {'req': 'job', 'auth': self._auth,
                   'job': _DispycosJob_(request, task, name, where, cpu, code, args, kwargs)}
            if (yield self.scheduler.deliver(msg, timeout=MsgTimeout)) == 1:
                reply = yield task.receive()
                if isinstance(reply, Task):
                    if self.status_task:
                        msg = DispycosTaskInfo(reply, args, kwargs, time.time())
                        self.status_task.send(DispycosStatus(Scheduler.TaskCreated, msg))
                if not request.endswith('async'):
                    reply = yield task.receive()
            else:
                reply = None
            raise StopIteration(reply)

        yield Task(_run_req).finish()

    def _pulse_proc(self, task=None):
        """For internal use only.
        """
        task.set_daemon()
        last_pulse = time.time()
        timeout = 2 * self._pulse_interval
        while 1:
            msg = yield task.receive(timeout=timeout)
            if msg == 'pulse':
                last_pulse = time.time()
            elif msg == 'quit':
                break
            elif msg is None and (time.time() - last_pulse) > (10 * self._pulse_interval):
                logger.warning('scheduler may have gone away!')
            else:
                logger.debug('ignoring invalid pulse message')
        self._pulse_task = None

    def __getstate__(self):
        state = {}
        for attr in ['_code', '_xfer_funcs', '_xfer_files', '_auth',  'scheduler', 'status_task',
                     '_pulse_interval', '_pulse_task', '_node_allocations',
                     '_node_setup', '_server_setup', '_disable_nodes', '_disable_servers',
                     '_peers_communicate']:
            state[attr] = getattr(self, attr)
        return state

    def __setstate__(self, state):
        for attr, value in state.items():
            setattr(self, attr, value)


class _DispycosJob_(object):
    """Internal use only.
    """
    __slots__ = ('request', 'client', 'name', 'where', 'cpu', 'code', 'args', 'kwargs', 'done')

    def __init__(self, request, client, name, where, cpu, code, args=None, kwargs=None):
        self.request = request
        self.client = client
        self.name = name
        self.where = where
        self.cpu = cpu
        self.code = code
        self.args = pycos.serialize(args)
        self.kwargs = pycos.serialize(kwargs)
        self.done = None


class Scheduler(object, metaclass=pycos.Singleton):

    # status indications ('status' attribute of DispycosStatus)
    NodeDiscovered = 1
    NodeInitialized = 2
    NodeClosed = 3
    NodeIgnore = 4
    NodeDisconnected = 5

    ServerDiscovered = 11
    ServerInitialized = 12
    ServerClosed = 13
    ServerIgnore = 14
    ServerDisconnected = 15

    TaskCreated = 20
    ComputationScheduled = 23
    ComputationClosed = 25

    _instance = None

    """This class is for use by Computation class (see below) only.  Other than
    the status indications above, none of its attributes are to be accessed
    directly.
    """

    __status_task = None

    class _Node(object):

        def __init__(self, name, addr):
            self.name = name
            self.addr = addr
            self.cpus_used = 0
            self.platform = None
            self.avail_info = None
            self.servers = {}
            self.disabled_servers = {}
            self.load = 0.0
            self.status = Scheduler.NodeClosed
            self.task = None
            self.last_pulse = time.time()
            self.lock = pycos.Lock()
            self.avail = pycos.Event()
            self.avail.clear()

    class _Server(object):

        def __init__(self, name, location):
            self.name = name
            self.task = None
            self.status = Scheduler.ServerClosed
            self.rtasks = {}
            self.xfer_files = []
            self.askew_results = {}
            self.avail = pycos.Event()
            self.avail.clear()
            self.scheduler = Scheduler._instance

        def run(self, job, computation, node):
            def _run(self, task=None):
                self.task.send({'req': 'run', 'auth': computation._auth, 'job': job, 'client': task})
                rtask = yield task.receive(timeout=MsgTimeout)
                # currently fault-tolerancy is not supported, so clear job's
                # args to save space
                job.args = job.kwargs = None
                if isinstance(rtask, Task):
                    # TODO: keep func too for fault-tolerance
                    job.done = pycos.Event()
                    self.rtasks[rtask] = (rtask, job)
                    if self.askew_results:
                        msg = self.askew_results.pop(rtask, None)
                        if msg:
                            Scheduler.__status_task.send(msg)
                else:
                    logger.debug('failed to create rtask: %s', rtask)
                    if job.cpu:
                        self.avail.set()
                        node.cpus_used -= 1
                        node.load = float(node.cpus_used) / len(node.servers)
                        self.scheduler._avail_nodes.add(node)
                        self.scheduler._nodes_avail.set()
                        node.avail.set()
                raise StopIteration(rtask)

            rtask = yield SysTask(_run, self).finish()
            job.client.send(rtask)

    def __init__(self, **kwargs):
        self.__class__._instance = self
        self._nodes = {}
        self._disabled_nodes = {}
        self._avail_nodes = set()
        self._nodes_avail = pycos.Event()
        self._nodes_avail.clear()

        self._cur_computation = None
        self.__cur_client_auth = None
        self.__cur_node_allocations = []
        self.__pulse_interval = kwargs.pop('pulse_interval', MaxPulseInterval)
        self.__ping_interval = kwargs.pop('ping_interval', 0)
        self.__zombie_period = kwargs.pop('zombie_period', 100 * MaxPulseInterval)
        self._node_port = kwargs.pop('dispycosnode_port', 51351)
        self.__server_locations = set()
        self.__job_scheduler_task = None

        kwargs['name'] = 'dispycos_scheduler'
        clean = kwargs.pop('clean', False)
        nodes = kwargs.pop('nodes', [])
        self.pycos = pycos.Pycos.instance(**kwargs)
        self.__dest_path = os.path.join(self.pycos.dest_path, 'dispycos', 'dispycosscheduler')
        if clean:
            shutil.rmtree(self.__dest_path)
        self.pycos.dest_path = self.__dest_path

        self.__computation_sched_event = pycos.Event()
        self.__computation_scheduler_task = SysTask(self.__computation_scheduler_proc, nodes)
        self.__client_task = SysTask(self.__client_proc)
        self.__timer_task = SysTask(self.__timer_proc)
        Scheduler.__status_task = self.__status_task = SysTask(self.__status_proc)
        self.__client_task.register('dispycos_scheduler')
        self.pycos.discover_peers(port=self._node_port)

    def status(self):
        pending = sum(node.cpus_used for node in self._nodes.values())
        servers = functools.reduce(operator.add, [list(node.servers.keys())
                                                  for node in self._nodes.values()], [])
        return {'Client': self._cur_computation._pulse_task.location if self._cur_computation else '',
                'Pending': pending, 'Nodes': list(self._nodes.keys()), 'Servers': servers
                }

    def print_status(self):
        status = self.status()
        print('')
        print('  Client: %s' % status['Client'])
        print('  Pending: %s' % status['Pending'])
        print('  nodes: %s' % len(status['Nodes']))
        print('  servers: %s' % len(status['Servers']))

    def __status_proc(self, task=None):
        task.set_daemon()
        task.register('dispycos_status')
        self.pycos.peer_status(task)
        while 1:
            msg = yield task.receive()
            now = time.time()
            if isinstance(msg, pycos.MonitorException):
                rtask = msg.args[0]
                if not isinstance(rtask, Task):
                    logger.warning('ignoring invalid rtask %s', type(rtask))
                    continue
                node = self._nodes.get(rtask.location.addr, None)
                if not node:
                    logger.warning('node %s is invalid', rtask.location.addr)
                    continue
                server = node.servers.get(rtask.location, None)
                if not server:
                    logger.warning('server "%s" is invalid', rtask.location)
                    continue
                node.last_pulse = now
                info = server.rtasks.pop(rtask, None)
                if not info:
                    # Due to 'yield' used to create rtask, scheduler may not
                    # have updated self._rtasks before the task's
                    # MonitorException is received, so put it in
                    # 'askew_results'. The scheduling task will resend it
                    # when it receives rtask
                    server.askew_results[rtask] = msg
                    continue
                # assert isinstance(info[1], _DispycosJob_)
                job = info[1]
                if job.cpu:
                    node.cpus_used -= 1
                    node.load = float(node.cpus_used) / len(node.servers)
                    self._avail_nodes.add(node)
                    self._nodes_avail.set()
                    node.avail.set()
                    server.avail.set()
                if job.request.endswith('async'):
                    if job.done:
                        job.done.set()
                else:
                    job.client.send(msg.args[1][1])
                if self._cur_computation and self._cur_computation.status_task:
                    if len(msg.args) > 2:
                        msg.args = (msg.args[0], msg.args[1])
                    self._cur_computation.status_task.send(msg)

            elif isinstance(msg, pycos.PeerStatus):
                if msg.status == pycos.PeerStatus.Online:
                    if msg.name.endswith('_proc-0'):
                        SysTask(self.__discover_node, msg)
                else:
                    # msg.status == pycos.PeerStatus.Offline
                    node = server = None
                    node = self._nodes.get(msg.location.addr, None)
                    if node:
                        server = node.servers.pop(msg.location, None)
                        if server:
                            SysTask(self.__close_server, server, self._cur_computation)
                        elif node.task and node.task.location == msg.location:
                            # TODO: inform scheduler / client
                            self._nodes.pop(msg.location.addr)
                        else:
                            node = None
                        # if not node.servers:
                        #     self._nodes.pop(msg.location.addr)
                    if ((not server and not node) and self._cur_computation and
                        self._cur_computation._pulse_task.location == msg.location):
                        logger.warning('Client %s terminated; closing computation %s',
                                       msg.location, self.__cur_client_auth)
                        SysTask(self.__close_computation)

            elif isinstance(msg, dict):  # message from a node's server
                status = msg.get('status', None)
                if status == 'pulse':
                    location = msg.get('location', None)
                    if not isinstance(location, pycos.Location):
                        continue
                    node = self._nodes.get(location.addr, None)
                    if node:
                        node.last_pulse = now
                        node_status = msg.get('node_status', None)
                        if (node_status and self._cur_computation and
                           self._cur_computation.status_task):
                            self._cur_computation.status_task.send(node_status)

                elif status == Scheduler.ServerDiscovered:
                    rtask = msg.get('task', None)
                    if not isinstance(rtask, pycos.Task):
                        continue
                    node = self._nodes.get(rtask.location.addr, None)
                    if not node:
                        node = self._disabled_nodes.get(rtask.location.addr, None)
                    if not node or (node.status != Scheduler.NodeInitialized and
                                    node.status != Scheduler.NodeDiscovered):
                        continue
                    server = node.servers.get(rtask.location, None)
                    if not server:
                        server = node.disabled_servers.get(rtask.location, None)
                    if server:
                        continue
                    server = Scheduler._Server(msg.get('name', None), rtask.location)
                    server.task = rtask
                    server.status = Scheduler.ServerDiscovered
                    node.disabled_servers[rtask.location] = server
                    if self._cur_computation and self._cur_computation.status_task:
                        info = DispycosStatus(server.status, server.task.location)
                        self._cur_computation.status_task.send(info)

                elif status == Scheduler.ServerInitialized:
                    rtask = msg.get('task', None)
                    if not isinstance(rtask, pycos.Task):
                        continue
                    if (not self._cur_computation or
                        self._cur_computation._auth != msg.get('auth', None)):
                        continue
                    node = self._nodes.get(rtask.location.addr, None)
                    if not node:
                        node = self._disabled_nodes.get(rtask.location.addr, None)
                    if not node or (node.status != Scheduler.NodeInitialized and
                                    node.status != Scheduler.NodeDiscovered):
                        continue
                    server = node.disabled_servers.pop(rtask.location, None)
                    if not server:
                        server = node.servers.get(rtask.location, None)
                    if server:
                        if server.status != Scheduler.ServerDiscovered:
                            continue
                    else:
                        server = Scheduler._Server(msg.get('name', None), rtask.location)
                        server.task = rtask

                    server.status = Scheduler.ServerInitialized
                    node.servers[rtask.location] = server
                    node.last_pulse = now
                    if node.status == Scheduler.NodeInitialized:
                        if self._cur_computation.status_task:
                            self._cur_computation.status_task.send(
                                DispycosStatus(server.status, server.task.location))
                        server.avail.set()
                        node.avail.set()
                        self._avail_nodes.add(node)
                        self._nodes_avail.set()
                    if self._cur_computation._peers_communicate:
                        server.task.send({'req': 'peers', 'auth': self._cur_computation._auth,
                                          'peers': list(self.__server_locations)})
                        self.__server_locations.add(server.task.location)

                elif status in (Scheduler.ServerClosed, Scheduler.ServerDisconnected):
                    location = msg.get('location', None)
                    if not isinstance(location, pycos.Location):
                        continue
                    node = self._nodes.get(location.addr, None)
                    if not node:
                        continue
                    if status == Scheduler.ServerClosed:
                        server = node.servers.get(location, None)
                        if server:
                            server.status = Scheduler.ServerIgnore
                    else:
                        server = node.servers.pop(location, None)
                        if not node.servers:
                            node.status = Scheduler.NodeIgnore
                    if not server:
                        continue
                    if not node.servers:
                        self._avail_nodes.discard(node)
                        if not self._avail_nodes:
                            self._nodes_avail.clear()
                        # self._nodes.pop(node.addr, None)
                    if self._cur_computation:
                        if self._cur_computation._peers_communicate:
                            self.__server_locations.discard(server.task.location)
                            # TODO: inform other servers
                        if self._cur_computation.status_task:
                            self._cur_computation.status_task.send(DispycosStatus(status, location))
                            # if not node.servers:
                            #     info = DispycosNodeInfo(node.name, node.addr, node.cpus,
                            #                            node.platform, node.avail_info)
                            #     self._cur_computation.status_task.send(
                            #         Scheduler.NodeClosed, DispycosStatus(node.status, info))

                else:
                    logger.warning('Ignoring invalid status message: %s', status)
            else:
                logger.warning('invalid status message ignored')

    def __node_allocate(self, node):
        for node_allocate in self.__cur_node_allocations:
            cpus = node_allocate.allocate(node.addr, node.name, node.platform, node.cpus,
                                          node.avail_info.memory, node.avail_info.disk)
            if cpus < 0:
                continue
            return min(cpus, node.cpus)
        return node.cpus

    def __get_node_info(self, node, task=None):
        if not node.task:
            logger.warning('Node %s is not valid', node.addr)
            raise StopIteration
        # assert node.addr in self._disabled_nodes
        node.task.send({'req': 'dispycos_node_info', 'client': task})
        node_info = yield task.receive(timeout=MsgTimeout)
        if not node_info:
            pycos.Task(pycos.Pycos.instance().close_peer, node.task.location)
            raise StopIteration
        node.name = node_info.name
        node.cpus = node_info.cpus
        node.platform = node_info.platform.lower()
        node.avail_info = node_info.avail_info
        if self._cur_computation:
            yield self.__init_node(node, task=task)

    def __init_node(self, node, setup_args=(), task=None):
        computation = self._cur_computation
        if not computation or not node.task:
            raise StopIteration(-1)
        # this task may be invoked in two different paths (when a node is
        # found right after computation is already scheduled, and when
        # computation is scheduled right after a node is found). To prevent
        # concurrent execution (that may reserve / initialize same node more
        # than once), lock is used
        yield node.lock.acquire()
        if node.status == Scheduler.NodeInitialized:
            node.lock.release()
            raise StopIteration(0)

        if node.status == Scheduler.NodeClosed:
            cpus = self.__node_allocate(node)
            if not cpus:
                node.status = Scheduler.NodeIgnore
                node.lock.release()
                raise StopIteration(0)

            node.task.send({'req': 'reserve', 'cpus': cpus, 'auth': computation._auth,
                            'status_task': self.__status_task, 'client': task,
                            'computation_location': computation._pulse_task.location})
            cpus = yield task.receive(timeout=MsgTimeout)
            if not cpus:
                logger.warning('setup of %s failed', node.task)
                node_task, node.task = node.task, None
                node.lock.release()
                self._disabled_nodes.pop(node.addr, None)
                yield pycos.Pycos.instance().close_peer(node_task.location)
                raise StopIteration(-1)

            if computation != self._cur_computation:
                node.status = Scheduler.NodeClosed
                node.task.send({'req': 'release', 'auth': computation._auth, 'client': None})
                node.lock.release()
                raise StopIteration(-1)

            node.status = Scheduler.NodeDiscovered
            if self._cur_computation and self._cur_computation.status_task:
                info = DispycosNodeInfo(node.name, node.addr, node.cpus, node.platform,
                                        node.avail_info)
                self._cur_computation.status_task.send(DispycosStatus(node.status, info))
            if self._cur_computation._disable_nodes:
                node.lock.release()
                raise StopIteration(0)

        for xf, dst, sep in computation._xfer_files:
            reply = yield self.pycos.send_file(node.task.location, xf, dir=dst, timeout=MsgTimeout)
            if reply < 0 or computation != self._cur_computation:
                logger.debug('failed to transfer file %s: %s', xf, reply)
                node.status = Scheduler.NodeClosed
                node.task.send({'req': 'release', 'auth': computation._auth, 'client': None})
                node.lock.release()
                raise StopIteration(-1)

        node.task.send({'req': 'computation', 'computation': computation, 'auth': computation._auth,
                        'setup_args': setup_args, 'client': task})
        cpus = yield task.receive(timeout=MsgTimeout)
        if not cpus or computation != self._cur_computation:
            node.status = Scheduler.NodeClosed
            node.task.send({'req': 'release', 'auth': computation._auth, 'client': None})
            node.lock.release()
            raise StopIteration(-1)
        node.cpus = cpus
        node.status = Scheduler.NodeInitialized
        self._disabled_nodes.pop(node.addr, None)
        self._nodes[node.addr] = node
        if computation.status_task:
            info = DispycosNodeInfo(node.name, node.addr, node.cpus, node.platform, node.avail_info)
            computation.status_task.send(DispycosStatus(node.status, info))
        node.lock.release()
        if node.servers:
            for server in node.servers.values():
                server.avail.set()
                if computation.status_task:
                    computation.status_task.send(DispycosStatus(server.status, server.task.location))
            node.avail.set()
            self._avail_nodes.add(node)
            self._nodes_avail.set()

    def __discover_node(self, msg, task=None):
        for _ in range(10):
            rtask = yield Task.locate('dispycos_node', location=msg.location, timeout=MsgTimeout)
            if not isinstance(rtask, Task):
                yield task.sleep(0.1)
                continue
            node = self._nodes.get(msg.location.addr, None)
            if not node:
                node = self._disabled_nodes.get(msg.location.addr, None)
            if node and node.task == rtask:
                raise StopIteration

            if not node:
                node = Scheduler._Node(msg.name, msg.location.addr)
                self._disabled_nodes[msg.location.addr] = node
            node.task = rtask
            yield self.__get_node_info(node, task=task)
            raise StopIteration

    def __timer_proc(self, task=None):
        task.set_daemon()
        node_check = client_pulse = last_ping = time.time()
        async_scheduler = task.scheduler()
        while 1:
            try:
                msg = yield task.receive(timeout=self.__pulse_interval)
            except GeneratorExit:
                break
            now = time.time()
            if self.__cur_client_auth:
                if self._cur_computation._pulse_task.send('pulse') == 0:
                    client_pulse = now
                elif ((now - client_pulse) > self.__zombie_period):
                    logger.warning('Closing zombie computation %s', self.__cur_client_auth)
                    SysTask(self.__close_computation)

                if (now - node_check) > self.__zombie_period:
                    node_check = now
                    for node in self._nodes.values():
                        if (node.status != Scheduler.NodeInitialized and
                            node.status != Scheduler.NodeDiscovered):
                            continue
                        if (now - node.last_pulse) > self.__zombie_period:
                            logger.warning('dispycos node %s is zombie!', node.addr)
                            SysTask(self.__close_node, node, self._cur_computation)

                    if not self._cur_computation._disable_nodes:
                        for node in self._disabled_nodes.values():
                            if node.task:
                                SysTask(self.__init_node, node)

            if self.__ping_interval and ((now - last_ping) > self.__ping_interval):
                last_ping = now
                async_scheduler.discover_peers(port=self._node_port)

    def __computation_scheduler_proc(self, nodes, task=None):
        task.set_daemon()
        for node in nodes:
            yield pycos.Pycos.instance().peer(node, broadcast=True)
        while 1:
            if self._cur_computation:
                self.__computation_sched_event.clear()
                yield self.__computation_sched_event.wait()
                continue

            self._cur_computation, client = yield task.receive()
            if self.__job_scheduler_task:
                self.__job_scheduler_task.terminate()
            self.__pulse_interval = self._cur_computation._pulse_interval

            self.__cur_client_auth = self._cur_computation._auth
            self._cur_computation._auth = hashlib.sha1(os.urandom(20)).hexdigest()
            self.__cur_node_allocations = self._cur_computation._node_allocations
            self._cur_computation._node_allocations = []

            self._disabled_nodes.update(self._nodes)
            self._avail_nodes.clear()
            self._nodes_avail.clear()
            for node in self._disabled_nodes.values():
                node.disabled_servers.update(node.servers)
                node.servers.clear()
                node.avail.clear()
                for server in node.disabled_servers.values():
                    server.avail.clear()
            logger.debug('Computation %s / %s scheduled', self.__cur_client_auth,
                         self._cur_computation._auth)
            self.__job_scheduler_task = SysTask(self.__job_scheduler_proc)
            msg = {'resp': 'scheduled', 'auth': self.__cur_client_auth}
            if (yield client.deliver(msg, timeout=MsgTimeout)) != 1:
                logger.warning('client not reachable?')
                self._cur_client_auth = None
                self._cur_computation = None
                continue
            for node in self._disabled_nodes.values():
                SysTask(self.__get_node_info, node)
            self.__timer_task.send(None)
        self.__computation_scheduler_task = None

    def __job_scheduler_proc(self, task=None):
        task.set_daemon()
        while 1:
            job = yield task.receive()
            if (not isinstance(job, pycos.dispycos._DispycosJob_) or
                not isinstance(job.client, Task)):
                logger.warning('Ignoring invalid client job request')
                continue
            cpu = job.cpu
            where = job.where
            if not where:
                if cpu:
                    while not self._avail_nodes:
                        # self._nodes_avail.clear()
                        yield self._nodes_avail.wait()
                    node = None
                    load = None
                    for host in self._avail_nodes:
                        if host.avail.is_set() and (load is None or host.load < load):
                            node = host
                            load = host.load
                else:
                    while not self._nodes:
                        # self._nodes_avail.clear()
                        yield self._nodes_avail.wait()
                    node = None
                    load = None
                    for host in self._nodes.values():
                        if host.avail.is_set() and (load is None or host.load < load):
                            node = host
                            load = host.load
                server = None
                load = None
                for proc in node.servers.values():
                    if cpu:
                        if proc.avail.is_set() and (load is None or len(proc.rtasks) < load):
                            server = proc
                            load = len(proc.rtasks)
                    elif (load is None or len(proc.rtasks) < load):
                        server = proc
                        load = len(proc.rtasks)
                if not server:
                    job.client.send(None)
                    raise StopIteration
                if cpu:
                    server.avail.clear()
                    node.cpus_used += 1
                    node.load = float(node.cpus_used) / len(node.servers)
                    if node.cpus_used == len(node.servers):
                        node.avail.clear()
                        self._avail_nodes.discard(node)
                        if not self._avail_nodes:
                            self._nodes_avail.clear()
                yield server.run(job, self._cur_computation, node)
            elif isinstance(where, str):
                node = self._nodes.get(where, None)
                if not node:
                    job.client.send(None)
                    raise StopIteration
                server = None
                load = None
                for proc in node.servers.values():
                    if cpu:
                        if proc.avail.is_set() and (load is None or len(proc.rtasks) < load):
                            server = proc
                            load = len(proc.rtasks)
                    elif (load is None or len(proc.rtasks) < load):
                        server = proc
                        load = len(proc.rtasks)
                if not server:
                    job.client.send(None)
                    raise StopIteration
                if cpu:
                    server.avail.clear()
                    node.cpus_used += 1
                    node.load = float(node.cpus_used) / len(node.servers)
                    if node.cpus_used == len(node.servers):
                        node.avail.clear()
                        self._avail_nodes.discard(node)
                        if not self._avail_nodes:
                            self._nodes_avail.clear()
                yield server.run(job, self._cur_computation, node)
            elif isinstance(where, pycos.Location):
                node = self._nodes.get(where.addr)
                if not node:
                    job.client.send(None)
                    raise StopIteration
                server = node.servers.get(where)
                if not server:
                    job.client.send(None)
                    raise StopIteration
                if cpu:
                    if server.rtasks:
                        yield server.avail.wait()
                    server.avail.clear()
                    node.cpus_used += 1
                    node.load = float(node.cpus_used) / len(node.servers)
                    if node.cpus_used == len(node.servers):
                        node.avail.clear()
                        self._avail_nodes.discard(node)
                        if not self._avail_nodes:
                            self._nodes_avail.clear()
                yield server.run(job, self._cur_computation, node)
            else:
                job.client.send(None)

    def __client_proc(self, task=None):
        task.set_daemon()
        computations = {}

        while 1:
            msg = yield task.receive()
            if not isinstance(msg, dict):
                continue
            req = msg.get('req', None)
            auth = msg.get('auth', None)
            if self.__cur_client_auth != auth:
                if req == 'schedule' or req == 'await':
                    pass
                else:
                    continue

            if req == 'job':
                self.__job_scheduler_task.send(msg['job'])
                continue

            client = msg.get('client', None)

            if req == 'enable_server':
                loc = msg.get('server', None)
                if not loc:
                    continue
                node = self._nodes.get(loc.addr, None)
                if not node:
                    continue
                server = node.disabled_servers.get(loc, None)
                if not server or not server.task or server.status != Scheduler.ServerDiscovered:
                    continue
                args = msg.get('setup_args', ())
                server.task.send({'req': 'enable_server', 'setup_args': args,
                                  'auth': self._cur_computation._auth})

            elif req == 'enable_node':
                addr = msg.get('addr', None)
                if not addr:
                    continue
                node = self._disabled_nodes.pop(addr, None)
                if not node:
                    continue
                if not node or not node.task or node.status != Scheduler.NodeDiscovered:
                    continue
                setup_args = msg.get('setup_args', ())
                self._nodes[node.addr] = node
                SysTask(self.__init_node, node, setup_args=setup_args)

            elif req == 'nodes':
                if isinstance(client, Task):
                    nodes = [node.addr for node in self._nodes.values()
                             if node.status == Scheduler.NodeInitialized]
                    client.send(nodes)

            elif req == 'servers':
                if isinstance(client, Task):
                    servers = [server.task.location for node in self._nodes.values()
                               if node.status == Scheduler.NodeInitialized
                               for server in node.servers.values()
                               # if server.status == Scheduler.ServerInitialized
                               ]
                    client.send(servers)

            elif req == 'schedule':
                if not isinstance(client, Task):
                    logger.warning('Ignoring invalid client request "%s"', req)
                    continue
                try:
                    computation = pycos.deserialize(msg['computation'])
                    assert isinstance(computation, Computation) or \
                        computation.__class__.__name__ == 'Computation'
                    assert isinstance(computation._pulse_task, Task)
                    if computation._pulse_task.location == self.pycos.location:
                        computation._pulse_task._id = int(computation._pulse_task._id)
                        if computation.status_task:
                            computation.status_task._id = int(computation.status_task._id)
                    assert isinstance(computation._pulse_interval, (float, int))
                    assert (MinPulseInterval <= computation._pulse_interval <= MaxPulseInterval)
                except:
                    logger.warning('ignoring invalid computation request')
                    client.send(None)
                    continue
                while 1:
                    computation._auth = hashlib.sha1(os.urandom(20)).hexdigest()
                    if not os.path.exists(os.path.join(self.__dest_path, computation._auth)):
                        break
                try:
                    os.mkdir(os.path.join(self.__dest_path, computation._auth))
                except:
                    logger.debug('Could not create "%s"',
                                 os.path.join(self.__dest_path, computation._auth))
                    client.send(None)
                    continue
                # TODO: save it on disk instead
                computations[computation._auth] = computation
                client.send(computation._auth)

            elif req == 'await':
                if not isinstance(client, Task):
                    logger.warning('Ignoring invalid client request "%s"', req)
                    continue
                computation = computations.pop(auth, None)
                if not computation:
                    client.send(None)
                    continue
                if computation._pulse_task.location.addr != self.pycos.location.addr:
                    computation._xfer_files = [(os.path.join(self.__dest_path, computation._auth,
                                                             os.path.join(*(dst.split(sep))),
                                                             xf.split(sep)[-1]),
                                                os.path.join(*(dst.split(sep))), os.sep)
                                               for xf, dst, sep in computation._xfer_files]
                for xf, dst, sep in computation._xfer_files:
                    if not os.path.isfile(xf):
                        logger.warning('File "%s" for computation %s is not valid',
                                       xf, computation._auth)
                        computation = None
                        break
                if computation is None:
                    client.send(None)
                else:
                    self.__computation_scheduler_task.send((computation, client))
                    self.__computation_sched_event.set()

            elif req == 'close_computation':
                if not isinstance(client, Task):
                    logger.warning('Ignoring invalid client request "%s"', req)
                    continue
                SysTask(self.__close_computation, client=client,
                        await_async=msg.get('await_async', False))

            else:
                logger.warning('Ignoring invalid client request "%s"', req)

    def __close_node(self, node, computation, await_async=False, task=None):
        if (not computation) or (node.status not in (Scheduler.NodeInitialized,
                                                     Scheduler.NodeDiscovered)):
            logger.warning('Closing node %s ignored: %s', node.addr, node.status)
            raise StopIteration(-1)
        while node.cpus_used:
            logger.info('Waiting for %s remote tasks at %s to finish',
                        node.cpus_used, node.addr)
            node.avail.clear()
            yield node.avail.wait()
        close_tasks = [SysTask(self.__close_server, server, computation, await_async=await_async)
                       for server in node.servers.values()]
        close_tasks.extend([SysTask(self.__close_server, server, computation)
                            for server in node.disabled_servers.values() if server.task])
        for close_task in close_tasks:
            yield close_task.finish()
        if node.task:
            node.task.send({'req': 'release', 'auth': computation._auth, 'client': task})
            yield task.receive(timeout=MsgTimeout)
        node.status = Scheduler.NodeClosed

    def __close_server(self, server, computation, await_async=False, task=None):
        if ((not computation or not server.task) or
            (server.status not in (Scheduler.ServerInitialized, Scheduler.ServerDiscovered))):
            logger.debug('Closing server %s ignored: %s', server.task.location if server.task else '',
                         server.status)
            raise StopIteration(-1)
        node = self._nodes.get(server.task.location.addr, None)
        if not node:
            raise StopIteration(-1)
        disconnected = server.task.location not in node.servers
        if disconnected:
            if computation and computation.status_task:
                computation.status_task.send(DispycosStatus(Scheduler.ServerDisconnected,
                                                            server.task.location))
        else:
            if not server.avail.is_set():
                logger.debug('Waiting for remote tasks at %s to finish', server.task.location)
                yield server.avail.wait()
            if await_async:
                while server.rtasks:
                    rtask, job = server.rtasks[next(iter(server.rtasks))]
                    logger.debug('Waiting for %s to finish', rtask)
                    yield job.done.wait()
            server.task.send({'req': 'close', 'auth': computation._auth, 'client': task})
            yield task.receive(timeout=MsgTimeout)
        if server.rtasks:  # wait a bit for server to terminate tasks
            for _ in range(5):
                yield task.sleep(0.1)
                if not server.rtasks:
                    break
        if server.rtasks:
            logger.warning('%s tasks running at %s', len(server.rtasks), server.task.location)
            if computation and computation.status_task:
                for (rtask, cpu) in server.rtasks.values():
                    status = pycos.MonitorException(rtask, (Scheduler.ServerClosed, None))
                    computation.status_task.send(status)
                node.cpus_used -= len(server.rtasks)
                server.rtasks.clear()

        server_task, server.task = server.task, None
        if self.__server_locations:
            self.__server_locations.discard(server_task.location)
            # TODO: inform other servers
        node.servers.pop(server_task.location, None)

        server.status = Scheduler.ServerClosed
        server.xfer_files = []
        server.askew_results.clear()
        if computation and computation.status_task and server_task:
            computation.status_task.send(DispycosStatus(server.status, server_task.location))

        if node.cpus_used == len(node.servers):
            self._avail_nodes.discard(node)
            if not self._avail_nodes:
                self._nodes_avail.clear()
            node.avail.clear()

        if disconnected and not node.servers:
            node.cpus_used = 0
            node.load = 0.0
            node.status = Scheduler.NodeClosed
            if computation and computation.status_task:
                info = DispycosNodeInfo(node.name, node.addr, node.cpus, node.platform,
                                        node.avail_info)
                computation.status_task.send(DispycosStatus(node.status, info))

        raise StopIteration(0)

    def __close_computation(self, client=None, await_async=False, task=None):
        self.__server_locations.clear()
        if self._cur_computation:
            close_tasks = [SysTask(self.__close_node, node, self._cur_computation,
                                   await_async=await_async) for node in self._nodes.values()]
            close_tasks.extend([SysTask(self.__close_node, node, self._cur_computation)
                                for node in self._disabled_nodes.values()
                                if node.status == Scheduler.NodeDiscovered])
            for close_task in close_tasks:
                yield close_task.finish()
        if self.__cur_client_auth:
            computation_path = os.path.join(self.__dest_path, self.__cur_client_auth)
            if os.path.isdir(computation_path):
                shutil.rmtree(computation_path, ignore_errors=True)
        if self._cur_computation and self._cur_computation.status_task:
            self._cur_computation.status_task.send(DispycosStatus(Scheduler.ComputationClosed,
                                                                  id(self._cur_computation)))
        self.__cur_client_auth = self._cur_computation = None
        self.__computation_sched_event.set()
        if client:
            client.send('closed')
        raise StopIteration(0)

    def close(self, task=None):
        """Close current computation and quit scheduler.

        Must be called with 'yield' as 'yield scheduler.close()' or as
        task.
        """
        yield self.__close_computation(task=task)
        raise StopIteration(0)


if __name__ == '__main__':
    """The scheduler can be started either within a client program (if no other
    client programs use the nodes simultaneously), or can be run on a node with
    the options described below (usually no options are necessary, so the
    scheduler can be strated with just 'dispycos.py')
    """

    import logging
    import argparse
    import signal
    try:
        import readline
    except ImportError:
        pass

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--ip_addr', dest='node', action='append', default=[],
                        help='IP address or host name of this node')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addr', action='append', default=[],
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('-u', '--udp_port', dest='udp_port', type=int, default=51350,
                        help='UDP port number to use')
    parser.add_argument('-t', '--tcp_port', dest='tcp_port', type=int, default=0,
                        help='TCP port number to use')
    parser.add_argument('-n', '--name', dest='name', default=None,
                        help='(symbolic) name given to schduler')
    parser.add_argument('--dest_path', dest='dest_path', default=None,
                        help='path prefix to where files sent by peers are stored')
    parser.add_argument('--max_file_size', dest='max_file_size', default=None, type=int,
                        help='maximum file size of any file transferred')
    parser.add_argument('-s', '--secret', dest='secret', default='',
                        help='authentication secret for handshake with peers')
    parser.add_argument('--certfile', dest='certfile', default='',
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default='',
                        help='file containing SSL key')
    parser.add_argument('--node', action='append', dest='nodes', default=[],
                        help='additional remote nodes (names or IP address) to use')
    parser.add_argument('--pulse_interval', dest='pulse_interval', type=float,
                        default=MaxPulseInterval,
                        help='interval in seconds to send "pulse" messages to check nodes '
                        'and client are connected')
    parser.add_argument('--ping_interval', dest='ping_interval', type=float, default=0,
                        help='interval in seconds to broadcast "ping" message to discover nodes')
    parser.add_argument('--zombie_period', dest='zombie_period', type=int,
                        default=(100 * MaxPulseInterval),
                        help='maximum time in seconds computation is idle')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('--clean', action='store_true', dest='clean', default=False,
                        help='if given, files copied from or generated by clients will be removed')
    parser.add_argument('--dispycosnode_port', dest='dispycosnode_port', type=int, default=51351,
                        help='UDP port number used by dispycosnode')
    parser.add_argument('--daemon', action='store_true', dest='daemon', default=False,
                        help='if given, input is not read from terminal')
    config = vars(parser.parse_args(sys.argv[1:]))
    del parser

    if config['zombie_period'] and config['zombie_period'] < MaxPulseInterval:
        raise Exception('zombie_period must be >= %s' % MaxPulseInterval)

    if not config['name']:
        config['name'] = 'dispycos_scheduler'

    if config['loglevel']:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    del config['loglevel']

    if config['certfile']:
        config['certfile'] = os.path.abspath(config['certfile'])
    else:
        config['certfile'] = None
    if config['keyfile']:
        config['keyfile'] = os.path.abspath(config['keyfile'])
    else:
        config['keyfile'] = None

    daemon = config.pop('daemon', False)
    _dispycos_scheduler = Scheduler(**config)
    _dispycos_scheduler._shared = True
    del config

    def sighandler(signum, frame):
        # Task(_dispycos_scheduler.close).value()
        raise KeyboardInterrupt

    try:
        signal.signal(signal.SIGHUP, sighandler)
        signal.signal(signal.SIGQUIT, sighandler)
    except:
        pass
    signal.signal(signal.SIGINT, sighandler)
    signal.signal(signal.SIGABRT, sighandler)
    signal.signal(signal.SIGTERM, sighandler)
    del sighandler

    if not daemon:
        try:
            if os.getpgrp() != os.tcgetpgrp(sys.stdin.fileno()):
                daemon = True
        except:
            pass

    if daemon:
        del daemon
        while 1:
            try:
                time.sleep(3600)
            except:
                break
    else:
        del daemon
        while 1:
            try:
                _dispycos_cmd = input(
                    '\n\nEnter "quit" or "exit" to terminate dispycos scheduler\n'
                    '      "status" to show status of scheduler: '
                    )
            except:
                break
            _dispycos_cmd = _dispycos_cmd.strip().lower()
            if _dispycos_cmd in ('quit', 'exit'):
                break
            if _dispycos_cmd == 'status':
                _dispycos_scheduler.print_status()

    logger.info('terminating dispycos scheduler')
    Task(_dispycos_scheduler.close).value()
