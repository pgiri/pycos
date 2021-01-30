#!/usr/bin/python3

"""
This file is part of pycos; see https://pycos.org for details.

This module provides API for creating distributed communicating
processes. 'Client' class should be used to package client components
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
import shutil
import operator
import functools
import re
import copy
import stat

import pycos
import pycos.netpycos
import pycos.config
from pycos import Task, SysTask, logger, Location, MonitorStatus

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2014-2015 Giridhar Pemmasani"
__license__ = "Apache 2.0"
__url__ = "https://pycos.org"

__all__ = ['Scheduler', 'Client', 'Computation', 'DispycosStatus', 'DispycosTaskInfo',
           'DispycosNodeInfo', 'DispycosNodeAvailInfo', 'DispycosNodeAllocate']

# status about nodes / servers are sent with this structure
DispycosStatus = collections.namedtuple('DispycosStatus', ['status', 'info'])
DispycosTaskInfo = collections.namedtuple('DispycosTaskInfo', ['task', 'args', 'kwargs',
                                                               'start_time'])
DispycosNodeInfo = collections.namedtuple('DispycosNodeInfo', ['name', 'addr', 'cpus', 'platform',
                                                               'avail_info'])
MsgTimeout = pycos.config.MsgTimeout
MinPulseInterval = pycos.config.MinPulseInterval
MaxPulseInterval = pycos.config.MaxPulseInterval
logger.name = 'dispycos'
# PyPI / pip packaging adjusts assertion below for Python 3.7+
assert sys.version_info.major == 3 and sys.version_info.minor < 7, \
    ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
     (__file__, sys.version_info.major, sys.version_info.minor))


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
    """Allocation of nodes can be customized by specifying 'nodes' of Client
    with DispycosNodeAllocate instances.

    'node' must be hostname or IP address (with possibly '*' to match rest of IP
    address), 'port must be TCP port used by node (only if 'node' doesn't have
    '*'), 'cpus', if given, must be number of servers running on that node if
    positive, or number of CPUs not to use if negative, 'memory' is minimum
    available memory in bytes, 'disk' is minimum available disk space (on the
    partition where dispycosnode servers are running from), and 'platform' is
    regular expression to match output of"platform.platform()" on that node,
    e.g., "Linux.*x86_64" to accept only nodes that run 64-bit Linux.
    """

    def __init__(self, node, port=None, platform='', cpus=None, memory=None, disk=None):
        if node.find('*') < 0:
            self.ip_rex = pycos.Pycos.host_ipaddr(node)
        else:
            self.ip_rex = node

        if self.ip_rex:
            self.ip_rex = self.ip_rex.replace('.', '\\.').replace('*', '.*')
        else:
            logger.warning('node "%s" is invalid', node)
            self.ip_rex = ''

        self.port = port
        self.platform = platform.lower()
        self.cpus = cpus
        self.memory = memory
        self.disk = disk

    def allocate(self, ip_addr, name, platform, cpus, memory, disk):
        """When a node is found, scheduler calls this method with IP address, name,
        CPUs, memory and disk available on that node. This method should return
        a number indicating number of CPUs to use. If return value is 0, the
        node is not used; if the return value is < 0, this allocation is ignored
        (next allocation in the 'nodes' list, if any, is applied).
        """
        if (self.platform and not re.search(self.platform, platform)):
            return -1
        if ((self.memory and memory and self.memory > memory) or
            (self.disk and disk and self.disk > disk)):
            return 0
        if not isinstance(self.cpus, int):
            return cpus
        if self.cpus == 0:
            return 0
        elif self.cpus > 0:
            if self.cpus > cpus:
                return 0
            return self.cpus
        else:
            cpus += self.cpus
            if cpus < 0:
                return 0
            return cpus

    def __getstate__(self):
        state = {}
        for attr in ['ip_rex', 'port', 'platform', 'cpus', 'memory', 'disk']:
            state[attr] = getattr(self, attr)
        return state


class Client(object):
    """Packages components to distribute to remote pycos schedulers to create
    (remote) tasks.
    """

    def __init__(self, components, nodes=[], status_task=None, node_setup=None, server_setup=None,
                 disable_nodes=False, disable_servers=False,
                 pulse_interval=(5*MinPulseInterval), node_allocations=[],
                 ping_interval=None, restart_servers=False,
                 zombie_period=None, abandon_zombie_nodes=False, scheduler=None):
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
        if ping_interval and ping_interval < pulse_interval:
            raise Exception('"ping_interval" must be at least %s' % (pulse_interval))
        if not isinstance(nodes, list):
            raise Exception('"nodes" must be list of strings or DispycosNodeAllocate instances')
        if node_allocations:
            logger.warning('  WARNING: "node_allocations" is deprecated; use "nodes" instead')
            if not isinstance(node_allocations, list):
                raise Exception('"node_allocations" must be list of DispycosNodeAllocate instances')
            nodes.extend(node_allocations)
        if any(not isinstance(_, (DispycosNodeAllocate, str)) for _ in nodes):
            raise Exception('"nodes" must be list of strings or DispycosNodeAllocate instances')
        if status_task and not isinstance(status_task, Task):
            raise Exception('status_task must be Task instance')
        if node_setup and not inspect.isgeneratorfunction(node_setup):
            raise Exception('"node_setup" must be a task (generator) function')
        if server_setup and not inspect.isgeneratorfunction(server_setup):
            raise Exception('"server_setup" must be a task (generator) function')
        if (disable_nodes or disable_servers) and not status_task:
            raise Exception('status_task must be given when nodes or servers are disabled')
        if zombie_period:
            if zombie_period < 5*pulse_interval:
                raise Exception('"zombie_period" must be at least 5*pulse_interval')
        elif zombie_period is None:
            zombie_period = 10*pulse_interval

        if not isinstance(components, list):
            components = [components]

        self._code = ''
        self._xfer_files = []
        self._auth = None
        self.__xfer_funcs = set()
        if scheduler is None:
            self.__scheduler = None
            Scheduler()
        elif isinstance(scheduler, Location):
            self.__scheduler = Location(pycos.Pycos.host_ipaddr(scheduler.addr), scheduler.port)
        elif isinstance(scheduler, str):
            if not isinstance(pycos.config.DispycosSchedulerPort, int):
                pycos.config.DispycosSchedulerPort = eval(pycos.config.DispycosSchedulerPort)
            self.__scheduler = Location(pycos.Pycos.host_ipaddr(scheduler),
                                        pycos.config.DispycosSchedulerPort)
        else:
            raise Exception('"scheduler" must be an instance of Location or host name or IP')
        self._pulse_task = None
        if zombie_period:
            self._pulse_interval = min(pulse_interval, zombie_period / 3)
        else:
            self._pulse_interval = pulse_interval
        self._ping_interval = ping_interval
        self._zombie_period = zombie_period
        if nodes:
            self._node_allocations = [node if isinstance(node, DispycosNodeAllocate)
                                      else DispycosNodeAllocate(node) for node in nodes]
        else:
            self._node_allocations = []
        self._node_allocations.append(DispycosNodeAllocate('*'))
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
        self._disable_nodes = bool(disable_nodes)
        self._disable_servers = bool(disable_servers)
        self._restart_servers = bool(restart_servers)
        self._abandon_zombie = bool(abandon_zombie_nodes)
        self.__rtasks = {}
        self.__askew_tasks = {}

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
                except Exception:
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
                self.__xfer_funcs.add(name)
                self._code += '\n' + inspect.getsource(dep).lstrip()
            else:
                raise Exception('Invalid component: %s' % dep)
        # check code can be compiled
        compile(self._code, '<string>', 'exec')
        # Under Windows dispycos server may send objects with '__mp_main__'
        # scope, so make an alias to '__main__'.  Do so even if scheduler is not
        # running on Windows; it is possible the client is not Windows, but a
        # node is.
        if os.name == 'nt' and '__mp_main__' not in sys.modules:
            sys.modules['__mp_main__'] = sys.modules['__main__']

    def schedule(self, timeout=None):
        """Schedule client for execution. Must be used with 'yield' as
        'result = yield client.schedule()'. If scheduler is executing other clients,
        this will block until scheduler processes them (clients are processed in the
        order submitted).
        """

        if self._auth is not None:
            raise StopIteration(0)
        self._auth = ''
        if self.status_task is not None and not isinstance(self.status_task, Task):
            raise StopIteration(-1)

        if not self._pulse_task:
            self._pulse_task = SysTask(self._pulse_proc)
        location = None
        if self.__scheduler is None:
            location = self._pulse_task.location
        elif isinstance(self.__scheduler, Location):
            if (yield pycos.Pycos.instance().peer(self.__scheduler)) == 0:
                location = self.__scheduler
        self.__scheduler = yield SysTask.locate('dispycos_scheduler', location=location,
                                                timeout=MsgTimeout)
        if not isinstance(self.__scheduler, Task):
            pycos.logger.warning('could not find dispycos scheduler at %s', location)
            raise StopIteration(-1)

        def _schedule(self, task=None):
            msg = {'req': 'schedule', 'client': pycos.serialize(self),
                   'auth': self._auth, 'reply_task': task}
            self.__scheduler.send(msg)
            self._auth = yield task.receive(timeout=MsgTimeout)
            if not isinstance(self._auth, str):
                logger.debug('Could not send client to scheduler %s: %s',
                             self.__scheduler, self._auth)
                raise StopIteration(-1)
            SysTask.scheduler().atexit(10, lambda: SysTask(self.close))
            if task.location != self.__scheduler.location:
                for xf, dst, sep in self._xfer_files:
                    drive, xf = os.path.splitdrive(xf)
                    if xf.startswith(sep):
                        xf = os.path.join(os.sep, *(xf.split(sep)))
                    else:
                        xf = os.path.join(*(xf.split(sep)))
                    xf = drive + xf
                    dst = os.path.join(self._auth, os.path.join(*(dst.split(sep))))
                    if (yield pycos.Pycos.instance().send_file(
                       self.__scheduler.location, xf, dir=dst, timeout=MsgTimeout)) < 0:
                        logger.warning('Could not send file "%s" to scheduler', xf)
                        yield self.close()
                        raise StopIteration(-1)
            msg = {'req': 'await', 'auth': self._auth, 'reply_task': task}
            self.__scheduler.send(msg)
            resp = yield task.receive(timeout=timeout)
            if (isinstance(resp, dict) and resp.get('auth') == self._auth and
               resp.get('resp') == 'scheduled'):
                raise StopIteration(0)
            else:
                yield self.close()
                raise StopIteration(-1)

        raise StopIteration((yield Task(_schedule, self).finish()))

    def nodes(self):
        """Get list of addresses of nodes initialized for this client. Must
        be used with 'yield' as 'yield client.nodes()'.
        """

        def _nodes(self, task=None):
            msg = {'req': 'nodes', 'auth': self._auth, 'reply_task': task}
            if (yield self.__scheduler.deliver(msg, timeout=MsgTimeout)) == 1:
                yield task.receive(MsgTimeout)
            else:
                raise StopIteration([])

        raise StopIteration((yield Task(_nodes, self).finish()))

    def servers(self):
        """Get list of Location instances of servers initialized for this
        client. Must be used with 'yield' as 'yield client.servers()'.
        """

        def _servers(self, task=None):
            msg = {'req': 'servers', 'auth': self._auth, 'reply_task': task}
            if (yield self.__scheduler.deliver(msg, timeout=MsgTimeout)) == 1:
                yield task.receive(MsgTimeout)
            else:
                raise StopIteration([])

        raise StopIteration((yield Task(_servers, self).finish()))

    def tasks(self, where):
        """Get list of tasks at given node or server for this client.
        Must be used with 'yield' as 'yield client.tasks()'.
        """

        if isinstance(where, str):
            addr = pycos.Pycos.host_ipaddr(where)
        else:
            addr = None
        if not addr:
            logger.warning('invalid host name / IP address "%s"', where)

        def _tasks(self, task=None):
            msg = {'req': 'tasks', 'auth': self._auth, 'reply_task': task, 'at': addr}
            if (yield self.__scheduler.deliver(msg, timeout=MsgTimeout)) == 1:
                yield task.receive(MsgTimeout)
            else:
                raise StopIteration([])

        raise StopIteration((yield Task(_tasks, self).finish()))

    def close(self, await_io=False, terminate=False, timeout=None):
        """Close client. Must be used with 'yield' as 'yield client.close()'.
        """

        def _close(self, done, task=None):
            msg = {'req': 'close_client', 'auth': self._auth, 'reply_task': task,
                   'await_io': bool(await_io), 'terminate': bool(terminate)}
            self.__scheduler.send(msg)
            msg = yield task.receive(timeout=timeout)
            if msg != 'closed':
                logger.warning('%s: closing client failed?', self._auth)
            self._auth = None
            if self._pulse_task:
                yield self._pulse_task.send('quit')
                self._pulse_task = None
            done.set()

        if self._auth:
            done = pycos.Event()
            SysTask(_close, self, done)
            yield done.wait()

    def rtask_at(self, where, gen, *args, **kwargs):
        """Must be used with 'yield' as

        'rtask = yield client.rtask_at(where, gen, ...)'

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
        if isinstance(where, Location):
            addr = where
        elif isinstance(where, str):
            addr = pycos.Pycos.host_ipaddr(where)
        else:
            addr = None
        if not addr:
            logger.warning('invalid host name / IP address "%s"', where)
        raise StopIteration((yield self._rtask_req(addr, 1, gen, *args, **kwargs)))

    def rtask(self, gen, *args, **kwargs):
        """Run CPU bound task at any remote server; see 'rtask_at' above.
        """
        raise StopIteration((yield self._rtask_req(None, 1, gen, *args, **kwargs)))

    def io_rtask_at(self, where, gen, *args, **kwargs):
        """Must be used with 'yield' as

        'rtask = yield client.io_rtask_at(where, gen, ...)'

        Run given generator function 'gen' with arguments 'args' and 'kwargs' at
        remote server 'where'.  If the request is successful, 'rtask' will be a
        (remote) task; check result with 'isinstance(rtask,
        pycos.Task)'. The generator is supposed to be (mostly) I/O bound and
        not consume CPU time. The scheduler will schedule unlimited number of these
        I/O tasks at a server, whereas at most one CPU bound task is scheduled at
        any time.

        If 'where' is a string, it is assumed to be IP address of a node, in
        which case the task is scheduled at that node on a server at that
        node. If 'where' is a Location instance, it is assumed to be server
        location in which case the task is scheduled at that server.

        'gen' must be generator function, as it is used to run task at
        remote location.
        """
        if isinstance(where, Location):
            addr = where
        elif isinstance(where, str):
            addr = pycos.Pycos.host_ipaddr(where)
        else:
            addr = None
        if not addr:
            logger.warning('invalid host name / IP address "%s"', where)
        raise StopIteration((yield self._rtask_req(addr, 0, gen, *args, **kwargs)))

    def io_rtask(self, gen, *args, **kwargs):
        """Run I/O bound task at any server; see 'io_rtask_at' above.
        """
        raise StopIteration((yield self._rtask_req(None, 0, gen, *args, **kwargs)))

    run_at = rtask_at
    run = rtask
    run_async_at = io_rtask_at
    run_async = io_rtask

    def enable_node(self, node, *setup_args):
        """If client disabled nodes (with 'disabled_nodes=True' when
        Client is constructed), nodes are not automatically used by the
        scheduler until nodes are enabled with 'enable_node'.

        'node' must be either IP address or host name of the node to be
        enabled.

        'setup_args' is arguments passed to 'node_setup' function specific to
        that node. If 'node_setup' succeeds (i.e., finishes with value 0), the
        node is used for clients.
        """
        if self.__scheduler:
            if isinstance(node, Location):
                addr = node.addr
            elif isinstance(node, str):
                addr = pycos.Pycos.host_ipaddr(node)
            else:
                addr = None
            if not addr:
                logger.warning('invalid host name / IP address "%s"', node)
            self.__scheduler.send({'req': 'enable_node', 'auth': self._auth, 'addr': addr,
                                   'setup_args': pycos.serialize(setup_args)})

    def enable_server(self, location, *setup_args):
        """If client disabled servers (with 'disabled_servers=True' when
        Client is constructed), servers are not automatically used by the
        scheduler until they are enabled with 'enable_server'.

        'location' must be Location instance of the server to be enabled.

        'setup_args' is arguments passed to 'server_setup' function specific to
        that server. If 'server_setup' succeeds (i.e., finishes with value 0), the
        server is used for clients.
        """
        if self.__scheduler:
            if not isinstance(location, Location):
                logger.warning('invalid location "%s"', location)
            self.__scheduler.send({'req': 'enable_server', 'auth': self._auth, 'server': location,
                                   'setup_args': pycos.serialize(setup_args)})

    def suspend_node(self, node):
        """Suspend submitting jobs (tasks) at this node. Any currently running
        tasks are left running.
        """
        if self.__scheduler:
            if isinstance(node, Location):
                addr = node.addr
            elif isinstance(node, str):
                addr = pycos.Pycos.host_ipaddr(node)
            else:
                addr = None
            if not addr:
                logger.warning('invalid host name / IP address "%s"', node)
            self.__scheduler.send({'req': 'suspend_node', 'auth': self._auth, 'addr': addr})

    def resume_node(self, node):
        """Resume submitting jobs (tasks) at this node.
        """
        if self.__scheduler:
            if isinstance(node, Location):
                addr = node.addr
            elif isinstance(node, str):
                addr = pycos.Pycos.host_ipaddr(node)
            else:
                addr = None
            if not addr:
                logger.warning('invalid host name / IP address "%s"', node)
            self.__scheduler.send({'req': 'enable_node', 'auth': self._auth, 'addr': addr})

    def suspend_server(self, location):
        """Suspend submitting jobs (tasks) at this server. Any currently running
        tasks are left running.
        """
        if self.__scheduler:
            if not isinstance(location, Location):
                logger.warning('invalid location "%s"', location)
            self.__scheduler.send({'req': 'suspend_server', 'auth': self._auth, 'server': location})

    def resume_server(self, location):
        """Resume submitting jobs (tasks) at this server.
        """
        if self.__scheduler:
            if not isinstance(location, Location):
                logger.warning('invalid location "%s"', location)
            self.__scheduler.send({'req': 'enable_server', 'auth': self._auth, 'server': location})

    def close_server(self, location, terminate=False, restart=False):
        """Close server at given location. After this call, no more tasks are scheduled
        at that server.

        If 'terminate' is True, any tasks running at that server are terminated without waiting
        for them to finish. If it is False, the server will wait until tasks finish before closing.
        """
        if self.__scheduler:
            if not isinstance(location, Location):
                logger.warning('invalid location "%s"', location)
            self.__scheduler.send({'req': 'close_server', 'auth': self._auth, 'addr': location,
                                   'terminate': bool(terminate), 'restart': bool(restart)})

    def restart_server(self, location, terminate=False):
        """Restart server at given location. If 'terminate' is True, kill any running tasks.
        """
        if self.__scheduler:
            if not isinstance(location, Location):
                logger.warning('invalid location "%s"', location)
            self.__scheduler.send({'req': 'close_server', 'auth': self._auth, 'addr': location,
                                   'terminate': bool(terminate), 'restart': True})

    def close_node(self, node, terminate=False):
        """Close node at given location, which can be either a Location instance (of any server
        at that node or of node itself) or IP address. After this call, no more tasks are
        scheduled at that node.

        If 'terminate' is True, any tasks running at any of the servers at the node are terminated
        without waiting for them to finish. If it is False, the node will wait until tasks finish
        before closing.
        """
        if self.__scheduler:
            if isinstance(node, Location):
                addr = node.addr
            elif isinstance(node, str):
                addr = pycos.Pycos.host_ipaddr(node)
            else:
                addr = None
            if not addr:
                logger.warning('invalid host name / IP address "%s"', node)
            self.__scheduler.send({'req': 'close_node', 'auth': self._auth, 'addr': addr,
                                   'terminate': bool(terminate), 'restart': False})

    def restart_node(self, node, *setup_args, terminate=False):
        """Close node at given location, which can be either a Location instance (of any server
        at that node or of node itself) or IP address. After this call, no more tasks are
        scheduled at that node.

        If 'terminate' is True, any tasks running at any of the servers at the node are terminated
        without waiting for them to finish. If it is False, the node will wait until tasks finish
        before closing.
        """
        if self.__scheduler:
            if isinstance(node, Location):
                addr = node.addr
            elif isinstance(node, str):
                addr = pycos.Pycos.host_ipaddr(node)
            else:
                addr = None
            if not addr:
                logger.warning('invalid host name / IP address "%s"', node)
            self.__scheduler.send({'req': 'close_node', 'auth': self._auth, 'addr': addr,
                                   'terminate': bool(terminate), 'restart': True,
                                   'setup_args': pycos.serialize(setup_args)})

    def node_allocate(self, node_allocate):
        """Request scheduler to add 'node_allocate' to any previously sent
        'node_allocations'.
        """
        if not isinstance(node_allocate, DispycosNodeAllocate):
            return -1
        if not self._pulse_task:
            return -1
        if (node_allocate.__class__ != DispycosNodeAllocate and
            self._pulse_task.location != self.__scheduler.location):
            node_allocate = copy.copy(node_allocate)
            node_allocate.__class__ = DispycosNodeAllocate
        return self.__scheduler.send({'req': 'node_allocate', 'auth': self._auth,
                                      'node': node_allocate})

    def abandon_zombie(self, location, flag):
        """If a node at given location is deemed zombie (i.e., no response in 'zombie_period'),
        then abandon any jobs running on servers on that node. If a node is detected later,
        it will be treated as new (instance of) node.

        'location' can be either Location instance of any server at the node or of the node
        itself, IP address of node or None. If 'location' is None, then all nodes currently used
        by client and any nodes added for client will be abandoned (when they become
        zombies) as well.

        'flag' must be either True or False indicating whether nodes would be abandoned or not.
        """
        if self.__scheduler:
            if isinstance(location, Location):
                location = location.addr
            self.__scheduler.send({'req': 'abandon_zombie', 'auth': self._auth, 'addr': location,
                                   'flag': bool(flag)})

    def _rtask_req(self, where, cpu, gen, *args, **kwargs):
        """Internal use only.
        """
        if not inspect.isgeneratorfunction(gen):
            logger.warning('rtask first argument must be generator function')
            raise StopIteration(None)

        name = gen.__name__
        if name in self.__xfer_funcs:
            code = None
        else:
            code = inspect.getsource(gen).lstrip()

        def _job_req(task=None):
            msg = {'req': 'job', 'auth': self._auth, 'reply_task': task,
                   'job': _DispycosJob_(name, where, cpu, code, args, kwargs)}
            if (yield self.__scheduler.deliver(msg, timeout=MsgTimeout)) != 1:
                pycos.logger.warning('scheduling %s timedout', name)
                raise StopIteration(None)

            msg = yield task.receive()
            if not isinstance(msg, Task):
                if (isinstance(msg, MonitorStatus) and
                    isinstance(msg.info, str) and isinstance(msg.value, str)):
                    msg = ': %s\n%s' % (msg.info, msg.value)
                else:
                    msg = ''
                pycos.logger.warning('running %s failed%s', name, msg)
                raise StopIteration(None)
            rtask = msg
            setattr(rtask, '_complete', pycos.Event())
            rtask._complete.clear()
            if self.__askew_tasks:
                askew = self.__askew_tasks.pop(rtask, None)
            else:
                askew = None
            if askew:
                # assert isinstance(askew._value, MonitorStatus)
                askew._value.info = rtask
                if askew._value.type == StopIteration:
                    pycos.logger.debug('rtask %s done', rtask)
                    rtask._value = askew._value.value
                elif askew._value.type == Scheduler.TaskTerminated:
                    pycos.logger.warning('rtask %s terminated', rtask)
                elif askew._value.type == Scheduler.TaskAbandoned:
                    pycos.logger.warning('rtask %s abandoned', rtask)
                else:
                    rtask._value = askew._value
                    pycos.logger.warning('rtask %s failed: %s with %s',
                                         rtask, askew._value.type, askew._value.value)
                rtask._complete.set()
                if self.status_task:
                    self.status_task.send(askew._value)
            else:
                setattr(rtask, '_value', None)
                self.__rtasks[rtask] = rtask
                if self.status_task:
                    msg = DispycosTaskInfo(rtask, args, kwargs, time.time())
                    self.status_task.send(DispycosStatus(Scheduler.TaskStarted, msg))
            raise StopIteration(rtask)

        raise StopIteration((yield Task(_job_req).finish()))

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

            elif isinstance(msg, dict):
                if msg.get('auth', None) != self._auth:
                    continue
                last_pulse = time.time()
                req = msg.get('req', None)

                if isinstance(req, MonitorStatus):
                    # TODO: process in Scheduler if not shared
                    if not isinstance(req.info, Task):
                        if isinstance(req.info, str) and isinstance(req.value, str):
                            pycos.logger.info('%s: %s with %s', req.info, req.type, req.value)
                        else:
                            pycos.logger.warning('invalid MonitorStatus message ignored: %s',
                                                 type(req.info))
                        continue
                    rtask = self.__rtasks.pop(req.info, None)
                    if rtask:
                        req.info = rtask
                        if req.type == StopIteration:
                            pycos.logger.debug('rtask %s done', rtask)
                            rtask._value = req.value
                        elif req.type == Scheduler.TaskTerminated:
                            pycos.logger.warning('rtask %s terminated', rtask)
                        elif req.type == Scheduler.TaskAbandoned:
                            pycos.logger.warning('rtask %s abandoned', rtask)
                        else:
                            rtask._value = req
                            pycos.logger.warning('rtask %s failed: %s with %s',
                                                 rtask, req.type, req.value)
                        rtask._complete.set()
                        if self.status_task:
                            self.status_task.send(req)
                    else:
                        rtask = req.info
                        setattr(rtask, '_value', req)
                        self.__askew_tasks[rtask] = rtask

                elif req == 'allocate':
                    reply_task = msg.get('reply_task', None)
                    args = msg.get('args', ())
                    if not isinstance(reply_task, Task) or not args:
                        logger.warning('Ignoring allocate request: %s', type(reply_task))
                        continue
                    ip_addr = args[0]
                    try:
                        node_allocation = self._node_allocations[int(msg['alloc_id'])]
                        assert re.match(node_allocation.ip_rex, ip_addr)
                        cpus = node_allocation.allocate(*args)
                    except Exception:
                        cpus = 0
                    reply_task.send({'auth': self._auth, 'req': 'allocate',
                                     'ip_addr': ip_addr, 'cpus': cpus})

                else:
                    pycos.logger.debug('Ignoring message: %s', type(msg))

            elif msg == 'quit':
                break

            elif msg is None:
                logger.warning('scheduler is not reachable!')
                if (time.time() - last_pulse) > (10 * self._pulse_interval):
                    # TODO: inform status and / or "close"?
                    pass

            else:
                logger.debug('ignoring invalid pulse message')

    def __getstate__(self):
        state = {}
        for attr in ['_auth', '_code', 'status_task', '_xfer_files', '_node_setup', '_server_setup',
                     '_disable_nodes', '_disable_servers', '_pulse_interval', '_pulse_task',
                     '_ping_interval', '_restart_servers', '_zombie_period', '_abandon_zombie']:
            state[attr] = getattr(self, attr)
        if (isinstance(self._pulse_task, Task) and
            isinstance(getattr(self, '__scheduler', None), Task) and
            self._pulse_task.location == self.__scheduler.location):
            node_allocations = self._node_allocations
        else:
            node_allocations = []
            for i in range(len(self._node_allocations)):
                obj = self._node_allocations[i]
                if obj.__class__ != DispycosNodeAllocate:
                    ip_rex = obj.ip_rex
                    obj = DispycosNodeAllocate('*', port=obj.port)
                    obj.ip_rex = ip_rex
                    obj.cpus = str(i)
                node_allocations.append(obj)
        state['_node_allocations'] = node_allocations
        return state

    def __setstate__(self, state):
        for attr, value in state.items():
            setattr(self, attr, value)


# for backward compatability
Computation = Client


class _DispycosJob_(object):
    """Internal use only.
    """
    __slots__ = ('name', 'where', 'cpu', 'code', 'args', 'kwargs', 'done')

    def __init__(self, name, where, cpu, code, args=None, kwargs=None):
        self.name = name
        self.where = where
        self.cpu = cpu
        self.code = code
        self.args = pycos.serialize(args)
        self.kwargs = pycos.serialize(kwargs)


class Scheduler(object, metaclass=pycos.Singleton):

    # status indications ('status' attribute of DispycosStatus)
    NodeIgnore = 1
    NodeDiscovered = 2
    NodeInitialized = 3
    NodeSuspended = 4
    NodeResumed = 5
    NodeClosed = 6
    NodeDisconnected = 7
    NodeAbandoned = 8

    # ServerIgnore = 11
    ServerDiscovered = 12
    ServerInitialized = 13
    ServerSuspended = 14
    ServerResumed = 15
    ServerClosed = 16
    ServerDisconnected = 17
    ServerAbandoned = 18

    TaskStarted = 21
    TaskFinished = 22
    TaskAbandoned = 23
    TaskTerminated = 24
    TaskCreated = TaskStarted

    ClientScheduled = 31
    ClientClosed = 32

    """This class is for use by Client class (see below) only.  Other than
    the status indications above, none of its attributes are to be accessed
    directly.
    """

    class _Node(object):

        def __init__(self, name, addr):
            self.name = name
            self.addr = addr
            self.cpus_used = 0
            self.cpus = 0
            self.platform = None
            self.avail_info = None
            self.servers = {}
            self.disabled_servers = {}
            self.load = 0.0
            self.status = Scheduler.NodeClosed
            self.task = None
            self.auth = None
            self.last_pulse = time.time()
            self.lock = pycos.Lock()
            self.cpu_avail = pycos.Event()
            self.cpu_avail.clear()
            self.abandon_zombie = False

    class _Server(object):

        def __init__(self, task, scheduler):
            self.task = task
            self.status = Scheduler.ServerClosed
            self.rtasks = {}
            self.xfer_files = []
            self.askew_results = {}
            self.cpu_avail = pycos.Event()
            self.cpu_avail.clear()
            self.scheduler = scheduler
            self.name = None
            self.pid = None
            self.done = pycos.Event()

        def run(self, job, reply_task, node):
            def _run(self, task=None):
                self.task.send({'req': 'task', 'auth': node.auth, 'job': job, 'reply_task': task})
                rtask = yield task.receive(timeout=MsgTimeout)
                # currently fault-tolerancy is not supported, so clear job's
                # args to save space
                job.args = job.kwargs = None
                if isinstance(rtask, Task):
                    # TODO: keep func too for fault-tolerance
                    self.rtasks[rtask] = job
                    if self.askew_results:
                        msg = self.askew_results.pop(rtask, None)
                        if msg:
                            self.scheduler.__status_task.send(msg)
                else:
                    if job.cpu:
                        self.cpu_avail.set()
                        if (self.status == Scheduler.ServerInitialized and
                            node.status == Scheduler.NodeInitialized):
                            node.cpu_avail.set()
                            self.scheduler._cpu_nodes.add(node)
                            self.scheduler._cpus_avail.set()
                            node.cpus_used -= 1
                            node.load = float(node.cpus_used) / len(node.servers)
                raise StopIteration(rtask)

            rtask = yield SysTask(_run, self).finish()
            reply_task.send(rtask)

    def __init__(self, **kwargs):
        self._nodes = {}
        self._disabled_nodes = {}
        self._cpu_nodes = set()
        self._cpus_avail = pycos.Event()
        self._cpus_avail.clear()
        self._remote = False

        self.__client = None
        self.__client_auth = None
        self.__cur_node_allocations = []
        self.__pulse_interval = kwargs.pop('pulse_interval', MaxPulseInterval)
        self.__ping_interval = kwargs.pop('ping_interval', 0)
        self.__zombie_period = kwargs.pop('zombie_period', 100 * MaxPulseInterval)
        if not isinstance(pycos.config.DispycosSchedulerPort, int):
            pycos.config.DispycosSchedulerPort = eval(pycos.config.DispycosSchedulerPort)
        if not isinstance(pycos.config.DispycosNodePort, int):
            pycos.config.DispycosNodePort = eval(pycos.config.DispycosNodePort)
        self._node_port = pycos.config.DispycosNodePort

        kwargs['name'] = 'dispycos_scheduler'
        clean = kwargs.pop('clean', False)
        nodes = kwargs.pop('nodes', [])
        relay_nodes = kwargs.pop('relay_nodes', False)
        kwargs['udp_port'] = kwargs['tcp_port'] = pycos.config.DispycosSchedulerPort
        self.pycos = pycos.Pycos.instance(**kwargs)
        self.__dest_path = os.path.join(self.pycos.dest_path, 'dispycos', 'scheduler')
        if clean:
            shutil.rmtree(self.__dest_path)
        self.pycos.dest_path = self.__dest_path
        os.chmod(self.__dest_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

        self.__client_sched_event = pycos.Event()
        self.__client_scheduler_task = SysTask(self.__client_scheduler_proc)
        self.__status_task = SysTask(self.__status_proc)
        self.__timer_task = SysTask(self.__timer_proc)
        self.__client_task = SysTask(self.__client_proc)
        self.__client_task.register('dispycos_scheduler')
        for node in nodes:
            if not isinstance(node, Location):
                node = Location(node, self._node_port)
            Task(self.pycos.peer, node, relay=relay_nodes)

    def status(self):
        pending_cpu = sum(node.cpus_used for node in self._nodes.values())
        pending = sum(len(server.rtasks) for node in self._nodes.values()
                      for server in node.servers.values())
        servers = functools.reduce(operator.add, [list(node.servers.keys())
                                                  for node in self._nodes.values()], [])
        return {'Client': self.__client._pulse_task.location if self.__client else '',
                'Pending': pending, 'PendingCPU': pending_cpu,
                'Nodes': list(self._nodes.keys()), 'Servers': servers
                }

    def print_status(self):
        status = self.status()
        print('')
        print('  Client: %s' % status['Client'])
        print('  Pending: %s' % status['Pending'])
        print('  Pending CPU: %s' % status['PendingCPU'])
        print('  nodes: %s' % len(status['Nodes']))
        print('  servers: %s' % len(status['Servers']))

    def __status_proc(self, task=None):
        task.set_daemon()
        task.register('dispycos_status')
        self.pycos.peer_status(self.__status_task)
        while 1:
            msg = yield task.receive()
            now = time.time()
            if isinstance(msg, MonitorStatus):
                if not isinstance(msg.info, Task):
                    if self._remote and self.__client:
                        self.__client._pulse_task.send({'req': msg, 'auth': self.__client_auth})
                    else:
                        if isinstance(msg.info, str) and isinstance(msg.value, str):
                            logger.info('%s: %s with %s', msg.info, msg.type, msg.value)
                        else:
                            logger.warning('invalid MonitorStatus ignored: %s', type(msg.info))
                    continue
                rtask = msg.info
                node = self._nodes.get(rtask.location.addr, None)
                if not node:
                    node = self._disabled_nodes.get(rtask.location.addr, None)
                    if not node:
                        logger.warning('node %s is invalid', rtask.location.addr)
                        continue
                    if node.status == Scheduler.NodeAbandoned:
                        SysTask(self.__reclaim_node, node)

                server = node.servers.get(rtask.location, None)
                if not server:
                    server = node.disabled_servers.get(rtask.location, None)
                    if not server:
                        logger.warning('server "%s" is invalid', rtask.location)
                        continue
                node.last_pulse = now
                job = server.rtasks.pop(rtask, None)
                if not job:
                    # Due to 'yield' used to create rtask, scheduler may not have updated
                    # self._rtasks before the task's MonitorStatus is received, so put it in
                    # 'askew_results'. The scheduling task will resend it when it receives rtask
                    server.askew_results[rtask] = msg
                    continue
                # assert isinstance(job, _DispycosJob_)
                if job.cpu:
                    server.cpu_avail.set()
                    if (server.status == Scheduler.ServerInitialized and
                        node.status == Scheduler.NodeInitialized):
                        node.cpu_avail.set()
                        self._cpu_nodes.add(node)
                        self._cpus_avail.set()
                        node.cpus_used -= 1
                        node.load = float(node.cpus_used) / len(node.servers)
                if self.__client:
                    self.__client._pulse_task.send({'req': msg, 'auth': self.__client_auth})

            elif isinstance(msg, pycos.PeerStatus):
                if msg.status == pycos.PeerStatus.Online:
                    if msg.name.endswith('_dispycosnode'):
                        SysTask(self.__discover_node, msg)
                else:
                    # msg.status == pycos.PeerStatus.Offline
                    node = server = None
                    node = self._nodes.get(msg.location.addr, None)
                    if not node:
                        node = self._disabled_nodes.get(msg.location.addr, None)
                    if node:
                        server = node.servers.pop(msg.location, None)
                        if server:
                            node.disabled_servers[msg.location] = server
                        else:
                            server = node.disabled_servers.get(msg.location, None)

                        if server:
                            server.status = Scheduler.ServerDisconnected
                            SysTask(self.__close_server, server, server.pid, node)
                        elif node.task and node.task.location == msg.location:
                            # TODO: inform scheduler / client
                            if self._nodes.pop(node.addr, None):
                                self._disabled_nodes[node.addr] = node
                            node.status = Scheduler.NodeDisconnected
                            SysTask(self.__close_node, node)

                    if ((not server and not node) and self._remote and self.__client and
                        self.__client._pulse_task.location == msg.location):
                        logger.warning('Client %s terminated; closing client %s',
                                       msg.location, self.__client_auth)
                        SysTask(self.__close_client)

            elif isinstance(msg, dict):  # message from a node's server
                status = msg.get('status', None)
                if status == 'pulse':
                    location = msg.get('location', None)
                    if not isinstance(location, Location):
                        continue
                    node = self._nodes.get(location.addr, None)
                    if node:
                        node.last_pulse = now
                        node_status = msg.get('node_status', None)
                        if (node_status and self.__client and
                           self.__client.status_task):
                            self.__client.status_task.send(node_status)
                    else:
                        node = self._disabled_nodes.get(location.addr, None)
                        if node and node.status == Scheduler.NodeAbandoned:
                            SysTask(self.__reclaim_node, node)

                elif status == Scheduler.ServerDiscovered:
                    rtask = msg.get('task', None)
                    if not isinstance(rtask, Task):
                        continue
                    node = self._nodes.get(rtask.location.addr, None)
                    if not node:
                        node = self._disabled_nodes.get(rtask.location.addr, None)
                    if not node or (node.status != Scheduler.NodeInitialized and
                                    node.status != Scheduler.NodeDiscovered and
                                    node.status != Scheduler.NodeSuspended):
                        logger.warning('Node is not valid for server %s: %s', rtask.location,
                                       node.status if node else None)
                        continue
                    if node.auth != msg.get('auth', None):
                        logger.warning('Ignoring status %s from server %s', status, rtask.location)
                        continue
                    server = node.servers.get(rtask.location, None)
                    if server and server.pid == msg.get('pid', None):
                        continue
                    server = Scheduler._Server(rtask, self)
                    server.status = status
                    server.pid = msg.get('pid', None)
                    node.disabled_servers[rtask.location] = server
                    if (node.status == Scheduler.NodeInitialized and self.__client and
                        self.__client.status_task):
                        info = DispycosStatus(server.status, server.task.location)
                        self.__client.status_task.send(info)

                elif status == Scheduler.ServerInitialized:
                    rtask = msg.get('task', None)
                    if not isinstance(rtask, Task):
                        continue
                    node = self._nodes.get(rtask.location.addr, None)
                    if not node:
                        node = self._disabled_nodes.get(rtask.location.addr, None)
                    if not node or (node.status != Scheduler.NodeInitialized and
                                    node.status != Scheduler.NodeDiscovered and
                                    node.status != Scheduler.NodeSuspended):
                        logger.warning('Node is not valid for server %s: %s', rtask.location,
                                       node.status if node else None)
                        continue
                    if node.auth != msg.get('auth', None):
                        logger.warning('Ignoring status %s from server %s', status, rtask.location)
                        continue
                    server = node.disabled_servers.pop(rtask.location, None)
                    if server and server.pid == msg.get('pid', None):
                        # if (server.status != Scheduler.ServerDiscovered and
                        #     server.status != Scheduler.ServerSuspended):
                        #     continue
                        server.task = rtask
                    else:
                        server = Scheduler._Server(rtask, self)

                    server.name = msg.get('name')
                    server.status = status
                    server.pid = msg.get('pid', None)
                    node.last_pulse = now
                    if node.status == Scheduler.NodeInitialized:
                        if not node.servers:
                            if self.__client and self.__client.status_task:
                                info = DispycosNodeInfo(node.name, node.addr, node.cpus,
                                                        node.platform, node.avail_info)
                                info = DispycosStatus(node.status, info)
                                self.__client.status_task.send(info)
                            self._disabled_nodes.pop(rtask.location.addr, None)
                            self._nodes[rtask.location.addr] = node
                        node.servers[rtask.location] = server
                        server.cpu_avail.set()
                        self._cpu_nodes.add(node)
                        self._cpus_avail.set()
                        node.cpu_avail.set()
                        node.load = float(node.cpus_used) / len(node.servers)
                        if self.__client and self.__client.status_task:
                            self.__client.status_task.send(
                                DispycosStatus(server.status, server.task.location))
                    else:
                        node.disabled_servers[rtask.location] = server

                elif status in (Scheduler.ServerClosed, Scheduler.ServerDisconnected):
                    location = msg.get('location', None)
                    if not isinstance(location, Location):
                        continue
                    node = self._nodes.get(location.addr, None)
                    if not node:
                        node = self._disabled_nodes.get(location.addr, None)
                        if not node:
                            continue
                    if node.auth != msg.get('auth', None):
                        logger.warning('Ignoring status %s from server %s', status, rtask.location)
                        continue
                    server = node.servers.pop(location, None)
                    if server:
                        if server.pid != msg.get('pid', None):
                            logger.warning('Ignoring status %s from server %s: invalid PID: %s / %s',
                                           status, location, server.pid, msg.get('pid', None))
                        node.disabled_servers[location] = server
                    else:
                        server = node.disabled_servers.get(location, None)
                        if not server:
                            continue
                        if server.pid != msg.get('pid', None):
                            logger.warning('Ignoring status %s from server %s: invalid PID: %s / %s',
                                           status, location, server.pid, msg.get('pid', None))
                    server.status = status
                    SysTask(self.__close_server, server, server.pid, node)

                elif status == Scheduler.NodeClosed:
                    location = msg.get('location', None)
                    if not isinstance(location, Location):
                        continue
                    node = self._nodes.pop(location.addr, None)
                    if node:
                        self._disabled_nodes[location.addr] = node
                    else:
                        node = self._disabled_nodes.get(location.addr, None)
                        if not node:
                            continue
                    if node.auth != msg.get('auth', None):
                        logger.warning('Ignoring status %s from node %s', status, location)
                        continue
                    node.status = status
                    SysTask(self.__close_node, node)

                else:
                    logger.warning('Ignoring invalid status message: %s', status)
            else:
                logger.warning('invalid status message ignored')

    def __node_allocate(self, node, task=None):
        if not task:
            task = pycos.Pycos.cur_task()
        for node_allocate in self.__cur_node_allocations:
            if not re.match(node_allocate.ip_rex, node.addr):
                continue
            if self._remote and isinstance(node_allocate.cpus, str):
                req = {'req': 'allocate', 'auth': self.__client_auth,
                       'alloc_id': node_allocate.cpus, 'reply_task': task,
                       'args': (node.addr, node.name, node.platform, node.cpus,
                                node.avail_info.memory, node.avail_info.disk)}
                self.__client._pulse_task.send(req)
                resp = yield task.recv(timeout=MsgTimeout)
                if (isinstance(resp, dict) and resp.get('auth', None) == self.__client_auth and
                    resp.get('req', None) == 'allocate' and resp.get('ip_addr', '') == node.addr):
                    cpus = resp.get('cpus', 0)
                else:
                    cpus = 0
            else:
                cpus = node_allocate.allocate(node.addr, node.name, node.platform, node.cpus,
                                              node.avail_info.memory, node.avail_info.disk)
            if cpus < 0:
                continue
            raise StopIteration(min(cpus, node.cpus))
        raise StopIteration(node.cpus)

    def __get_node_info(self, node, task=None):
        assert node.addr in self._disabled_nodes
        node.task.send({'req': 'dispycos_node_info', 'reply_task': task})
        node_info = yield task.receive(timeout=MsgTimeout)
        if not node_info:
            node.status = Scheduler.NodeIgnore
            raise StopIteration
        node.name = node_info.name
        node.cpus = node_info.cpus
        node.platform = node_info.platform.lower()
        node.avail_info = node_info.avail_info
        if self.__client:
            yield self.__init_node(node, task=task)

    def __init_node(self, node, setup_args=pycos.serialize(()), task=None):
        client = self.__client
        if not client or not node.task:
            raise StopIteration(-1)
        # this task may be invoked in two different paths (when a node is
        # found right after client is already scheduled, and when
        # client is scheduled right after a node is found). To prevent
        # concurrent execution (that may reserve / initialize same node more
        # than once), lock is used
        yield node.lock.acquire()
        # assert node.addr in self._disabled_nodes
        if node.status not in (Scheduler.NodeDiscovered, Scheduler.NodeClosed):
            logger.warning('Ignoring node initialization for %s: %s', node.addr, node.status)
            node.lock.release()
            raise StopIteration(0)

        if node.status == Scheduler.NodeClosed:
            cpus = yield self.__node_allocate(node, task=task)
            if not cpus:
                node.status = Scheduler.NodeIgnore
                node.lock.release()
                raise StopIteration(0)

            node.task.send({'req': 'reserve', 'cpus': cpus, 'status_task': self.__status_task,
                            'reply_task': task, 'client_location': client._pulse_task.location,
                            'abandon_zombie': client._abandon_zombie,
                            'pulse_interval': client._pulse_interval})
            resp = yield task.receive(timeout=MsgTimeout)
            if not isinstance(resp, dict) or resp.get('cpus', 0) <= 0:
                logger.debug('Reserving %s failed', node.addr)
                self._disabled_nodes.pop(node.addr, None)
                # node.status = Scheduler.NodeDiscoverd
                node.lock.release()
                yield pycos.Pycos.instance().close_peer(node.task.location)
                raise StopIteration(-1)
            if client != self.__client:
                node.status = Scheduler.NodeClosed
                node.task.send({'req': 'release', 'auth': node.auth, 'reply_task': None})
                node.lock.release()
                raise StopIteration(-1)

            node.status = Scheduler.NodeDiscovered
            node.cpus = resp['cpus']
            node.auth = resp['auth']
            if self.__client and self.__client.status_task:
                info = DispycosNodeInfo(node.name, node.addr, node.cpus, node.platform,
                                        node.avail_info)
                self.__client.status_task.send(DispycosStatus(node.status, info))

            if self.__client._disable_nodes:
                node.lock.release()
                raise StopIteration(0)
        else:
            assert node.addr in self._disabled_nodes

        for name, dst, sep in client._xfer_files:
            resp = yield self.pycos.send_file(node.task.location, name, dir=dst, timeout=MsgTimeout,
                                              overwrite=True)
            if resp < 0 or client != self.__client:
                logger.debug('Failed to transfer file %s: %s', name, resp)
                node.status = Scheduler.NodeClosed
                node.task.send({'req': 'release', 'auth': node.auth, 'reply_task': None})
                node.lock.release()
                raise StopIteration(-1)

        info = copy.copy(client)
        info._xfer_files = []
        info.status_task = None
        info._zombie_period = None
        node.task.send({'req': 'client', 'client': pycos.serialize(info),
                        'auth': node.auth, 'setup_args': setup_args,
                        'restart_servers': client._restart_servers, 'reply_task': task})
        reply = yield task.receive(timeout=MsgTimeout)
        if not isinstance(reply, int) or reply <= 0 or client != self.__client:
            if client._pulse_task:
                client._pulse_task.send({'req': reply, 'auth': self.__client_auth})
            node.status = Scheduler.NodeClosed
            node.task.send({'req': 'release', 'auth': node.auth, 'reply_task': None})
            node.lock.release()
            raise StopIteration(-1)

        node.cpus = reply
        node.status = Scheduler.NodeInitialized
        node.lock.release()
        if client.status_task:
            info = DispycosNodeInfo(node.name, node.addr, node.cpus, node.platform, node.avail_info)
            client.status_task.send(DispycosStatus(node.status, info))
        for server in list(node.disabled_servers.values()):
            if server.status == Scheduler.ServerDiscovered:
                if client.status_task:
                    info = DispycosStatus(server.status, server.task.location)
                    self.__client.status_task.send(info)
            elif server.status == Scheduler.ServerInitialized:
                node.disabled_servers.pop(server.task.location)
                node.servers[server.task.location] = server
                server.cpu_avail.set()
                if client.status_task:
                    client.status_task.send(DispycosStatus(server.status, server.task.location))
        if node.servers:
            self._disabled_nodes.pop(node.addr, None)
            self._nodes[node.addr] = node
            node.cpu_avail.set()
            self._cpu_nodes.add(node)
            self._cpus_avail.set()

    def __discover_node(self, peer_status, task=None):
        for _ in range(10):
            node_task = yield Task.locate('dispycos_node', location=peer_status.location,
                                          timeout=MsgTimeout)
            if not isinstance(node_task, Task):
                yield task.sleep(0.1)
                continue
            node = self._nodes.pop(peer_status.location.addr, None)
            if not node:
                node = self._disabled_nodes.pop(peer_status.location.addr, None)
            if node:
                logger.warning('Rediscovered dispycosnode at %s', peer_status.location.addr)
                if node_task == node.task and node.status == Scheduler.NodeAbandoned:
                    SysTask(self.__reclaim_node, node)
                    raise StopIteration
                node.status = Scheduler.NodeDisconnected
                yield SysTask(self.__close_node, node).finish()
            node = Scheduler._Node(peer_status.name, peer_status.location.addr)
            self._disabled_nodes[peer_status.location.addr] = node
            node.task = node_task
            yield self.__get_node_info(node, task=task)
            raise StopIteration

    def __reclaim_node(self, node, task=None):
        client = self.__client
        if not client:
            raise StopIteration
        node.task.send({'req': 'status', 'status_task': self.__status_task, 'reply_task': task,
                        'auth': node.auth})
        status = yield task.recv(timeout=MsgTimeout)
        if not isinstance(status, dict):
            logger.debug('dispycosnode %s is used by another scheduler', node.addr)
            raise StopIteration(-1)
        self._disabled_nodes.pop(node.addr, None)
        node.disabled_servers.update(node.servers)
        node.servers.clear()
        if client == self.__client and client._auth == status.get('client_auth'):
            for rtask in status.get('servers', []):
                server = node.disabled_servers.pop(rtask.location, None)
                if not server:
                    logger.warning('Invalid server %s ignored', rtask)
                    continue
                # TODO: get number of CPU jobs only
                server.task.send({'req': 'num_jobs', 'auth': client._auth, 'reply_task': task})
                n = yield task.receive(timeout=MsgTimeout)
                if not isinstance(n, int):
                    continue
                if n == 0:
                    server.cpu_avail.set()
                else:
                    server.cpu_avail.clear()
                server.status = Scheduler.ServerInitialized
                node.servers[rtask.location] = server
            node.status = Scheduler.NodeInitialized
            node.last_pulse = time.time()
            if any(server.cpu_avail.is_set() for server in node.servers.values()):
                node.cpu_avail.set()
                self._cpu_nodes.add(node)
                self._cpus_avail.set()
            self._nodes[node.addr] = node
            logger.debug('Rediscovered node %s with %s servers', node.addr, len(node.servers))
            raise StopIteration

        node.status = Scheduler.NodeDisconnected
        yield SysTask(self.__close_node, node).finish()
        self._disabled_nodes[node.addr] = node
        yield self.__get_node_info(node, task=task)
        raise StopIteration

    def __timer_proc(self, task=None):
        task.set_daemon()
        node_check = client_pulse = last_ping = time.time()
        while 1:
            try:
                yield task.sleep(self.__pulse_interval)
            except GeneratorExit:
                break
            now = time.time()
            if self.__client_auth:
                if (yield self.__client._pulse_task.deliver('pulse')) == 1:
                    client_pulse = now
                if self.__zombie_period:
                    if ((now - client_pulse) > self.__zombie_period):
                        logger.warning('Closing zombie client %s', self.__client_auth)
                        SysTask(self.__close_client)

                    if (now - node_check) > self.__zombie_period:
                        node_check = now
                        for node in list(self._nodes.values()):
                            if (node.status != Scheduler.NodeInitialized and
                                node.status != Scheduler.NodeDiscovered and
                                node.status != Scheduler.NodeSuspended):
                                continue
                            if (now - node.last_pulse) > self.__zombie_period:
                                logger.warning('dispycos node %s is zombie!', node.addr)
                                self._nodes.pop(node.addr, None)
                                self._disabled_nodes[node.addr] = node
                                # TODO: assuming servers are zombies as well
                                node.status = Scheduler.NodeAbandoned
                                SysTask(self.__close_node, node)

                        if not self.__client._disable_nodes:
                            for node in self._disabled_nodes.values():
                                if node.task and node.status == Scheduler.NodeDiscovered:
                                    SysTask(self.__init_node, node)

            if self.__ping_interval and ((now - last_ping) > self.__ping_interval):
                last_ping = now
                if not self.pycos.ignore_peers:
                    self.pycos.discover_peers(port=self._node_port)

    def __client_scheduler_proc(self, task=None):
        task.set_daemon()
        while 1:
            if self.__client:
                self.__client_sched_event.clear()
                yield self.__client_sched_event.wait()
                continue

            self.__client, reply_task = yield task.receive()
            self.__pulse_interval = self.__client._pulse_interval
            self.__ping_interval = self.__client._ping_interval
            if not self._remote:
                self.__zombie_period = self.__client._zombie_period

            self.__client_auth = self.__client._auth
            self.__cur_node_allocations = self.__client._node_allocations
            self.__client._node_allocations = []

            self._disabled_nodes.update(self._nodes)
            self._nodes.clear()
            self._cpu_nodes.clear()
            self._cpus_avail.clear()
            for node in self._disabled_nodes.values():
                node.status = Scheduler.NodeClosed
                node.disabled_servers.clear()
                node.servers.clear()
                node.cpu_avail.clear()
            logger.debug('Client %s scheduled', self.__client_auth)
            msg = {'resp': 'scheduled', 'auth': self.__client_auth}
            if (yield reply_task.deliver(msg, timeout=MsgTimeout)) != 1:
                logger.warning('client not reachable?')
                self.__client_auth = self.__client = None
                continue
            for node in self.__cur_node_allocations:
                if node.ip_rex.find('*') >= 0:
                    continue
                loc = Location(node.ip_rex.replace('\\.', '.'),
                               node.port if node.port else self._node_port)
                SysTask(self.pycos.peer, loc)
            for node in self._disabled_nodes.values():
                SysTask(self.__get_node_info, node)
            if not self.pycos.ignore_peers:
                self.pycos.discover_peers(port=self._node_port)
            self.__timer_task.resume()
        self.__client_scheduler_task = None

    def __submit_job(self, msg, task=None):
        task.set_daemon()
        job = msg['job']
        auth = msg.get('auth', None)
        reply_task = msg.get('reply_task', None)
        if (not isinstance(job, _DispycosJob_) or not isinstance(reply_task, Task)):
            logger.warning('Ignoring invalid client job request: %s' % type(job))
            raise StopIteration
        cpu = job.cpu
        where = job.where
        if not where:
            while 1:
                node = None
                load = None
                if cpu:
                    for host in self._cpu_nodes:
                        if host.cpu_avail.is_set() and (load is None or host.load < load):
                            node = host
                            load = host.load
                else:
                    for host in self._nodes.values():
                        if load is None or host.load < load:
                            node = host
                            load = host.load
                if not node:
                    self._cpus_avail.clear()
                    yield self._cpus_avail.wait()
                    if self.__client_auth != auth:
                        raise StopIteration
                    continue
                server = None
                load = None
                for proc in node.servers.values():
                    if cpu:
                        if proc.cpu_avail.is_set() and (load is None or len(proc.rtasks) < load):
                            server = proc
                            load = len(proc.rtasks)
                    elif (load is None or len(proc.rtasks) < load):
                        server = proc
                        load = len(proc.rtasks)
                if server:
                    break
                else:
                    self._cpus_avail.clear()
                    yield self._cpus_avail.wait()
                    if self.__client_auth != auth:
                        raise StopIteration
                    continue

            if cpu:
                server.cpu_avail.clear()
                node.cpus_used += 1
                node.load = float(node.cpus_used) / len(node.servers)
                if node.cpus_used == len(node.servers):
                    node.cpu_avail.clear()
                    self._cpu_nodes.discard(node)
                    if not self._cpu_nodes:
                        self._cpus_avail.clear()
            yield server.run(job, reply_task, node)

        elif isinstance(where, str):
            node = self._nodes.get(where, None)
            if not node:
                reply_task.send(None)
                raise StopIteration
            while 1:
                server = None
                load = None
                for proc in node.servers.values():
                    if cpu:
                        if proc.cpu_avail.is_set() and (load is None or len(proc.rtasks) < load):
                            server = proc
                            load = len(proc.rtasks)
                    elif (load is None or len(proc.rtasks) < load):
                        server = proc
                        load = len(proc.rtasks)

                if server:
                    break
                else:
                    yield node.cpu_avail.wait()
                    if self.__client_auth != auth:
                        raise StopIteration
                    continue

            if cpu:
                server.cpu_avail.clear()
                node.cpus_used += 1
                node.load = float(node.cpus_used) / len(node.servers)
                if node.cpus_used >= len(node.servers):
                    node.cpu_avail.clear()
                    self._cpu_nodes.discard(node)
                    if not self._cpu_nodes:
                        self._cpus_avail.clear()
            yield server.run(job, reply_task, node)

        elif isinstance(where, Location):
            node = self._nodes.get(where.addr)
            if not node:
                reply_task.send(None)
                raise StopIteration
            server = node.servers.get(where)
            if not server:
                reply_task.send(None)
                raise StopIteration
            if cpu:
                while (not node.cpu_avail.is_set() or not server.cpu_avail.is_set()):
                    yield server.cpu_avail.wait()
                    if self.__client_auth != auth:
                        raise StopIteration
                server.cpu_avail.clear()
                node.cpus_used += 1
                node.load = float(node.cpus_used) / len(node.servers)
                if node.cpus_used >= len(node.servers):
                    node.cpu_avail.clear()
                    self._cpu_nodes.discard(node)
                    if not self._cpu_nodes:
                        self._cpus_avail.clear()
            yield server.run(job, reply_task, node)

        else:
            reply_task.send(None)

    def __client_proc(self, task=None):
        task.set_daemon()
        clients = {}

        while 1:
            msg = yield task.receive()
            if not isinstance(msg, dict):
                continue
            req = msg.get('req', None)
            auth = msg.get('auth', None)
            if self.__client_auth != auth:
                if req == 'schedule' or req == 'await':
                    pass
                else:
                    continue

            if req == 'job':
                SysTask(self.__submit_job, msg)
                continue

            reply_task = msg.get('reply_task', None)
            if not isinstance(reply_task, Task):
                reply_task = None

            if req == 'enable_server':
                loc = msg.get('server', None)
                if not isinstance(loc, Location):
                    continue
                node = self._nodes.get(loc.addr, None)
                if not node:
                    node = self._disabled_nodes.get(loc.addr, None)
                if not node or node.status not in (Scheduler.NodeInitialized,
                                                   Scheduler.NodeSuspended):
                    continue
                server = node.disabled_servers.get(loc, None)
                if not server or server.status not in (Scheduler.ServerDiscovered,
                                                       Scheduler.ServerSuspended):
                    continue
                if server.status == Scheduler.ServerDiscovered:
                    args = msg.get('setup_args', pycos.serialize(()))
                    server.task.send({'req': 'enable_server', 'setup_args': args,
                                      'auth': node.auth})
                elif server.status == Scheduler.ServerSuspended:
                    node.disabled_servers.pop(loc)
                    node.servers[loc] = server
                    server.status = Scheduler.ServerInitialized
                    server.cpu_avail.set()
                    if len(node.servers) == 1:
                        node.cpu_avail.set()
                        self._cpu_nodes.add(node)
                        self._cpus_avail.set()
                    if self.__client.status_task:
                        info = DispycosStatus(Scheduler.ServerResumed, loc)
                        self.__client.status_task.send(info)
                    if node.status == Scheduler.NodeSuspended:
                        self._disabled_nodes.pop(node.addr, None)
                        self._nodes[node.addr] = node
                        node.status = Scheduler.NodeInitialized
                        if self.__client.status_task:
                            info = DispycosNodeInfo(node.name, node.addr, node.cpus, node.platform,
                                                    node.avail_info)
                            info = DispycosStatus(Scheduler.NodeResumed, info)
                            self.__client.status_task.send(info)

            elif req == 'enable_node':
                addr = msg.get('addr', None)
                if not addr:
                    continue
                node = self._disabled_nodes.get(addr, None)
                if not node or node.status not in (Scheduler.NodeDiscovered,
                                                   Scheduler.NodeSuspended):
                    continue
                if node.status == Scheduler.NodeDiscovered:
                    setup_args = msg.get('setup_args', pycos.serialize(()))
                    SysTask(self.__init_node, node, setup_args=setup_args)
                elif node.status == Scheduler.NodeSuspended:
                    if node.servers:
                        node.status = Scheduler.NodeInitialized
                        self._disabled_nodes.pop(addr, None)
                        self._nodes[node.addr] = node
                        node.cpu_avail.set()
                        self._cpu_nodes.add(node)
                        self._cpus_avail.set()
                        if self.__client.status_task:
                            info = DispycosNodeInfo(node.name, node.addr, node.cpus, node.platform,
                                                    node.avail_info)
                            info = DispycosStatus(Scheduler.NodeResumed, info)
                            self.__client.status_task.send(info)

            elif req == 'suspend_server':
                loc = msg.get('server', None)
                if not isinstance(loc, Location):
                    continue
                node = self._nodes.get(loc.addr, None)
                if not node:
                    continue
                server = node.servers.pop(loc, None)
                if not server:
                    continue
                if server.status not in (Scheduler.ServerInitialized, Scheduler.ServerDiscovered):
                    node.servers[loc] = server
                    continue
                node.disabled_servers[loc] = server
                if server.status == Scheduler.ServerInitialized:
                    server.status = Scheduler.ServerSuspended
                server.cpu_avail.clear()
                if self.__client.status_task:
                    info = DispycosStatus(server.status, loc)
                    self.__client.status_task.send(info)
                if not node.servers:
                    self._nodes.pop(node.addr)
                    self._disabled_nodes[node.addr] = node
                    node.status = Scheduler.NodeSuspended
                    node.cpu_avail.clear()
                    self._cpu_nodes.discard(node)
                    if not self._cpu_nodes:
                        self._cpus_avail.clear()
                    if self.__client.status_task:
                        info = DispycosNodeInfo(node.name, node.addr, node.cpus, node.platform,
                                                node.avail_info)
                        info = DispycosStatus(node.status, info)
                        self.__client.status_task.send(info)

            elif req == 'suspend_node':
                addr = msg.get('addr', None)
                if not addr:
                    continue
                node = self._nodes.pop(addr, None)
                if not node:
                    continue
                if node.status not in (Scheduler.NodeInitialized, Scheduler.NodeDiscovered):
                    self._nodes[addr] = node
                    continue
                self._disabled_nodes[node.addr] = node
                if node.status == Scheduler.NodeInitialized:
                    node.status = Scheduler.NodeSuspended
                node.cpu_avail.clear()
                self._cpu_nodes.discard(node)
                if not self._cpu_nodes:
                    self._cpus_avail.clear()
                if self.__client.status_task:
                    info = DispycosNodeInfo(node.name, node.addr, node.cpus, node.platform,
                                            node.avail_info)
                    info = DispycosStatus(node.status, info)
                    self.__client.status_task.send(info)

            elif req == 'close_server':
                addr = msg.get('addr', None)
                if not isinstance(addr, Location):
                    continue
                node = self._nodes.get(addr.addr, None)
                if not node:
                    node = self._disabled_nodes.get(addr.addr, None)
                    if not node:
                        continue
                if not node.task:
                    continue
                server = node.servers.get(addr, None)
                if not server:
                    server = node.disabled_servers.get(addr, None)
                    if not server:
                        continue
                SysTask(self.__close_server, server, server.pid, node,
                        terminate=msg.get('terminate', False), restart=msg.get('restart', False))

            elif req == 'close_node':
                addr = msg.get('addr', None)
                if not addr:
                    continue
                node = self._nodes.pop(addr, None)
                if not node:
                    continue
                self._disabled_nodes[node.addr] = node
                if node.task and self.__client:
                    req = {'req': 'release', 'auth': node.auth,
                           'setup_args': msg.get('setup_args', None),
                           'terminate': msg.get('terminate', False),
                           'restart': msg.get('restart', False)}
                    node.task.send(req)

            elif req == 'node_allocate':
                node = req.get('node', None)
                if not isinstance(node, DispycosNodeAllocate):
                    continue
                self.__cur_node_allocations = [node] + [na for na in self.__cur_node_allocations
                                                        if na.ip_rex != node.ip_rex]
                if node.ip_rex.find('*') >= 0:
                    continue
                loc = Location(node.ip_rex.replace('\\.', '.'),
                               node.port if node.port else self._node_port)
                SysTask(self.pycos.peer, loc)

            elif req == 'nodes':
                if reply_task:
                    nodes = [node.addr for node in self._nodes.values()
                             if node.status == Scheduler.NodeInitialized]
                    reply_task.send(nodes)

            elif req == 'servers':
                if reply_task:
                    servers = [server.task.location for node in self._nodes.values()
                               if node.status == Scheduler.NodeInitialized
                               for server in node.servers.values()
                               # if server.status == Scheduler.ServerInitialized
                               ]
                    reply_task.send(servers)

            elif req == 'tasks':
                servers = []
                tasks = []
                where = msg.get('at', None)
                if where:
                    if isinstance(where, Location):
                        addr = where.addr
                    else:
                        addr = where
                    node = self._nodes.get(addr, None)
                    if not node:
                        node = self._disabled_nodes.get(addr, None)
                    if node:
                        if isinstance(where, Location):
                            server = node.servers.get(where, None)
                            if not server:
                                server = node.disabled_servers.get(where, None)
                            if server:
                                servers = [server]
                        else:
                            servers.extend(node.servers.values())
                            servers.extend(node.disabled_servers.values())

                else:
                    for node in self._nodes.values():
                        servers.extend(node.servers.values())
                        servers.extend(node.disabled_servers.values())

                for server in servers:
                    tasks.extend(list(server.rtasks.keys()))
                if reply_task:
                    reply_task.send(tasks)

            elif req == 'schedule':
                if not reply_task:
                    logger.warning('Ignoring invalid client request "%s"', req)
                    continue
                try:
                    client = pycos.deserialize(msg['client'])
                    assert isinstance(client, Client) or client.__class__.__name__ == 'Client'
                    assert isinstance(client._pulse_task, Task)
                    if client._pulse_task.location == self.pycos.location:
                        client._pulse_task._id = int(client._pulse_task._id)
                        if client.status_task:
                            client.status_task._id = int(client.status_task._id)
                    assert isinstance(client._pulse_interval, (float, int))
                    assert (MinPulseInterval <= client._pulse_interval <= MaxPulseInterval)
                except Exception:
                    logger.warning('ignoring invalid client request')
                    reply_task.send(None)
                    continue
                while 1:
                    client._auth = hashlib.sha1(os.urandom(20)).hexdigest()
                    if not os.path.exists(os.path.join(self.__dest_path, client._auth)):
                        break
                try:
                    os.mkdir(os.path.join(self.__dest_path, client._auth))
                except Exception:
                    logger.debug('Could not create "%s"',
                                 os.path.join(self.__dest_path, client._auth))
                    reply_task.send(None)
                    continue
                # TODO: save it on disk instead
                clients[client._auth] = client
                reply_task.send(client._auth)

            elif req == 'await':
                if not reply_task:
                    logger.warning('Ignoring invalid client request "%s"', req)
                    continue
                client = clients.pop(auth, None)
                if not client:
                    reply_task.send(None)
                    continue
                if client._pulse_task.location.addr != self.pycos.location.addr:
                    client._xfer_files = [(os.path.join(self.__dest_path, client._auth,
                                                        os.path.join(*(dst.split(sep))),
                                                        xf.split(sep)[-1]),
                                           os.path.join(*(dst.split(sep))), os.sep)
                                          for xf, dst, sep in client._xfer_files]
                for xf, dst, sep in client._xfer_files:
                    if not os.path.isfile(xf):
                        logger.warning('File "%s" for client %s is not valid',
                                       xf, client._auth)
                        client = None
                        break
                if client is None:
                    reply_task.send(None)
                else:
                    self.__client_scheduler_task.send((client, reply_task))
                    self.__client_sched_event.set()

            elif req == 'close_client':
                if not reply_task:
                    logger.warning('Ignoring invalid client request "%s"', req)
                    continue
                SysTask(self.__close_client, reply_task=reply_task,
                        await_io=msg.get('await_io', False), terminate=msg.get('terminate', False))

            elif req == 'abandon_zombie':
                addr = msg.get('addr', None)
                if not addr:
                    if self.__client:
                        if self.__client.abandon_zombie == bool(msg.get('flag', False)):
                            continue
                        self.__client.abandon_zombie = bool(msg.get('flag', False))
                        req = {'req': 'abandon_zombie', 'flag': bool(msg.get('flag', False))}
                        for node in self._nodes.values():
                            if node.task:
                                req['auth'] = node.auth
                                node.task.send(req)
                    continue
                node = self._nodes.get(addr, None)
                if node:
                    node.abandon_zombie = msg.get('flag', False)
                    if node.task and self.__client:
                        node.task.send({'req': 'abandon_zombie', 'auth': node.auth,
                                        'flag': node.abandon_zombie})

            else:
                logger.warning('Ignoring invalid client request "%s"', req)

    def __close_node(self, node, await_io=False, terminate=False, task=None):
        if not node.task:
            logger.debug('Closing node %s ignored: %s', node.addr, node.status)
            raise StopIteration(-1)

        node.cpu_avail.clear()
        self._cpu_nodes.discard(node)
        if not self._cpu_nodes:
            self._cpus_avail.clear()
        self._nodes.pop(node.addr, None)
        self._disabled_nodes[node.addr] = node
        client = self.__client
        if node.status == Scheduler.NodeAbandoned:
            # TODO: safe to assume servers are disconnected as well?
            for server in node.disabled_servers.values():
                if server.status < Scheduler.ServerDisconnected:
                    server.status = Scheduler.ServerAbandoned

        servers = list(node.servers.values())
        servers.extend(list(node.disabled_servers.values()))
        for server in servers:
            if server.task:
                SysTask(self.__close_server, server, server.pid, node, await_io=await_io,
                        terminate=terminate)
        for server in servers:
            if server.task:
                yield server.done.wait()
        if (client and client.status_task):
            status_info = DispycosNodeInfo(node.name, node.addr, node.cpus, node.platform,
                                           node.avail_info)
            client.status_task.send(DispycosStatus(node.status, status_info))

        # if ((node.status == Scheduler.NodeDisconnected) or
        #     (node.status == Scheduler.NodeAbandoned and node.abandon_zombie)):
        #     # self._disabled_nodes.pop(node.addr, None)
        #     # TODO: it is not safe to throw away peer if node is still running
        #     Task(pycos.Pycos.instance().close_peer, node.task.location, timeout=2)
        if node.task and node.status < Scheduler.NodeClosed:
            node.task.send({'req': 'release', 'auth': node.auth})

    def __close_server(self, server, pid, node, await_io=False, terminate=False, restart=False,
                       task=None):
        if server.pid != pid:
            raise StopIteration(0)
        if server.status == Scheduler.ServerDisconnected:
            for _ in range(10):
                if not server.rtasks:
                    break
                yield task.sleep(0.2)
            server.done.set()
        server_task, server.task = server.task, None
        if not server_task or not node.task:
            raise StopIteration(0)
        if node.servers.pop(server_task.location, None):
            node.disabled_servers[server_task.location] = server
            if node.servers:
                if server.cpu_avail.is_set():
                    node.load = float(node.cpus_used) / len(node.servers)
            else:
                if node.cpu_avail.is_set():
                    node.cpu_avail.clear()
                    self._cpu_nodes.discard(node)
                    if not self._cpu_nodes:
                        self._cpus_avail.clear()

        client = self.__client
        if server.status < Scheduler.ServerClosed:
            node.task.send({'req': 'close_server', 'terminate': terminate, 'restart': restart,
                            'pid': pid, 'addr': server_task.location, 'auth': node.auth})
            server.status = Scheduler.ServerClosed
        if server.status == Scheduler.ServerClosed:
            if server.rtasks:
                if (not server.cpu_avail.is_set()):
                    logger.info('Waiting for %s remote tasks at %s to finish',
                                len(server.rtasks), server_task.location)
                    yield server.cpu_avail.wait(timeout=MsgTimeout if terminate else None)
            if await_io:
                while server.rtasks:
                    logger.info('Waiting for %s remote tasks at %s to finish',
                                len(server.rtasks), server_task.location)
                    yield server.done.wait(timeout=MsgTimeout)
                    if terminate:
                        break
            if server.rtasks:  # wait a bit for monitor to process
                for _ in range(10):
                    yield task.sleep(0.1)
                    if not server.rtasks:
                        break

        if server.rtasks:
            logger.warning('%s tasks abandoned at %s', len(server.rtasks), server_task.location)
            for rtask, job in server.rtasks.items():
                if client:
                    status = MonitorStatus(rtask, Scheduler.TaskAbandoned)
                    client._pulse_task.send({'req': status, 'auth': self.__client_auth})
            server.rtasks.clear()

        server.xfer_files = []
        server.askew_results.clear()
        if client and client.status_task:
            client.status_task.send(DispycosStatus(server.status, server_task.location))

        if not server.done.is_set():
            server.done.set()
            if node.servers:
                node.load = float(node.cpus_used) / len(node.servers)
            else:
                node.load = 0.0
        raise StopIteration(0)

    def __close_client(self, reply_task=None, await_io=False, terminate=False, task=None):
        if self.__client:
            close_tasks = [SysTask(self.__close_node, node, await_io=await_io,
                                   terminate=terminate) for node in self._nodes.values()]
            close_tasks.extend([SysTask(self.__close_node, node, await_io=await_io,
                                        terminate=terminate)
                                for node in self._disabled_nodes.values()])
            for close_task in close_tasks:
                yield close_task.finish()
        if self.__client_auth:
            client_path = os.path.join(self.__dest_path, self.__client_auth)
            if os.path.isdir(client_path):
                shutil.rmtree(client_path, ignore_errors=True)
        if self.__client and self.__client.status_task:
            self.__client.status_task.send(DispycosStatus(Scheduler.ClientClosed, id(self.__client)))
        self.__client_auth = self.__client = None
        self.__client_sched_event.set()
        if reply_task:
            reply_task.send('closed')
        raise StopIteration(0)

    def close(self, task=None):
        """Close current client and quit scheduler.

        Must be called with 'yield' as 'yield scheduler.close()' or as
        task.
        """
        yield self.__close_client(task=task)
        raise StopIteration(0)


if __name__ == '__main__':
    """The scheduler can be started either within a client program (if no other
    client programs use the nodes simultaneously), or can be run on a node with
    the options described below (usually no options are necessary, so the
    scheduler can be strated with just 'dispycos.py')
    """

    import argparse
    import signal
    try:
        import readline
    except ImportError:
        pass

    import pycos.dispycos
    setattr(sys.modules['pycos.dispycos'], '_DispycosJob_', _DispycosJob_)

    pycos.config.DispycosSchedulerPort = eval(pycos.config.DispycosSchedulerPort)
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--ip_addr', dest='host', action='append', default=[],
                        help='IP address or host name of this node')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addr', action='append', default=[],
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('--scheduler_port', dest='scheduler_port', type=str,
                        default=str(pycos.config.DispycosSchedulerPort),
                        help='port number for dispycos scheduler')
    parser.add_argument('--node_port', dest='node_port', type=str,
                        default=str(eval(pycos.config.DispycosNodePort)),
                        help='port number for dispycos node')
    parser.add_argument('--ipv4_udp_multicast', dest='ipv4_udp_multicast', action='store_true',
                        default=False, help='use multicast for IPv4 UDP instead of broadcast')
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
    parser.add_argument('--relay_nodes', action='store_true', dest='relay_nodes', default=False,
                        help='request each node to relay scheduler info on its network')
    parser.add_argument('--pulse_interval', dest='pulse_interval', type=float,
                        default=MaxPulseInterval,
                        help='interval in seconds to send "pulse" messages to check nodes '
                        'and client are connected')
    parser.add_argument('--ping_interval', dest='ping_interval', type=float, default=0,
                        help='interval in seconds to broadcast "ping" message to discover nodes')
    parser.add_argument('--zombie_period', dest='zombie_period', type=int,
                        default=(100 * MaxPulseInterval),
                        help='maximum time in seconds client is idle')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    parser.add_argument('--clean', action='store_true', dest='clean', default=False,
                        help='if given, files copied from or generated by clients will be removed')
    parser.add_argument('--daemon', action='store_true', dest='daemon', default=False,
                        help='if given, input is not read from terminal')
    config = vars(parser.parse_args(sys.argv[1:]))
    del parser

    if config['zombie_period'] and config['zombie_period'] < MaxPulseInterval:
        raise Exception('zombie_period must be >= %s' % MaxPulseInterval)

    if not config['name']:
        config['name'] = 'dispycos_scheduler'

    if config['loglevel']:
        logger.setLevel(logger.DEBUG)
    else:
        logger.setLevel(logger.INFO)
    del config['loglevel']

    if config['certfile']:
        config['certfile'] = os.path.abspath(config['certfile'])
    else:
        config['certfile'] = None
    if config['keyfile']:
        config['keyfile'] = os.path.abspath(config['keyfile'])
    else:
        config['keyfile'] = None

    pycos.config.DispycosSchedulerPort = config.pop('scheduler_port')
    pycos.config.DispycosNodePort = config.pop('node_port')
    daemon = config.pop('daemon', False)

    _dispycos_scheduler = Scheduler(**config)
    _dispycos_scheduler._remote = True
    del config

    def sighandler(signum, frame):
        # Task(_dispycos_scheduler.close).value()
        raise KeyboardInterrupt

    try:
        signal.signal(signal.SIGHUP, sighandler)
        signal.signal(signal.SIGQUIT, sighandler)
    except Exception:
        pass
    signal.signal(signal.SIGINT, sighandler)
    signal.signal(signal.SIGABRT, sighandler)
    signal.signal(signal.SIGTERM, sighandler)
    del sighandler

    if not daemon:
        try:
            if os.getpgrp() != os.tcgetpgrp(sys.stdin.fileno()):
                daemon = True
        except Exception:
            pass

    if daemon:
        del daemon
        while 1:
            try:
                time.sleep(3600)
            except (Exception, KeyboardInterrupt):
                break
    else:
        del daemon
        while 1:
            try:
                _dispycos_cmd = input(
                    '\n\nEnter "quit" or "exit" to terminate dispycos scheduler\n'
                    '      "status" to show status of scheduler: '
                    )
            except KeyboardInterrupt:
                break
            except EOFError:
                logger.warning('EOF ignored!\n')
                continue
            _dispycos_cmd = _dispycos_cmd.strip().lower()
            if _dispycos_cmd in ('quit', 'exit'):
                break
            if _dispycos_cmd == 'status':
                _dispycos_scheduler.print_status()

    logger.info('terminating dispycos scheduler')
    try:
        Task(_dispycos_scheduler.close).value()
    except KeyboardInterrupt:
        pass
