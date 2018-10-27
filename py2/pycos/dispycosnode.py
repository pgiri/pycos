#!/usr/bin/python

"""
This file is part of pycos project. See https://pycos.sourceforge.io for details.

This program can be used to start dispycos server processes so dispycos
scheduler (see 'dispycos.py') can send computations to these server processes
for executing distributed communicating proceses (tasks). All tasks in a server
execute in the same thread, so multiple CPUs are not used by one server. If CPU
intensive computations are to be run on systems with multiple processors, then
this program should be run with multiple instances (see below for '-c' option to
this program).

See 'dispycos_*.py' files for example use cases.
"""

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2014 Giridhar Pemmasani"
__license__ = "Apache 2.0"
__url__ = "https://pycos.sourceforge.io"


def _dispycos_server_proc():
    # task
    """Server process receives a computation and runs tasks for it.
    """

    import os
    import shutil
    import traceback
    import sys
    import time

    from pycos.dispycos import MinPulseInterval, MaxPulseInterval, \
        DispycosNodeInfo, DispycosNodeAvailInfo, Scheduler
    from pycos.netpycos import Task, SysTask, Location, MonitorException, deserialize, logger
    global _DispycosJob_
    from pycos.dispycos import _DispycosJob_

    for _dispycos_var in ('_dispycos_server_process', '_dispycos_server_proc'):
       globals().pop(_dispycos_var, None)
    _dispycos_scheduler = pycos.Pycos.instance()
    _dispycos_task = pycos.Pycos.cur_task()
    _dispycos_task.register('dispycos_server')
    _dispycos_config = yield _dispycos_task.receive()
    _dispycos_scheduler_task = deserialize(_dispycos_config['scheduler_task'])
    _dispycos_computation_auth = _dispycos_config.pop('computation_auth', None)
    _dispycos_var = deserialize(_dispycos_config['node_location'])
    yield _dispycos_scheduler.peer(_dispycos_var)
    _dispycos_node_task = yield Task.locate('dispycos_node', location=_dispycos_var)
    yield _dispycos_node_task.deliver({'req': 'server_task', 'oid': 1, 'pid': os.getpid(),
                                       'server_id': _dispycos_config['id'],
                                       'location': _dispycos_task.location,
                                       'auth': _dispycos_computation_auth}, timeout=5)

    if _dispycos_config['min_pulse_interval'] > 0:
        MinPulseInterval = _dispycos_config['min_pulse_interval']
    if _dispycos_config['max_pulse_interval'] > 0:
        MaxPulseInterval = _dispycos_config['max_pulse_interval']
    _dispycos_busy_time = _dispycos_config.pop('busy_time')
    pycos.MsgTimeout = _dispycos_config.pop('msg_timeout')

    _dispycos_name = _dispycos_scheduler.name
    os.chdir(_dispycos_config['dest_path'])
    _dispycos_scheduler.dest_path = _dispycos_config['dest_path']
    sys.path.insert(0, _dispycos_config['dest_path'])

    for _dispycos_var in _dispycos_config.pop('peers', []):
        Task(_dispycos_scheduler.peer, deserialize(_dispycos_var))

    for _dispycos_var in ['min_pulse_interval', 'max_pulse_interval']:
        del _dispycos_config[_dispycos_var]

    logger.info('dispycos server %s started at %s; computation files will be saved in "%s"',
                _dispycos_config['id'], _dispycos_task.location, _dispycos_config['dest_path'])
    _dispycos_req = _dispycos_client = _dispycos_auth = _dispycos_msg = None
    _dispycos_peer_status = _dispycos_monitor_task = _dispycos_monitor_proc = _dispycos_job = None
    _dispycos_job_tasks = set()
    _dispycos_jobs_done = pycos.Event()

    def _dispycos_peer_status(task=None):
        task.set_daemon()
        while 1:
            status = yield task.receive()
            if not isinstance(status, pycos.PeerStatus):
                logger.warning('Invalid peer status %s ignored', type(status))
                continue
            if status.status == pycos.PeerStatus.Offline:
                if (_dispycos_scheduler_task and
                    _dispycos_scheduler_task.location == status.location):
                    if _dispycos_computation_auth:
                        _dispycos_task.send({'req': 'close', 'auth': _dispycos_computation_auth})

    def _dispycos_monitor_proc(pulse_interval, task=None):
        task.set_daemon()
        while 1:
            msg = yield task.receive(timeout=pulse_interval)
            if isinstance(msg, MonitorException):
                logger.debug('task %s done at %s', msg.args[0], task.location)
                _dispycos_job_tasks.discard(msg.args[0])
                if not _dispycos_job_tasks:
                    _dispycos_jobs_done.set()
                _dispycos_busy_time.value = int(time.time())
            elif not msg:
                if _dispycos_job_tasks:
                    _dispycos_busy_time.value = int(time.time())
            else:
                logger.warning('invalid message to monitor ignored: %s', type(msg))

    _dispycos_scheduler.peer_status(SysTask(_dispycos_peer_status))
    _dispycos_var = deserialize(_dispycos_config['computation_location'])
    if ((yield _dispycos_scheduler.peer(_dispycos_var)) or
        (yield _dispycos_scheduler.peer(_dispycos_scheduler_task.location))):
        raise StopIteration(-1)
    _dispycos_scheduler_task.send({'status': Scheduler.ServerDiscovered, 'task': _dispycos_task,
                                   'name': _dispycos_name, 'auth': _dispycos_computation_auth})

    if _dispycos_config['_server_setup']:
        if _dispycos_config['_disable_servers']:
            while 1:
                _dispycos_msg = yield _dispycos_task.receive()
                if not isinstance(_dispycos_msg, dict):
                    continue
                _dispycos_req = _dispycos_msg.get('req', None)
                if (_dispycos_req == 'enable_server' and
                    _dispycos_msg.get('auth', None) == _dispycos_computation_auth):
                    _dispycos_var = _dispycos_msg['setup_args']
                    if not isinstance(_dispycos_var, tuple):
                        _dispycos_var = tuple(_dispycos_var)
                    break
                else:
                    _dispycos_auth = _dispycos_msg.get('node_auth', None)
                    if (_dispycos_req == 'terminate' and
                        _dispycos_auth == _dispycos_config['node_auth']):
                        if _dispycos_scheduler_task:
                            _dispycos_scheduler_task.send({'status': Scheduler.ServerClosed,
                                                           'location': _dispycos_task.location,
                                                           'auth': _dispycos_computation_auth})
                        raise StopIteration
                    else:
                        logger.warning('Ignoring invalid request to run server setup')
        else:
            _dispycos_var = ()
        _dispycos_var = yield pycos.Task(globals()[_dispycos_config['_server_setup']],
                                         *_dispycos_var).finish()
        if _dispycos_var:
            logger.debug('dispycos server %s @ %s setup failed', _dispycos_config['id'],
                         _dispycos_task.location)
            raise StopIteration(_dispycos_var)
        _dispycos_config['_server_setup'] = None
    _dispycos_scheduler_task.send({'status': Scheduler.ServerInitialized, 'task': _dispycos_task,
                                   'name': _dispycos_name, 'auth': _dispycos_computation_auth})

    _dispycos_var = _dispycos_config['pulse_interval']
    _dispycos_monitor_task = SysTask(_dispycos_monitor_proc, _dispycos_var)
    _dispycos_busy_time.value = int(time.time())
    logger.debug('dispycos server "%s": Computation "%s" from %s', _dispycos_name,
                 _dispycos_computation_auth, _dispycos_scheduler_task.location)

    while 1:
        _dispycos_msg = yield _dispycos_task.receive()
        try:
            _dispycos_req = _dispycos_msg.get('req', None)
        except Exception:
            continue

        if _dispycos_req == 'run':
            _dispycos_client = _dispycos_msg.get('client', None)
            _dispycos_auth = _dispycos_msg.get('auth', None)
            _dispycos_job = _dispycos_msg.get('job', None)
            if (not isinstance(_dispycos_client, Task) or
                _dispycos_auth != _dispycos_computation_auth):
                logger.warning('invalid run: %s', type(_dispycos_job))
                if isinstance(_dispycos_client, Task):
                    _dispycos_client.send(None)
                continue
            try:
                if _dispycos_job.code:
                    exec(_dispycos_job.code) in globals()
                _dispycos_job.args = deserialize(_dispycos_job.args)
                _dispycos_job.kwargs = deserialize(_dispycos_job.kwargs)
            except Exception:
                logger.debug('invalid computation to run')
                _dispycos_var = (sys.exc_info()[0], _dispycos_job.name, traceback.format_exc())
                _dispycos_client.send(_dispycos_var)
            else:
                Task._pycos._lock.acquire()
                try:
                    _dispycos_var = Task(globals()[_dispycos_job.name],
                                         *(_dispycos_job.args), **(_dispycos_job.kwargs))
                except Exception:
                    _dispycos_var = (sys.exc_info()[0], _dispycos_job.name, traceback.format_exc())
                else:
                    _dispycos_job_tasks.add(_dispycos_var)
                    logger.debug('task %s created at %s', _dispycos_var, _dispycos_task.location)
                    _dispycos_var.notify(_dispycos_monitor_task)
                    _dispycos_var.notify(_dispycos_scheduler_task)
                _dispycos_client.send(_dispycos_var)
                Task._pycos._lock.release()

        elif _dispycos_req == 'close' or _dispycos_req == 'quit':
            _dispycos_auth = _dispycos_msg.get('auth', None)
            if (_dispycos_auth == _dispycos_computation_auth):
                pass
            elif (_dispycos_msg.get('node_auth', None) == _dispycos_config['node_auth']):
                if _dispycos_scheduler_task:
                    _dispycos_scheduler_task.send({'status': Scheduler.ServerClosed,
                                                  'location': _dispycos_task.location,
                                                   'auth': _dispycos_computation_auth})
                while _dispycos_job_tasks:
                    logger.debug('dispycos server "%s": Waiting for %s tasks to terminate before '
                                 'closing computation', _dispycos_name, len(_dispycos_job_tasks))
                    if (yield _dispycos_jobs_done.wait(timeout=5)):
                        break
            else:
                continue
            _dispycos_var = _dispycos_msg.get('client', None)
            if isinstance(_dispycos_var, Task):
                _dispycos_var.send(0)
            break

        elif _dispycos_req == 'terminate':
            _dispycos_auth = _dispycos_msg.get('node_auth', None)
            if (_dispycos_auth != _dispycos_config['node_auth']):
                continue
            if _dispycos_scheduler_task:
                _dispycos_scheduler_task.send({'status': Scheduler.ServerClosed,
                                               'location': _dispycos_task.location,
                                               'auth': _dispycos_computation_auth})
            break

        elif _dispycos_req == 'status':
            if _dispycos_msg.get('node_auth', None) != _dispycos_config['node_auth']:
                continue
            if _dispycos_scheduler_task:
                print('  dispycos server "%s" @ %s with PID %s running %d tasks for %s' %
                      (_dispycos_name, _dispycos_task.location, os.getpid(),
                       len(_dispycos_job_tasks), _dispycos_scheduler_task.location))
            else:
                print('  dispycos server "%s" @ %s with PID %s not used by any computation' %
                      (_dispycos_name, _dispycos_task.location, os.getpid()))

        elif _dispycos_req == 'peers':
            _dispycos_auth = _dispycos_msg.get('auth', None)
            if (_dispycos_auth == _dispycos_computation_auth):
                for _dispycos_var in _dispycos_msg.get('peers', []):
                    pycos.Task(_dispycos_scheduler.peer, _dispycos_var)

        else:
            logger.warning('invalid command "%s" ignored', _dispycos_req)
            _dispycos_client = _dispycos_msg.get('client', None)
            if not isinstance(_dispycos_client, Task):
                continue
            _dispycos_client.send(-1)

    # kill any pending jobs
    while _dispycos_job_tasks:
        for _dispycos_var in _dispycos_job_tasks:
            _dispycos_var.terminate()
        logger.debug('dispycos server "%s": Waiting for %s tasks to terminate '
                     'before closing computation', _dispycos_name, len(_dispycos_job_tasks))
        if (yield _dispycos_jobs_done.wait(timeout=5)):
            break
    if _dispycos_scheduler_task:
        _dispycos_scheduler_task.send({'status': Scheduler.ServerDisconnected,
                                       'location': _dispycos_task.location,
                                       'auth': _dispycos_computation_auth})
    if os.name == 'nt':
        os.chdir(os.path.join(_dispycos_config['dest_path'], '..'))
    try:
        shutil.rmtree(_dispycos_config['dest_path'], ignore_errors=True)
    except Exception:
        pass
    yield _dispycos_node_task.deliver({'req': 'server_task', 'oid': 3,
                                       'server_id': _dispycos_config['id'], 'location': None,
                                       'auth': _dispycos_computation_auth}, timeout=5)
    logger.debug('dispycos server %s @ %s done', _dispycos_config['id'], _dispycos_task.location)


def _dispycos_server_process(_dispycos_config, _dispycos_mp_queue, _dispycos_computation):
    import os
    import sys
    import time
    import signal
    # import traceback

    for _dispycos_var in sys.modules.keys():
        if _dispycos_var.startswith('pycos'):
            sys.modules.pop(_dispycos_var)
    globals().pop('pycos', None)

    global pycos
    import pycos
    import pycos.netpycos

    if _dispycos_config['loglevel']:
        pycos.logger.setLevel(pycos.logger.DEBUG)
        # pycos.logger.show_ms(True)
    else:
        pycos.logger.setLevel(pycos.logger.INFO)
    del _dispycos_config['loglevel']

    pycos.logger.name = 'dispycosserver'
    server_id = _dispycos_config['id']
    mp_queue, _dispycos_mp_queue = _dispycos_mp_queue, None
    config = {}
    for _dispycos_var in ['udp_port', 'tcp_port', 'node', 'ext_ip_addr', 'name', 'discover_peers',
                          'secret', 'certfile', 'keyfile', 'dest_path', 'max_file_size',
                          'ipv4_udp_multicast']:
        config[_dispycos_var] = _dispycos_config.pop(_dispycos_var, None)

    while 1:
        try:
            _dispycos_scheduler = pycos.Pycos(**config)
        except Exception:
            print('dispycos server %s failed for port %s; retrying in 5 seconds' %
                  (server_id, config['tcp_port']))
            # print(traceback.format_exc())
            time.sleep(5)
        else:
            break

    if os.name == 'nt':
        _dispycos_computation = pycos.deserialize(_dispycos_computation)
        if _dispycos_computation._code:
            exec(_dispycos_computation._code) in globals()

    _dispycos_config['_disable_servers'] = _dispycos_computation._disable_servers
    _dispycos_config['_server_setup'] = _dispycos_computation._server_setup
    _dispycos_task = pycos.SysTask(_dispycos_config.pop('server_proc'))
    assert isinstance(_dispycos_task, pycos.Task)
    computation_auth = _dispycos_config['computation_auth']
    _dispycos_config['dest_path'] = config['dest_path']
    _dispycos_task.send(_dispycos_config)
    mp_queue.put({'req': 'server_task', 'auth': computation_auth, 'oid': 1, 'server_id': server_id,
                  'pid': os.getpid(), 'location': _dispycos_task.location})

    node_auth = _dispycos_config['node_auth']
    _dispycos_config = None
    del config, _dispycos_var

    def sighandler(signum, frame):
        # pycos.logger.debug('Server %s received signal %s', server_id, signum)
        _dispycos_task.send({'req': 'terminate', 'node_auth': node_auth})

    signal.signal(signal.SIGINT, sighandler)
    if os.name == 'nt':
        signal.signal(signal.SIGBREAK, sighandler)

    del sighandler

    _dispycos_task.value()
    _dispycos_scheduler.ignore_peers(ignore=True)
    for location in _dispycos_scheduler.peers():
        pycos.Task(_dispycos_scheduler.close_peer, location)
    _dispycos_scheduler.finish()
    mp_queue.put({'req': 'server_task', 'auth': computation_auth, 'oid': 3, 'server_id': server_id,
                  'location': None, 'pid': os.getpid()})
    exit(0)


def _dispycos_spawn(_dispycos_config, _dispycos_id_ports, _dispycos_mp_queue,
                    _dispycos_pipe, _dispycos_computation, _dispycos_setup_args):
    import os
    import sys
    import signal
    import multiprocessing
    import traceback

    _dispycos_config['server_proc'] = _dispycos_server_proc
    server_process = _dispycos_server_process
    for _dispycos_var in list(globals()):
        if _dispycos_var.startswith('_dispycos_'):
            if _dispycos_var in ('_dispycos_server_process', '_dispycos_server_proc'):
                continue
            globals().pop(_dispycos_var)

    for _dispycos_var in sys.modules.keys():
        if _dispycos_var.startswith('pycos'):
            sys.modules.pop(_dispycos_var)
    globals().pop('pycos', None)

    import pycos

    pycos.logger.name = 'dispycosnode'
    if _dispycos_config['loglevel']:
        pycos.logger.setLevel(pycos.logger.DEBUG)
        # pycos.logger.show_ms(True)
    else:
        pycos.logger.setLevel(pycos.logger.INFO)

    procs = []
    os.chdir(_dispycos_config['dest_path'])
    sys.path.insert(0, _dispycos_config['dest_path'])
    os.environ['PATH'] = _dispycos_config['dest_path'] + os.pathsep + os.environ['PATH']
    suid = _dispycos_config.pop('suid', None)
    if suid is not None:
        sgid = _dispycos_config.pop('sgid', None)
        if hasattr(os, 'setresuid'):
            os.setresgid(sgid, sgid, sgid)
            os.setresuid(suid, suid, suid)
        else:
            os.setregid(sgid, sgid)
            os.setreuid(suid, suid)

    def close(status):
        for i in range(len(procs)):
            proc = procs[i]
            if not proc:
                continue
            if not proc.is_alive():
                continue
            try:
                if os.name == 'nt':
                    os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
                else:
                    os.kill(proc.pid, signal.SIGINT)
            except Exception:
                # print(traceback.format_exc())
                pass

        for i in range(len(procs)):
            proc = procs[i]
            if proc and proc.is_alive():
                for j in range(20):
                    proc.join(0.1)
                    if not proc.is_alive():
                        break
                    if j < 15:
                        continue
                    if j == 15:
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                else:
                    try:
                        if os.name == 'nt':
                            os.kill(proc.pid, signal.SIGTERM)
                        else:
                            os.kill(proc.pid, signal.SIGKILL)
                    except Exception:
                        pycos.logger.debug('Could not terminate server %s (PID %s): %s',
                                           i, proc.pid, traceback.format_exc())

            if proc and proc.exitcode:
                pycos.logger.warning('Server %s (process %s) reaped', _dispycos_id_ports[i][0],
                                     proc.pid)
                _dispycos_mp_queue.put({'req': 'server_task', 'oid': 3,
                                        'auth': _dispycos_config['computation_auth'],
                                        'server_id': _dispycos_id_ports[i][0], 'location': None})

        _dispycos_pipe.send({'msg': 'closed', 'auth': _dispycos_config['computation_auth']})
        exit(status)

    if os.name != 'nt':
        if _dispycos_computation._code:
            exec(_dispycos_computation._code) in globals()

        if _dispycos_computation._node_setup:
            try:
                if not isinstance(_dispycos_setup_args, tuple):
                    _dispycos_setup_args = tuple(_dispycos_setup_args)
                ret = pycos.Task(globals()[_dispycos_computation._node_setup],
                                 *_dispycos_setup_args).value()
            except Exception:
                pycos.logger.warning('node_setup failed for %s', _dispycos_computation._auth)
                # print(traceback.format_exc())
                ret = -1
            if ret != 0:
                close(ret)
            _dispycos_computation._node_setup = None

    def sighandler(signum, frame):
        # pycos.logger.debug('Spawn process received signal %s', signum)
        # _dispycos_pipe.send({'msg': 'closed', 'auth': _dispycos_config['computation_auth']})
        close(0)

    signal.signal(signal.SIGINT, sighandler)
    if os.name == 'nt':
        signal.signal(signal.SIGBREAK, sighandler)

    del sighandler

    for id_port in _dispycos_id_ports:
        server_config = dict(_dispycos_config)
        server_config['id'] = id_port[0]
        server_config['name'] = '%s_server-%s' % (_dispycos_config['name'], server_config['id'])
        server_config['tcp_port'] = id_port[1]
        server_config['peers'] = _dispycos_config['peers'][:]
        server_config['dest_path'] = os.path.join(_dispycos_config['dest_path'],
                                                  'dispycos_server_%s' % server_config['id'])
        proc = multiprocessing.Process(target=server_process, name=server_config['name'],
                                       args=(server_config, _dispycos_mp_queue,
                                             _dispycos_computation))
        if isinstance(proc, multiprocessing.Process):
            proc.start()
            pycos.logger.debug('dispycos server %s started with PID %s', id_port[0], proc.pid)
            procs.append(proc)
        else:
            pycos.logger.warning('Could not start dispycos server for %s at %s',
                                 id_port[0], id_port[1])

    _dispycos_pipe.send({'msg': 'started', 'auth': _dispycos_config['computation_auth'],
                         'cpus': len(procs)})

    while 1:
        try:
            req = _dispycos_pipe.recv()
        except Exception:
            break
        if (isinstance(req, dict) and req.get('msg') == 'quit' and
            req.get('auth') == _dispycos_config.get('computation_auth')):
            break
        else:
            pycos.logger.warning('Ignoring invalid pipe cmd: %s' % str(req))

    close(0)


def _dispycos_node():
    if ((hasattr(os, 'setresuid') or hasattr(os, 'setreuid')) and os.getuid() != os.geteuid()):
        _dispycos_config['suid'] = os.geteuid()
        _dispycos_config['sgid'] = os.getegid()
        if _dispycos_config['suid'] == 0:
            print('\n    WARNING: Python interpreter %s likely has suid set to 0 '
                  '\n    (administrator privilege), which is dangerous.\n\n' %
                  sys.executable)
        if _dispycos_config['sgid'] == 0:
            print('\n    WARNING: Python interpreter %s likely has sgid set to 0 '
                  '\n    (administrator privilege), which is dangerous.\n\n' %
                  sys.executable)

        os.setegid(os.getgid())
        os.seteuid(os.getuid())
        os.umask(0x007)
        pycos.logger.info('Computations will run with uid %s and gid %s' %
                          (_dispycos_config['suid'], _dispycos_config['sgid']))
    else:
        _dispycos_config.pop('suid', None)
        _dispycos_config.pop('sgid', None)

    if not _dispycos_config['min_pulse_interval']:
        _dispycos_config['min_pulse_interval'] = MinPulseInterval
    if not _dispycos_config['max_pulse_interval']:
        _dispycos_config['max_pulse_interval'] = MaxPulseInterval
    if _dispycos_config['msg_timeout'] < 1:
        raise Exception('msg_timeout must be at least 1')
    if (_dispycos_config['min_pulse_interval'] and
        _dispycos_config['min_pulse_interval'] < _dispycos_config['msg_timeout']):
        raise Exception('min_pulse_interval must be at least msg_timeout')
    if (_dispycos_config['max_pulse_interval'] and _dispycos_config['min_pulse_interval'] and
        _dispycos_config['max_pulse_interval'] < _dispycos_config['min_pulse_interval']):
        raise Exception('max_pulse_interval must be at least min_pulse_interval')
    if _dispycos_config['zombie_period']:
        if _dispycos_config['zombie_period'] < _dispycos_config['min_pulse_interval']:
            raise Exception('zombie_period must be at least min_pulse_interval')
    else:
        _dispycos_config['zombie_period'] = 0

    num_cpus = multiprocessing.cpu_count()
    if _dispycos_config['cpus'] > 0:
        if _dispycos_config['cpus'] > num_cpus:
            raise Exception('CPU count must be <= %s' % num_cpus)
        num_cpus = _dispycos_config['cpus']
    elif _dispycos_config['cpus'] < 0:
        if -_dispycos_config['cpus'] >= num_cpus:
            raise Exception('CPU count must be > -%s' % num_cpus)
        num_cpus += _dispycos_config['cpus']
    del _dispycos_config['cpus']

    tcp_ports = set()
    for tcp_port in _dispycos_config.pop('tcp_ports', []):
        tcp_port = tcp_port.split('-')
        if len(tcp_port) == 1:
            tcp_ports.add(int(tcp_port[0]))
        elif len(tcp_port) == 2:
            tcp_port = (int(tcp_port[0]), min(int(tcp_port[1]),
                                              int(tcp_port[0]) + num_cpus - len(tcp_ports)))
            tcp_ports = tcp_ports.union(range(tcp_port[0], tcp_port[1] + 1))
        else:
            raise Exception('Invalid TCP port range "%s"' % str(tcp_port))

    if tcp_ports:
        tcp_ports = sorted(tcp_ports)
        tcp_ports = tcp_ports[:num_cpus + 1]
    else:
        tcp_ports = [9706]

    for tcp_port in range(tcp_ports[-1] + 1, tcp_ports[-1] + 1 + num_cpus - len(tcp_ports) + 1):
        if tcp_ports[-1]:
            tcp_ports.append(tcp_port)
        else:
            tcp_ports.append(0)
    del tcp_port

    peer = None
    for peer in _dispycos_config['peers']:
        peer = peer.split(':')
        if len(peer) != 2:
            raise Exception('peer "%s" is not valid' % ':'.join(peer))
        _dispycos_config['peers'].append(pycos.serialize(pycos.Location(peer[0], peer[1])))
    del peer

    node_name = _dispycos_config['name']
    if not node_name:
        node_name = socket.gethostname()
        if not node_name:
            node_name = 'dispycos_server'

    daemon = _dispycos_config.pop('daemon', False)
    if not daemon:
        try:
            if os.getpgrp() != os.tcgetpgrp(sys.stdin.fileno()):
                daemon = True
        except Exception:
            pass

    _dispycos_config['discover_peers'] = False

    class Struct(object):

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def __setattr__(self, name, value):
            if hasattr(self, name):
                self.__dict__[name] = value
            else:
                raise AttributeError('Invalid attribute "%s"' % name)

    service_times = Struct(start=None, stop=None, end=None)
    # time at start of day
    _dispycos_var = time.localtime()
    _dispycos_var = (int(time.time()) - (_dispycos_var.tm_hour * 3600) -
                     (_dispycos_var.tm_min * 60) - _dispycos_var.tm_sec)
    if _dispycos_config['service_start']:
        service_times.start = time.strptime(_dispycos_config.pop('service_start'), '%H:%M')
        service_times.start = (_dispycos_var + (service_times.start.tm_hour * 3600) +
                               (service_times.start.tm_min * 60))
    if _dispycos_config['service_stop']:
        service_times.stop = time.strptime(_dispycos_config.pop('service_stop'), '%H:%M')
        service_times.stop = (_dispycos_var + (service_times.stop.tm_hour * 3600) +
                              (service_times.stop.tm_min * 60))
    if _dispycos_config['service_end']:
        service_times.end = time.strptime(_dispycos_config.pop('service_end'), '%H:%M')
        service_times.end = (_dispycos_var + (service_times.end.tm_hour * 3600) +
                             (service_times.end.tm_min * 60))

    if (service_times.start or service_times.stop or service_times.end):
        if not service_times.start:
            service_times.start = int(time.time())
        if service_times.stop:
            if service_times.start >= service_times.stop:
                raise Exception('"service_start" must be before "service_stop"')
        if service_times.end:
            if service_times.start >= service_times.end:
                raise Exception('"service_start" must be before "service_end"')
            if service_times.stop and service_times.stop >= service_times.end:
                raise Exception('"service_stop" must be before "service_end"')
        if not service_times.stop and not service_times.end:
            raise Exception('"service_stop" or "service_end" must also be given')

    if _dispycos_config['max_file_size']:
        _dispycos_var = re.match(r'(\d+)([kKmMgGtT]?)', _dispycos_config['max_file_size'])
        if (not _dispycos_var or
            len(_dispycos_var.group(0)) != len(_dispycos_config['max_file_size'])):
            raise Exception('Invalid max_file_size option')
        _dispycos_config['max_file_size'] = int(_dispycos_var.group(1))
        if _dispycos_var.group(2):
            _dispycos_var = _dispycos_var.group(2).lower()
            _dispycos_config['max_file_size'] *= 1024**({'k': 1, 'm': 2, 'g': 3,
                                                         't': 4}[_dispycos_var])
    else:
        _dispycos_config['max_file_size'] = 0

    if _dispycos_config['certfile']:
        _dispycos_config['certfile'] = os.path.abspath(_dispycos_config['certfile'])
    else:
        _dispycos_config['certfile'] = None
    if _dispycos_config['keyfile']:
        _dispycos_config['keyfile'] = os.path.abspath(_dispycos_config['keyfile'])
    else:
        _dispycos_config['keyfile'] = None

    busy_time = multiprocessing.Value('I', 0)
    mp_queue = multiprocessing.Queue()
    node_auth = hashlib.sha1(os.urandom(20)).hexdigest()
    node_servers = [None] * (num_cpus + 1)
    if _dispycos_config['dest_path']:
        dispycos_dest_path = _dispycos_config['dest_path']
    else:
        import tempfile
        dispycos_dest_path = os.path.join(os.sep, tempfile.gettempdir(), 'pycos')
    dispycos_dest_path = os.path.join(dispycos_dest_path, 'dispycos', 'node')
    _dispycos_config['dest_path'] = dispycos_dest_path

    if not os.path.isdir(dispycos_dest_path):
        try:
            os.makedirs(dispycos_dest_path)
        except Exception:
            print('Could not create directory "%s"' % dispycos_dest_path)
            exit(1)

    if os.name == 'nt':
        proc_signals = [signal.CTRL_BREAK_EVENT, signal.CTRL_BREAK_EVENT, signal.SIGTERM]
    else:
        proc_signals = [signal.SIGINT, 0, signal.SIGKILL]

    def close_server(server, ppid=None):
        server_dir = os.path.join(dispycos_dest_path, 'dispycos_server_%s' % server.id)

        psproc = pid = None
        if not ppid:
            try:
                with open(server.pid_file, 'rb') as fd:
                    pid_info = pickle.load(fd)
                    if not ppid:
                        ppid = pid_info['ppid']
            except Exception:
                pid_info = {}

        if server.psproc:
            psproc = server.psproc
            pid = server.psproc.pid
            if psproc.is_running():
                try:
                    ps_ppid = psproc.ppid()
                    if ppid and ps_ppid not in (ppid, 1):
                        ppid = ps_ppid
                except Exception:
                    # print(traceback.format_exc())
                    pass

        elif pid_info:
            pid = pid_info.get('pid', None)
            if pid and psutil:
                try:
                    psproc = psutil.Process(pid)
                    assert psproc.is_running()
                    assert psproc.ppid() in (ppid, 1)
                    if server.id:
                        main_proc = psutil.Process(ppid)
                    else:
                        main_proc = psproc
                    assert any(arg.endswith('dispycosnode.py') for arg in main_proc.cmdline())
                except Exception:
                    pid = None

        if pid:
            try:
                os.kill(pid, proc_signals[0])
            except Exception:
                # TODO: handle failures
                pycos.logger.debug(traceback.format_exc())

        if server.id and ppid and ppid != 1:
            try:
                os.kill(ppid, proc_signals[0])
            except Exception:
                pycos.logger.debug(traceback.format_exc())

        if psproc:
            for i in range(20):
                try:
                    psproc.wait(0.1)
                except Exception:
                    pass
                else:
                    time.sleep(0.1)
                if not psproc.is_running():
                    break
                if i < 15:
                    continue
                if i == 15:
                    try:
                        psproc.terminate()
                    except Exception:
                        # print(traceback.format_exc())
                        pass
                elif i == 19:
                    try:
                        psproc.kill()
                    except Exception:
                        # print(traceback.format_exc())
                        pass
            else:
                pycos.logger.warning('Could not terminate process with ID %s', pid)

        elif pid:
            signum = proc_signals[1]
            for i in range(1, 20):
                time.sleep(0.1)
                if signum and i < 15:
                    continue
                if i == 15:
                    signum = proc_signals[2]
                try:
                    os.kill(pid, signum)
                except OSError as err:
                    break
            else:
                pycos.logger.debug('Could not terminate process with ID %s', pid)

        shutil.rmtree(server_dir, ignore_errors=True)

        if os.path.exists(server.pid_file):
            try:
                os.remove(server.pid_file)
            except Exception:
                print('\n    Could not remove file "%s";\n'
                      '    ensure no dispycosnode and servers are running and\n'
                      '    remove *.pid files in "%s"\n' % (server.pid_file, dispycos_dest_path))

        server.psproc = None
        server.task = None
        return 0

    for _dispycos_id in range(0, num_cpus + 1):
        _dispycos_var = os.path.join(dispycos_dest_path, '..', 'server-%d.pkl' % _dispycos_id)
        node_servers[_dispycos_id] = Struct(id=_dispycos_id, psproc=None, task=None, msg_oid=0,
                                            name='%s_server-%s' % (node_name, _dispycos_id),
                                            port=tcp_ports[_dispycos_id], pid_file=_dispycos_var)
    node_servers[0].name = None

    if _dispycos_config['clean']:
        for _dispycos_id in range(1, len(node_servers)) + [0]:
            server = node_servers[_dispycos_id]
            if not os.path.exists(server.pid_file):
                continue
            close_server(server)
    if any(os.path.exists(server.pid_file) for server in node_servers):
        print('\n    Another dispycosnode seems to be running;\n'
              '    ensure no dispycosnode and servers are running and\n'
              '    remove *.pkl files in %s"\n' % (os.path.join(dispycos_dest_path, '..')))
        exit(1)

    dispycos_pid = os.getpid()
    if hasattr(os, 'getppid'):
        dispycos_ppid = os.getppid()
    else:
        dispycos_ppid = 1
    with open(node_servers[0].pid_file, 'wb') as fd:
        # TODO: store and check crate_time with psutil
        pickle.dump({'pid': dispycos_pid, 'ppid': dispycos_ppid, 'spid': -1}, fd)

    server_config = {}
    for _dispycos_var in ['udp_port', 'tcp_port', 'node', 'ext_ip_addr', 'name',
                          'discover_peers', 'secret', 'certfile', 'keyfile', 'dest_path',
                          'max_file_size', 'ipv4_udp_multicast']:
        server_config[_dispycos_var] = _dispycos_config.get(_dispycos_var, None)
    server_config['name'] = '%s_server-0' % node_name
    server_config['tcp_port'] = tcp_ports[0]
    if _dispycos_config['loglevel']:
        pycos.logger.setLevel(pycos.Logger.DEBUG)
        # pycos.logger.show_ms(True)
    else:
        pycos.logger.setLevel(pycos.Logger.INFO)
    dispycos_scheduler = pycos.Pycos(**server_config)
    dispycos_scheduler.ignore_peers(True)
    for _dispycos_var in dispycos_scheduler.peers():
        pycos.Task(dispycos_scheduler.close_peer, _dispycos_var)
    if dispycos_dest_path != dispycos_scheduler.dest_path:
        print('\n    Destination paths inconsistent: "%s" != "%s"\n' %
              (dispycos_dest_path, dispycos_scheduler.dest_path))
        exit(1)
    if 'suid' in _dispycos_config:
        os.chown(dispycos_dest_path, -1, _dispycos_config['sgid'])
        os.chmod(dispycos_dest_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                 stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | stat.S_ISGID)
    os.chmod(os.path.join(dispycos_dest_path, '..'), stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
             stat.S_IXGRP)

    del _dispycos_id

    def node_proc(task=None):
        from pycos.dispycos import DispycosNodeAvailInfo, DispycosNodeInfo

        task.register('dispycos_node')
        ping_interval = _dispycos_config.pop('ping_interval')
        msg_timeout = _dispycos_config['msg_timeout']
        zombie_period = _dispycos_config['zombie_period']
        disk_path = dispycos_scheduler.dest_path
        _dispycos_config['node_location'] = pycos.serialize(task.location)
        comp_state = Struct(auth=None, scheduler=None, cpus_reserved=0, spawn_mpproc=None,
                            interval=_dispycos_config['max_pulse_interval'])

        def service_available():
            now = time.time()
            if not _dispycos_config['serve']:
                return False
            if not service_times.start:
                return True
            if service_times.stop:
                if (service_times.start <= now < service_times.stop):
                    return True
            else:
                if (service_times.start <= now < service_times.end):
                    return True
            return False

        def service_times_proc(task=None):
            task.set_daemon()
            while 1:
                now = int(time.time())
                yield task.sleep(service_times.start - now)
                dispycos_scheduler.ignore_peers(False)
                dispycos_scheduler.discover_peers(port=_dispycos_config['scheduler_port'])

                if service_times.stop:
                    now = int(time.time())
                    yield task.sleep(service_times.stop - now)
                    for server in node_servers:
                        if server.task:
                            server.task.send({'req': 'quit', 'node_auth': node_auth})

                dispycos_scheduler.ignore_peers(True)
                if service_times.end:
                    now = int(time.time())
                    yield task.sleep(service_times.end - now)
                    for server in node_servers:
                        if server.task:
                            server.task.send({'req': 'terminate', 'node_auth': node_auth})
                    for peer in dispycos_scheduler.peers():
                        pycos.Task(dispycos_scheduler.close_peer, peer)

                # advance times for next day
                service_times.start += 24 * 3600
                if service_times.stop:
                    service_times.stop += 24 * 3600
                if service_times.end:
                    service_times.end += 24 * 3600

        def monitor_peers(task=None):
            task.set_daemon()
            while 1:
                msg = yield task.receive()
                if not isinstance(msg, pycos.PeerStatus):
                    continue
                if msg.status == pycos.PeerStatus.Offline:
                    if (comp_state.scheduler and comp_state.scheduler.location == msg.location):
                        node_task.send({'req': 'release', 'auth': comp_state.auth})
                else:  # msg.status == pycos.PeerStatus.Online
                    if comp_state.scheduler and comp_state.scheduler.location == msg.location:
                        node_task.send({'req': 'release', 'auth': comp_state.auth})

        def server_task_msg(msg, task=None):
            try:
                oid = msg['oid']
                auth = msg['auth']
                server_id = msg['server_id']
                location = msg['location']
            except Exception:
                raise StopIteration(-1)

            if auth != comp_state.auth and comp_state.auth:
                pycos.logger.warning('Ignoring invalid server msg %s: %s != %s',
                                     oid, auth, comp_state.auth)
                raise StopIteration(-1)
            if server_id < 1 or server_id > len(node_servers):
                pycos.logger.debug('Ignoring server task information for %s', server_id)
                raise StopIteration(-1)
            server = node_servers[server_id]
            if location:
                for i in range(3):
                    if server.task and server.msg_oid >= oid:
                        raise StopIteration(0)
                    if (yield dispycos_scheduler.peer(location)):
                        pycos.logger.warning('Could not communicate with server %s at %s',
                                             server.id, location)
                        yield task.sleep(0.2)
                    else:
                        break
                else:
                    raise StopIteration(-1)
                for i in range(3):
                    server_task = yield pycos.SysTask.locate('dispycos_server',
                                                             location=location, timeout=5)
                    if isinstance(server_task, pycos.SysTask):
                        break
                else:
                    raise StopIteration(-1)
                if (not server.task and auth == comp_state.auth and server.msg_oid < oid):
                    server.task = server_task
                    server.msg_oid = oid
                    pid = msg.get('pid', None)
                    with open(server.pid_file, 'wb') as fd:
                        pickle.dump({'pid': pid, 'ppid': comp_state.spawn_mpproc.pid}, fd)
                    if psutil and pid:
                        try:
                            server.psproc = psutil.Process(pid)
                        except Exception:
                            pass
            else:
                if server.msg_oid > oid or not server.task:
                    raise StopIteration(-1)
                server.task = None
                server.msg_oid = 0
                server.psproc = None
                if os.path.exists(server.pid_file):
                    try:
                        os.remove(server.pid_file)
                    except Exception:
                        pass
                comp_state.cpus_reserved -= 1
                if comp_state.cpus_reserved == 0:
                    pycos.Task(close_computation)

        def mp_queue_server():
            while 1:
                msg = mp_queue.get(block=True)
                if not isinstance(msg, dict):
                    pycos.logger.warning('Ignoring mp queue message: %s', type(msg))
                    continue
                pycos.Task(server_task_msg, msg)

        def close_computation(req='close', task=None):
            if not comp_state.scheduler:
                raise StopIteration
            for server in node_servers:
                if server.task:
                    server.task.send({'req': req, 'node_auth': node_auth})
            if (req == 'close' or req == 'quit') and any(server.task for server in node_servers):
                raise StopIteration
            if comp_state.spawn_mpproc and comp_state.spawn_mpproc.is_alive():
                spawn_pid = comp_state.spawn_mpproc.pid
                parent_pipe.send({'msg': 'quit', 'auth': comp_state.auth})
                for i in range(5):
                    if parent_pipe.poll(1):
                        msg = parent_pipe.recv()
                        if (isinstance(msg, dict) and msg.get('msg', None) == 'closed' and
                            msg.get('auth', None) == comp_state.auth):
                            comp_state.spawn_mpproc = None
                            break
                else:
                    if comp_state.spawn_mpproc and comp_state.spawn_mpproc.is_alive():
                        try:
                            comp_state.spawn_mpproc.terminate()
                        except Exception:
                            pass
                        comp_state.spawn_mpproc.join(1)
                        if comp_state.spawn_mpproc and comp_state.spawn_mpproc.is_alive():
                            try:
                                os.kill(spawn_pid, proc_signals[0])
                            except Exception:
                                pass
                        for i in range(10):
                            if not comp_state.spawn_mpproc or not comp_state.spawn_mpproc.is_alive():
                                break
                            comp_state.spawn_mpproc.join(1)
                            if i == 9:
                                try:
                                    os.kill(spawn_pid, proc_signals[2])
                                except Exception:
                                    pass
                    comp_state.spawn_mpproc = None

                # clear pipe
                while parent_pipe.poll(0.2):
                    msg = parent_pipe.recv()
                while child_pipe.poll(0.2):
                    msg = child_pipe.recv()
            else:
                spawn_pid = None

            for server in node_servers:
                if not server.id:
                    continue
                for i in range(20):
                    if server.task:
                        yield task.sleep(0.1)
                    else:
                        path = os.path.join(dispycos_dest_path, 'dispycos_server_%s' % server.id)
                        if os.path.isdir(path):
                            try:
                                shutil.rmtree(path)
                            except Exception:
                                pycos.logger.warning('Could not remove "%s"', path)
                        break
                else:
                    close_server(server, ppid=spawn_pid)
            if os.path.isdir(dispycos_dest_path):
                for name in os.listdir(dispycos_dest_path):
                    name = os.path.join(dispycos_dest_path, name)
                    try:
                        if os.path.isfile(name):
                            os.remove(name)
                        else:
                            shutil.rmtree(name, ignore_errors=True)
                    except Exception:
                        pycos.logger.warning('Could not remove "%s"' % name)

            loc = _dispycos_config.pop('computation_location', None)
            if loc:
                loc = pycos.deserialize(loc)
                pycos.Task(dispycos_scheduler.close_peer, loc)
            comp_state.scheduler.send({'status': pycos.dispycos.Scheduler.NodeClosed,
                                       'location': node_task.location, 'auth': comp_state.auth})
            if not os.path.isdir(dispycos_dest_path):
                pycos.logger.warning('Apparently dispycosnode directory "%s" vanished after '
                                     'computation from %s', dispycos_dest_path, comp_state.scheduler)
                try:
                    os.path.makedirs(dispycos_dest_path)
                    if 'suid' in _dispycos_config:
                        os.chmod(dispycos_dest_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                                 | stat.S_IXGRP | stat.S_IXOTH)
                except Exception:
                    pycos.logger.warning('Could not create dispyconode directory "%s"',
                                         dispycos_dest_path)
                    _dispycos_config['serve'] = 0

            comp_state.cpus_reserved = 0
            comp_state.auth = None
            comp_state.interval = _dispycos_config['max_pulse_interval']
            timer_task.resume()
            comp_state.scheduler = None
            if req == 'quit' or req == 'terminate':
                _dispycos_config['serve'] = 0
            elif _dispycos_config['serve'] > 0:
                _dispycos_config['serve'] -= 1

            if _dispycos_config['serve']:
                if service_available():
                    dispycos_scheduler.ignore_peers(False)
                    dispycos_scheduler.discover_peers(port=_dispycos_config['scheduler_port'])
            else:
                if all(not server.task for server in node_servers):
                    # mp_queue.close()
                    parent_pipe.close()
                    child_pipe.close()
                node_task.send({'req': 'exit', 'auth': node_auth})

            raise StopIteration

        def timer_proc(task=None):
            task.set_daemon()
            last_pulse = last_ping = time.time()
            while 1:
                yield task.sleep(comp_state.interval)
                now = time.time()
                if comp_state.scheduler:
                    msg = {'status': 'pulse', 'location': task.location}
                    if psutil:
                        msg['node_status'] = DispycosNodeAvailInfo(
                            task.location, 100.0 - psutil.cpu_percent(),
                            psutil.virtual_memory().available, psutil.disk_usage(disk_path).free,
                            100.0 - psutil.swap_memory().percent)

                    sent = yield comp_state.scheduler.deliver(msg, timeout=msg_timeout)
                    if sent == 1:
                        last_pulse = now
                    elif comp_state.auth and (now - last_pulse) > (5 * comp_state.interval):
                        pycos.logger.warning('Scheduler is not reachable; closing computation "%s"',
                                             comp_state.auth)
                        node_task.send({'req': 'release', 'auth': comp_state.auth,
                                        'client': comp_state.scheduler})
                        pycos.Task(dispycos_scheduler.close_peer, comp_state.scheduler.location)

                    if (zombie_period and ((now - busy_time.value) > zombie_period) and
                        comp_state.auth):
                        pycos.logger.warning('Closing zombie computation "%s"', comp_state.auth)
                        node_task.send({'req': 'release', 'auth': comp_state.auth,
                                        'client': comp_state.scheduler})

                if ping_interval and (now - last_ping) > ping_interval and service_available():
                    dispycos_scheduler.discover_peers(port=_dispycos_config['scheduler_port'])
                    last_ping = now

        timer_task = pycos.Task(timer_proc)
        qserver = threading.Thread(target=mp_queue_server)
        qserver.daemon = True
        qserver.start()
        dispycos_scheduler.peer_status(pycos.Task(monitor_peers))
        if service_times.start:
            pycos.Task(service_times_proc)
        else:
            dispycos_scheduler.ignore_peers(False)
            dispycos_scheduler.discover_peers(port=_dispycos_config['scheduler_port'])

        for peer in _dispycos_config['peers']:
            pycos.Task(dispycos_scheduler.peer, pycos.deserialize(peer))

        # TODO: create new pipe for each computation instead?
        parent_pipe, child_pipe = multiprocessing.Pipe(duplex=True)

        while 1:
            msg = yield task.receive()
            try:
                req = msg['req']
            except Exception:
                continue

            if req == 'server_task':
                pycos.Task(server_task_msg, msg)

            elif req == 'dispycos_node_info':
                client = msg.get('client', None)
                if isinstance(client, pycos.Task):
                    if psutil:
                        info = DispycosNodeAvailInfo(task.location,
                                                     100.0 - psutil.cpu_percent(),
                                                     psutil.virtual_memory().available,
                                                     psutil.disk_usage(disk_path).free,
                                                     100.0 - psutil.swap_memory().percent)
                    else:
                        info = DispycosNodeAvailInfo(task.location, None, None, None, None)
                    info = DispycosNodeInfo(node_name, task.location.addr,
                                            len(node_servers) - 1, platform.platform(), info)
                    client.send(info)

            elif req == 'reserve':
                client = msg.get('client', None)
                cpus = msg.get('cpus', -1)
                auth = msg.get('auth', None)
                avail_cpus = len([server for server in node_servers
                                  if server.id and not server.task])
                if (isinstance(client, pycos.Task) and isinstance(cpus, int) and
                    isinstance(msg.get('status_task', None), pycos.Task) and auth and
                    isinstance(msg.get('computation_location', None), pycos.Location)):
                    pass
                else:
                    continue

                if (not comp_state.auth and service_available() and (0 < cpus <= avail_cpus)):
                    dispycos_scheduler.ignore_peers(True)
                    if (yield dispycos_scheduler.peer(msg['computation_location'])):
                        cpus = 0
                    else:
                        if not cpus:
                            cpus = avail_cpus
                else:
                    cpus = 0
                if ((yield client.deliver(cpus, timeout=msg_timeout)) == 1 and cpus):
                    comp_state.cpus_reserved = cpus
                    comp_state.auth = auth
                    busy_time.value = int(time.time())
                    comp_state.scheduler = msg['status_task']
                    timer_task.resume()
                else:
                    dispycos_scheduler.ignore_peers(False)
                    dispycos_scheduler.discover_peers(port=_dispycos_config['scheduler_port'])

            elif req == 'computation':
                client = msg.get('client', None)
                computation = msg.get('computation', None)
                if (comp_state.auth == msg.get('auth', None) and
                    isinstance(client, pycos.Task) and isinstance(computation, Computation) and
                    comp_state.cpus_reserved > 0):
                    busy_time.value = int(time.time())
                    _dispycos_config['scheduler_task'] = pycos.serialize(comp_state.scheduler)
                    _dispycos_config['computation_location'] = pycos.serialize(
                        computation._pulse_task.location)
                    _dispycos_config['computation_auth'] = computation._auth
                    comp_state.interval = computation._pulse_interval
                    if comp_state.interval < _dispycos_config['min_pulse_interval']:
                        comp_state.interval = _dispycos_config['min_pulse_interval']
                        pycos.logger.warning('Pulse interval for computation from %s has been '
                                             'raised to %s', client.location,
                                             comp_state.interval)
                    if zombie_period:
                        _dispycos_config['pulse_interval'] = min(comp_state.interval,
                                                                 zombie_period / 3)
                    else:
                        _dispycos_config['pulse_interval'] = comp_state.interval

                    id_ports = [(server.id, server.port) for server in node_servers
                                if server.id and not server.task]
                    id_ports = id_ports[:comp_state.cpus_reserved]
                    if os.name == 'nt':
                        computation = pycos.serialize(computation)
                    args = (_dispycos_config, id_ports, mp_queue, child_pipe, computation,
                            msg.get('setup_args', ()))
                    comp_state.spawn_mpproc = multiprocessing.Process(target=_dispycos_spawn,
                                                                      args=args)
                    comp_state.spawn_mpproc.start()
                    with open(node_servers[0].pid_file, 'wb') as fd:
                        pickle.dump({'pid': dispycos_pid, 'ppid': dispycos_ppid,
                                     'spid': comp_state.spawn_mpproc.pid}, fd)
                    if parent_pipe.poll(10):
                        cpus = parent_pipe.recv()
                        if (isinstance(cpus, dict) and cpus.get('msg', None) == 'started' and
                            cpus.get('auth', None) == comp_state.auth):
                            cpus = cpus.get('cpus', 0)
                        else:
                            cpus = 0
                    else:
                        cpus = 0
                    if ((yield client.deliver(cpus)) == 1) and cpus:
                        timer_task.resume()
                    else:
                        pycos.Task(close_computation)

            elif req == 'release':
                auth = msg.get('auth', None)
                if comp_state.auth and auth == comp_state.auth:
                    yield close_computation(task=task, req='close')

            elif req == 'close' or req == 'quit' or req == 'terminate':
                auth = msg.get('auth', None)
                if auth == node_auth:
                    if _dispycos_config['serve']:
                        _dispycos_config['serve'] = 0
                    elif req == 'quit':
                        req = 'terminate'
                    if comp_state.scheduler:
                        yield close_computation(req=req, task=task)
                    elif req == 'quit' or req == 'terminate':
                        break

            elif req == 'exit':
                auth = msg.get('auth', None)
                if auth == node_auth:
                    break

            else:
                pycos.logger.warning('Invalid message %s ignored',
                                     str(msg) if isinstance(msg, dict) else '')

        try:
            os.remove(node_servers[0].pid_file)
        except Exception:
            pass
        if os.name == 'nt':
            os.kill(dispycos_pid, signal.SIGTERM)
        else:
            os.kill(dispycos_pid, signal.SIGINT)

    _dispycos_config['name'] = node_name
    _dispycos_config['dest_path'] = dispycos_dest_path
    _dispycos_config['node_auth'] = node_auth
    _dispycos_config['busy_time'] = busy_time
    node_task = pycos.Task(node_proc)
    del server_config, tcp_ports, _dispycos_var

    def sighandler(signum, frame):
        # pycos.logger.info('dispynode (%s) received signal %s', dispycos_pid, signum)
        if os.path.isfile(node_servers[0].pid_file):
            node_task.send({'req': 'terminate', 'auth': node_auth})
        else:
            raise KeyboardInterrupt

    signal.signal(signal.SIGINT, sighandler)
    if os.name == 'nt':
        signal.signal(signal.SIGBREAK, sighandler)
    if hasattr(signal, 'SIGQUIT'):
        signal.signal(signal.SIGQUIT, sighandler)
    if hasattr(signal, 'SIGHUP'):
        signal.signal(signal.SIGHUP, sighandler)

    del sighandler

    if daemon:
        while 1:
            try:
                time.sleep(3600)
            except Exception:
                if os.path.exists(node_servers[0].pid_file):
                    node_task.send({'req': 'quit', 'auth': node_auth})
                break
    else:
        while 1:
            # wait a bit for any output for previous command is done
            time.sleep(0.2)
            try:
                cmd = raw_input(
                        '\nEnter\n'
                        '  "status" to get status\n'
                        '  "close" to stop accepting new jobs and\n'
                        '          close current computation when current jobs are finished\n'
                        '  "quit" to "close" current computation and exit dispycosnode\n'
                        '  "terminate" to kill current jobs and "quit": ')
            except KeyboardInterrupt:
                if os.path.exists(node_servers[0].pid_file):
                    node_task.send({'req': 'quit', 'auth': node_auth})
                break
            else:
                cmd = cmd.strip().lower()
                if not cmd:
                    cmd = 'status'

            print('')
            if cmd == 'status':
                for server in node_servers:
                    if server.task:
                        server.task.send({'req': cmd, 'node_auth': node_auth})
                    elif server.id:
                        print('  dispycos server "%s" is not currently used' % server.name)
            elif cmd in ('close', 'quit', 'terminate'):
                node_task.send({'req': cmd, 'auth': node_auth})
                break

    try:
        node_task.value()
    except (Exception, KeyboardInterrupt):
        pass
    for peer in dispycos_scheduler.peers():
        pycos.Task(dispycos_scheduler.close_peer, peer)

    try:
        os.rmdir(dispycos_dest_path)
    except Exception:
        pass
    # if 'suid' in _dispycos_config:
    #     os.setegid(_dispycos_config['sgid'])
    #     os.seteuid(_dispycos_config['suid'])
    exit(0)


if __name__ == '__main__':

    """
    See http://pycos.sourceforge.io/dispycos.html#node-servers for details on
    options to start this program.
    """

    import sys
    import time
    import argparse
    import multiprocessing
    import threading
    import socket
    import os
    import stat
    import hashlib
    import re
    import signal
    import platform
    import shutil
    import cPickle as pickle
    import traceback
    try:
        import readline
    except Exception:
        pass
    try:
        import psutil
    except ImportError:
        print('\n'
              '    "psutil" module is not available;\n'
              '    wihout it, using "clean" option may be dangerous and\n'
              '    CPU, memory, disk status will not be sent to scheduler / client!\n')
        psutil = None
    else:
        psutil.cpu_percent(0.1)

    import pycos
    import pycos.netpycos
    from pycos.dispycos import MinPulseInterval, MaxPulseInterval, Computation

    pycos.logger.name = 'dispycosnode'
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', dest='config', default='',
                        help='use configuration in given file')
    parser.add_argument('--save_config', dest='save_config', default='',
                        help='save configuration in given file and exit')
    parser.add_argument('-c', '--cpus', dest='cpus', type=int, default=0,
                        help='number of CPUs/dispycos instances to run; '
                        'if negative, that many CPUs are not used')
    parser.add_argument('-i', '--ip_addr', dest='node', action='append', default=[],
                        help='IP address or host name of this node')
    parser.add_argument('--ext_ip_addr', dest='ext_ip_addr', action='append', default=[],
                        help='External IP address to use (needed in case of NAT firewall/gateway)')
    parser.add_argument('--tcp_ports', dest='tcp_ports', action='append', default=[],
                        help='TCP port numbers to use')
    parser.add_argument('-u', '--udp_port', dest='udp_port', type=int, default=9706,
                        help='UDP port number to use')
    parser.add_argument('--scheduler_port', dest='scheduler_port', type=int, default=9705,
                        help='UDP port number used by dispycos scheduler')
    parser.add_argument('--ipv4_udp_multicast', dest='ipv4_udp_multicast', action='store_true',
                        default=False, help='use multicast for IPv4 UDP instead of broadcast')
    parser.add_argument('-n', '--name', dest='name', default='',
                        help='(symbolic) name given to Pycos schdulers on this node')
    parser.add_argument('--dest_path', dest='dest_path', default='',
                        help='path prefix to where files sent by peers are stored')
    parser.add_argument('--max_file_size', dest='max_file_size', default='',
                        help='maximum file size of any file transferred')
    parser.add_argument('-s', '--secret', dest='secret', default='',
                        help='authentication secret for handshake with peers')
    parser.add_argument('--certfile', dest='certfile', default='',
                        help='file containing SSL certificate')
    parser.add_argument('--keyfile', dest='keyfile', default='',
                        help='file containing SSL key')
    parser.add_argument('--serve', dest='serve', default=-1, type=int,
                        help='number of clients to serve before exiting')
    parser.add_argument('--service_start', dest='service_start', default='',
                        help='time of day in HH:MM format when to start service')
    parser.add_argument('--service_stop', dest='service_stop', default='',
                        help='time of day in HH:MM format when to stop service '
                        '(continue to execute running jobs, but no new jobs scheduled)')
    parser.add_argument('--service_end', dest='service_end', default='',
                        help='time of day in HH:MM format when to end service '
                        '(terminate running jobs)')
    parser.add_argument('--msg_timeout', dest='msg_timeout', default=pycos.MsgTimeout, type=int,
                        help='timeout for delivering messages')
    parser.add_argument('--min_pulse_interval', dest='min_pulse_interval',
                        default=MinPulseInterval, type=int,
                        help='minimum pulse interval clients can use in number of seconds')
    parser.add_argument('--max_pulse_interval', dest='max_pulse_interval',
                        default=MaxPulseInterval, type=int,
                        help='maximum pulse interval clients can use in number of seconds')
    parser.add_argument('--zombie_period', dest='zombie_period', default=(10 * MaxPulseInterval),
                        type=int,
                        help='maximum number of seconds for client to not run computation')
    parser.add_argument('--ping_interval', dest='ping_interval', default=0, type=int,
                        help='interval in number of seconds for node to broadcast its address')
    parser.add_argument('--daemon', action='store_true', dest='daemon', default=False,
                        help='if given, input is not read from terminal')
    parser.add_argument('--clean', action='store_true', dest='clean', default=False,
                        help='if given, server processes from previous run will be killed '
                        'and new server process started')
    parser.add_argument('--peer', dest='peers', action='append', default=[],
                        help='peer location (in the form node:TCPport) to communicate')
    parser.add_argument('-d', '--debug', action='store_true', dest='loglevel', default=False,
                        help='if given, debug messages are printed')
    _dispycos_config = vars(parser.parse_args(sys.argv[1:]))

    if _dispycos_config['clean'] and not psutil:
        print('\n    Using "clean" option without "psutil" module is dangerous!\n')

    _dispycos_var = _dispycos_config.pop('config')
    if _dispycos_var:
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read(_dispycos_var)
        cfg = dict(cfg.items('DEFAULT'))
        cfg['cpus'] = int(cfg['cpus'])
        cfg['udp_port'] = int(cfg['udp_port'])
        cfg['serve'] = int(cfg['serve'])
        cfg['msg_timeout'] = int(cfg['msg_timeout'])
        cfg['min_pulse_interval'] = int(cfg['min_pulse_interval'])
        cfg['max_pulse_interval'] = int(cfg['max_pulse_interval'])
        cfg['zombie_period'] = int(cfg['zombie_period'])
        cfg['ping_interval'] = int(cfg['ping_interval'])
        cfg['daemon'] = cfg['daemon'] == 'True'
        cfg['clean'] = cfg['clean'] == 'True'
        # cfg['discover_peers'] = cfg['discover_peers'] == 'True'
        cfg['loglevel'] = cfg['loglevel'] == 'True'
        cfg['tcp_ports'] = [_dispycos_var.strip()[1:-1] for _dispycos_var in
                            cfg['tcp_ports'][1:-1].split(',')]
        cfg['tcp_ports'] = [_dispycos_var for _dispycos_var in cfg['tcp_ports'] if _dispycos_var]
        cfg['ipv4_udp_multicast'] = cfg['ipv4_udp_multicast'] == 'True'
        cfg['peers'] = [_dispycos_var.strip()[1:-1] for _dispycos_var in
                        cfg['peers'][1:-1].split(',')]
        cfg['peers'] = [_dispycos_var for _dispycos_var in cfg['peers'] if _dispycos_var]
        for key, value in _dispycos_config.items():
            if _dispycos_config[key] != parser.get_default(key) or key not in cfg:
                cfg[key] = _dispycos_config[key]
        _dispycos_config = cfg
        del key, value, cfg

    _dispycos_var = _dispycos_config.pop('save_config')
    if _dispycos_var:
        import configparser
        cfg = configparser.ConfigParser(_dispycos_config)
        cfgfp = open(_dispycos_var, 'w')
        cfg.write(cfgfp)
        cfgfp.close()
        exit(0)

    del parser, sys.modules['argparse'], globals()['argparse'], _dispycos_var

    _dispycos_node()
