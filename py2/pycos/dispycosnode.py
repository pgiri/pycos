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
    """Server process receives computations and runs tasks for it.
    """

    import os
    import shutil
    import traceback
    import sys
    import time

    from pycos.dispycos import MinPulseInterval, MaxPulseInterval, \
        DispycosNodeInfo, DispycosNodeAvailInfo, Scheduler
    import pycos.netpycos as pycos
    from pycos.netpycos import Task, SysTask, Location

    _dispycos_task = pycos.Pycos.cur_task()
    _dispycos_config = yield _dispycos_task.receive()
    _dispycos_node_task = pycos.deserialize(_dispycos_config['node_task'])
    _dispycos_scheduler_task = pycos.deserialize(_dispycos_config['scheduler_task'])
    assert isinstance(_dispycos_scheduler_task, Task)
    _dispycos_computation_auth = _dispycos_config.pop('computation_auth', None)

    if _dispycos_config['min_pulse_interval'] > 0:
        MinPulseInterval = _dispycos_config['min_pulse_interval']
    if _dispycos_config['max_pulse_interval'] > 0:
        MaxPulseInterval = _dispycos_config['max_pulse_interval']
    _dispycos_busy_time = _dispycos_config.pop('busy_time')
    pycos.MsgTimeout = _dispycos_config.pop('msg_timeout')

    _dispycos_name = pycos.Pycos.instance().name
    _dispycos_dest_path = os.path.join(pycos.Pycos.instance().dest_path,
                                       'dispycosproc-%s' % _dispycos_config['id'])
    if os.path.isdir(_dispycos_dest_path):
        shutil.rmtree(_dispycos_dest_path)
    pycos.Pycos.instance().dest_path = _dispycos_dest_path
    os.chdir(_dispycos_dest_path)
    sys.path.insert(0, _dispycos_dest_path)

    for _dispycos_var in _dispycos_config.pop('peers', []):
        Task(pycos.Pycos.instance().peer, pycos.deserialize(_dispycos_var))

    for _dispycos_var in ['clean', 'min_pulse_interval', 'max_pulse_interval']:
        del _dispycos_config[_dispycos_var]

    _dispycos_task.register('dispycos_server')
    pycos.logger.info('dispycos server %s started at %s; computation files will be saved in "%s"',
                      _dispycos_config['id'], _dispycos_task.location, _dispycos_dest_path)
    _dispycos_req = _dispycos_client = _dispycos_auth = _dispycos_msg = None
    _dispycos_peer_status = _dispycos_monitor_task = _dispycos_monitor_proc = _dispycos_job = None
    _dispycos_job_tasks = set()
    _dispycos_jobs_done = pycos.Event()

    def _dispycos_peer_status(task=None):
        task.set_daemon()
        while 1:
            status = yield task.receive()
            if not isinstance(status, pycos.PeerStatus):
                pycos.logger.warning('Invalid peer status %s ignored', type(status))
                continue
            if status.status == pycos.PeerStatus.Offline:
                if (_dispycos_scheduler_task and
                    _dispycos_scheduler_task.location == status.location):
                    if _dispycos_computation_auth:
                        _dispycos_task.send({'req': 'close', 'auth': _dispycos_computation_auth})

    def _dispycos_monitor_proc(zombie_period, task=None):
        task.set_daemon()
        while 1:
            msg = yield task.receive(timeout=zombie_period)
            if isinstance(msg, pycos.MonitorException):
                pycos.logger.debug('task %s done', msg.args[0])
                _dispycos_job_tasks.discard(msg.args[0])
                if not _dispycos_job_tasks:
                    _dispycos_jobs_done.set()
                _dispycos_busy_time.value = int(time.time())
            elif not msg:
                if _dispycos_job_tasks:
                    _dispycos_busy_time.value = int(time.time())
            else:
                pycos.logger.warning('invalid message to monitor ignored: %s', type(msg))

    _dispycos_var = _dispycos_config['computation_location']
    _dispycos_var = pycos.Location(_dispycos_var.addr, _dispycos_var.port)
    if (yield pycos.Pycos.instance().peer(_dispycos_var)):
        raise StopIteration(-1)
    pycos.Pycos.instance().peer_status(SysTask(_dispycos_peer_status))
    yield pycos.Pycos.instance().peer(_dispycos_node_task.location)
    yield pycos.Pycos.instance().peer(_dispycos_scheduler_task.location)
    _dispycos_scheduler_task.send({'status': Scheduler.ServerDiscovered, 'task': _dispycos_task,
                                   'name': _dispycos_name, 'auth': _dispycos_computation_auth})

    if _dispycos_config['_server_setup']:
        if _dispycos_config['_disable_servers']:
            while 1:
                _dispycos_var = yield _dispycos_task.receive()
                if (isinstance(_dispycos_var, dict) and
                    _dispycos_var.get('req', None) == 'enable_server' and
                    _dispycos_var.get('auth', None) == _dispycos_computation_auth):
                    _dispycos_var = _dispycos_var['setup_args']
                    if not isinstance(_dispycos_var, tuple):
                        _dispycos_var = tuple(_dispycos_var)
                    break
                else:
                    pycos.logger.warning('Ignoring invalid request to run server setup')
        else:
            _dispycos_var = ()
        _dispycos_var = yield pycos.Task(globals()[_dispycos_config['_server_setup']],
                                         *_dispycos_var).finish()
        if _dispycos_var:
            pycos.logger.debug('dispycos server %s @ %s setup failed', _dispycos_config['id'],
                               _dispycos_task.location)
            raise StopIteration(_dispycos_var)
        _dispycos_config['_server_setup'] = None
    _dispycos_scheduler_task.send({'status': Scheduler.ServerInitialized, 'task': _dispycos_task,
                                   'name': _dispycos_name, 'auth': _dispycos_computation_auth})

    _dispycos_var = _dispycos_config['zombie_period']
    if _dispycos_var:
        _dispycos_var /= 3
    else:
        _dispycos_var = None
    _dispycos_monitor_task = SysTask(_dispycos_monitor_proc, _dispycos_var)
    _dispycos_node_task.send({'req': 'server_setup', 'id': _dispycos_config['id'],
                              'task': _dispycos_task})
    _dispycos_busy_time.value = int(time.time())
    pycos.logger.debug('dispycos server "%s": Computation "%s" from %s', _dispycos_name,
                       _dispycos_computation_auth, _dispycos_scheduler_task.location)

    while 1:
        _dispycos_msg = yield _dispycos_task.receive()
        if not isinstance(_dispycos_msg, dict):
            continue
        _dispycos_req = _dispycos_msg.get('req', None)

        if _dispycos_req == 'run':
            _dispycos_client = _dispycos_msg.get('client', None)
            _dispycos_auth = _dispycos_msg.get('auth', None)
            _dispycos_job = _dispycos_msg.get('job', None)
            if (not isinstance(_dispycos_client, Task) or
                _dispycos_auth != _dispycos_computation_auth):
                pycos.logger.warning('invalid run: %s', type(_dispycos_job))
                if isinstance(_dispycos_client, Task):
                    _dispycos_client.send(None)
                continue
            try:
                if _dispycos_job.code:
                    exec(_dispycos_job.code) in globals()
                _dispycos_job.args = pycos.deserialize(_dispycos_job.args)
                _dispycos_job.kwargs = pycos.deserialize(_dispycos_job.kwargs)
            except:
                pycos.logger.debug('invalid computation to run')
                _dispycos_var = (sys.exc_info()[0], _dispycos_job.name, traceback.format_exc())
                _dispycos_client.send(_dispycos_var)
            else:
                Task._pycos._lock.acquire()
                try:
                    _dispycos_var = Task(globals()[_dispycos_job.name],
                                         *(_dispycos_job.args), **(_dispycos_job.kwargs))
                except:
                    _dispycos_var = (sys.exc_info()[0], _dispycos_job.name, traceback.format_exc())
                else:
                    _dispycos_job_tasks.add(_dispycos_var)
                    _dispycos_busy_time.value = int(time.time())
                    pycos.logger.debug('task %s created', _dispycos_var)
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
                                                  'location': _dispycos_task.location})
                while _dispycos_job_tasks:
                    pycos.logger.debug('dispycos server "%s": Waiting for %s tasks to '
                                       'terminate before closing computation',
                                       _dispycos_name, len(_dispycos_job_tasks))
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
                _dispycos_scheduler_task.send({'status': Scheduler.ServerDisconnected,
                                              'location': _dispycos_task.location})
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
                    pycos.Task(pycos.Pycos.instance().peer, _dispycos_var)

        else:
            pycos.logger.warning('invalid command "%s" ignored', _dispycos_req)
            _dispycos_client = _dispycos_msg.get('client', None)
            if not isinstance(_dispycos_client, Task):
                continue
            _dispycos_client.send(-1)

    # kill any pending jobs
    while _dispycos_job_tasks:
        for _dispycos_job_task in _dispycos_job_tasks:
            _dispycos_job_task.terminate()
        pycos.logger.debug('dispycos server "%s": Waiting for %s tasks to terminate '
                           'before closing computation', _dispycos_name, len(_dispycos_job_tasks))
        if (yield _dispycos_jobs_done.wait(timeout=5)):
            break
    pycos.logger.debug('dispycos server %s @ %s done', _dispycos_config['id'],
                       _dispycos_task.location)


def _dispycos_server_process(_dispycos_config, _dispycos_mp_queue, _dispycos_computation):
    import os
    import sys
    import time
    # import traceback

    for _dispycos_var in sys.modules.keys():
        if _dispycos_var.startswith('pycos'):
            sys.modules.pop(_dispycos_var)
    globals().pop('pycos', None)

    global pycos
    import pycos.netpycos as pycos

    _dispycos_pid_path = os.path.join(_dispycos_config['dest_path'],
                                      'dispycosproc-%s.pid' % _dispycos_config['id'])
    if os.path.isfile(_dispycos_pid_path):
        with open(_dispycos_pid_path, 'r') as _dispycos_req:
            _dispycos_var = _dispycos_req.read()
        _dispycos_var = int(_dispycos_var)
        if not _dispycos_config['clean']:
            print('\n   Another dispycosnode seems to be running;\n'
                  '   make sure server with PID %d quit and remove "%s"\n' %
                  (_dispycos_var, _dispycos_pid_path))
            _dispycos_var = os.getpid()

        import signal
        try:
            os.kill(_dispycos_var, signal.SIGINT)
            time.sleep(0.1)
            os.kill(_dispycos_var, signal.SIGKILL)
        except:
            pass
        else:
            time.sleep(0.1)
            try:
                if os.waitpid(_dispycos_var, os.WNOHANG)[0] != _dispycos_var:
                    pycos.logger.warning('Killing process %d failed', _dispycos_var)
            except:
                pass
        del signal, _dispycos_req

    with open(_dispycos_pid_path, 'w') as _dispycos_var:
        _dispycos_var.write('%s' % os.getpid())

    if _dispycos_config['loglevel']:
        pycos.logger.setLevel(pycos.logger.DEBUG)
        # pycos.logger.show_ms(True)
    else:
        pycos.logger.setLevel(pycos.logger.INFO)
    del _dispycos_config['loglevel']

    server_id = _dispycos_config['id']
    mp_queue, _dispycos_mp_queue = _dispycos_mp_queue, None
    config = {}
    for _dispycos_var in ['udp_port', 'tcp_port', 'node', 'ext_ip_addr', 'name', 'discover_peers',
                          'secret', 'certfile', 'keyfile', 'dest_path', 'max_file_size']:
        config[_dispycos_var] = _dispycos_config.pop(_dispycos_var, None)

    while 1:
        try:
            _dispycos_scheduler = pycos.Pycos(**config)
        except:
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
    _dispycos_config['computation_location'] = _dispycos_computation._pulse_task.location
    _dispycos_task = pycos.SysTask(_dispycos_server_proc)
    assert isinstance(_dispycos_task, pycos.Task)
    mp_queue.put((server_id, pycos.serialize(_dispycos_task)))
    _dispycos_task.send(_dispycos_config)

    _dispycos_config = None
    del config, _dispycos_var

    _dispycos_task.value()
    _dispycos_scheduler.ignore_peers(ignore=True)
    for location in _dispycos_scheduler.peers():
        pycos.Task(_dispycos_scheduler.close_peer, location)
    _dispycos_scheduler.finish()
    try:
        os.remove(_dispycos_pid_path)
    except:
        pass
    mp_queue.put((server_id, None))
    exit(0)


def _dispycos_spawn(_dispycos_config, _dispycos_id_ports, _dispycos_mp_queue,
                   _dispycos_pipe, _dispycos_computation, _dispycos_setup_args):
    import os
    import sys
    import signal
    import multiprocessing
    # import traceback

    try:
        signal.signal(signal.SIGHUP, signal.SIG_DFL)
        signal.signal(signal.SIGQUIT, signal.SIG_DFL)
    except:
        pass
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGABRT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    for _dispycos_var in sys.modules.keys():
        if _dispycos_var.startswith('pycos'):
            sys.modules.pop(_dispycos_var)
    globals().pop('pycos', None)

    import pycos

    os.chdir(_dispycos_config['dest_path'])
    sys.path.insert(0, _dispycos_config['dest_path'])
    os.environ['PATH'] = _dispycos_config['dest_path'] + os.pathsep + os.environ['PATH']
    procs = [None] * len(_dispycos_id_ports)

    def terminate(status):
        for i in range(len(_dispycos_id_ports)):
            proc = procs[i]
            if not proc:
                continue
            if proc.is_alive():
                try:
                    proc.terminate()
                except:
                    pass
                else:
                    proc.join(1)
            if (not proc.is_alive()) and proc.exitcode:
                pycos.logger.warning('Server %s (process %s) reaped', _dispycos_id_ports[i][0],
                                     proc.pid)
                _dispycos_mp_queue.put((_dispycos_id_ports[i][0], None))
            _dispycos_pid_path = os.path.join(_dispycos_config['dest_path'],
                                              'dispycosproc-%s.pid' % _dispycos_id_ports[i][0])
            try:
                os.remove(_dispycos_pid_path)
            except:
                pass

        _dispycos_pipe.send('closed')
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
            except:
                pycos.logger.warning('node_setup failed for %s', _dispycos_computation._auth)
                # print(traceback.format_exc())
                ret = -1
            if ret != 0:
                _dispycos_pipe.send(0)
                terminate(ret)
            _dispycos_computation._node_setup = None

    def start_process(i, procs):
        server_config = dict(_dispycos_config)
        server_config['id'] = _dispycos_id_ports[i][0]
        server_config['name'] = '%s_proc-%s' % (_dispycos_config['name'], server_config['id'])
        server_config['tcp_port'] = _dispycos_id_ports[i][1]
        server_config['peers'] = _dispycos_config['peers'][:]
        procs[i] = multiprocessing.Process(target=_dispycos_server_process,
                                           name=server_config['name'],
                                           args=(server_config, _dispycos_mp_queue,
                                                 _dispycos_computation))
        procs[i].start()

    for i in range(len(procs)):
        start_process(i, procs)
        pycos.logger.debug('dispycos server %s started with PID %s',
                           _dispycos_id_ports[i][0], procs[i].pid)

    _dispycos_pipe.send(len(procs))

    while 1:
        req = _dispycos_pipe.recv()
        if req['req'] == 'quit':
            break
        else:
            pycos.logger.warning('Ignoring invalid pipe cmd: %s' % str(req))

    for proc in procs:
        proc.join(1)

    terminate(0)


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
    import hashlib
    import re
    import signal
    import platform
    import shutil
    try:
        import readline
    except:
        pass
    try:
        import psutil
    except ImportError:
        print('\n   \'psutil\' module is not available; '
              'CPU, memory, disk status will not be sent!\n')
        psutil = None
    else:
        psutil.cpu_percent(0.1)
    from pycos.dispycos import MinPulseInterval, MaxPulseInterval, Scheduler, Computation
    import pycos.netpycos as pycos

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
    parser.add_argument('-u', '--udp_port', dest='udp_port', type=int, default=51351,
                        help='UDP port number to use')
    parser.add_argument('--tcp_ports', dest='tcp_ports', action='append', default=[],
                        help='TCP port numbers to use')
    parser.add_argument('--scheduler_port', dest='scheduler_port', type=int, default=51350,
                        help='UDP port number used by dispycos scheduler')
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
                        type=int, help='maximum number of seconds for client to not run computation')
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
        cfg['peers'] = [_dispycos_var.strip()[1:-1] for _dispycos_var in
                        cfg['peers'][1:-1].split(',')]
        cfg['peers'] = [_dispycos_var for _dispycos_var in cfg['peers'] if _dispycos_var]
        for key, value in _dispycos_config.items():
            if _dispycos_config[key] != parser.get_default(key) or key not in cfg:
                cfg[key] = _dispycos_config[key]
        _dispycos_config = cfg
        del key, value, cfg
    del parser, MinPulseInterval, MaxPulseInterval
    del sys.modules['argparse'], globals()['argparse']

    _dispycos_var = _dispycos_config.pop('save_config')
    if _dispycos_var:
        import configparser
        cfg = configparser.ConfigParser(_dispycos_config)
        cfgfp = open(_dispycos_var, 'w')
        cfg.write(cfgfp)
        cfgfp.close()
        exit(0)

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

    _dispycos_cpus = multiprocessing.cpu_count()
    if _dispycos_config['cpus'] > 0:
        if _dispycos_config['cpus'] > _dispycos_cpus:
            raise Exception('CPU count must be <= %s' % _dispycos_cpus)
        _dispycos_cpus = _dispycos_config['cpus']
    elif _dispycos_config['cpus'] < 0:
        if -_dispycos_config['cpus'] >= _dispycos_cpus:
            raise Exception('CPU count must be > -%s' % _dispycos_cpus)
        _dispycos_cpus += _dispycos_config['cpus']
    del _dispycos_config['cpus']

    _dispycos_tcp_ports = set()
    tcp_port = tcp_ports = None
    for tcp_port in _dispycos_config.pop('tcp_ports', []):
        tcp_ports = tcp_port.split('-')
        if len(tcp_ports) == 1:
            _dispycos_tcp_ports.add(int(tcp_ports[0]))
        elif len(tcp_ports) == 2:
            _dispycos_tcp_ports = _dispycos_tcp_ports.union(range(int(tcp_ports[0]),
                                                                  int(tcp_ports[1]) + 1))
        else:
            raise Exception('Invalid TCP port range "%s"' % tcp_ports)
    _dispycos_tcp_ports = sorted(_dispycos_tcp_ports)

    if _dispycos_tcp_ports:
        for tcp_port in range(_dispycos_tcp_ports[-1] + 1,
                              _dispycos_tcp_ports[-1] + 1 +
                              (_dispycos_cpus + 1) - len(_dispycos_tcp_ports)):
            _dispycos_tcp_ports.append(int(tcp_port))
        # _dispycos_tcp_ports = _dispycos_tcp_ports[:(_dispycos_cpus + 1)]
    else:
        _dispycos_tcp_ports = [0] * (_dispycos_cpus + 1)
    del tcp_port, tcp_ports

    peers, _dispycos_config['peers'] = _dispycos_config['peers'], []
    peer = None
    for peer in peers:
        peer = peer.split(':')
        if len(peer) != 2:
            raise Exception('peer "%s" is not valid' % ':'.join(peer))
        _dispycos_config['peers'].append(pycos.serialize(pycos.Location(peer[0], peer[1])))
    del peer, peers

    _dispycos_name = _dispycos_config['name']
    if not _dispycos_name:
        _dispycos_name = socket.gethostname()
        if not _dispycos_name:
            _dispycos_name = 'dispycos_server'

    _dispycos_daemon = _dispycos_config.pop('daemon', False)
    if not _dispycos_daemon:
        try:
            if os.getpgrp() != os.tcgetpgrp(sys.stdin.fileno()):
                _dispycos_daemon = True
        except:
            pass
        if os.name == 'nt':
            # Python 3 under Windows blocks multiprocessing.Process on reading
            # input; pressing "Enter" twice works (for one subprocess). Until
            # this is understood / fixed, disable reading input.
            print('\nReading standard input disabled, as multiprocessing does not seem to work'
                  'with reading input under Windows\n')
            _dispycos_daemon = True

    _dispycos_config['discover_peers'] = False

    # time at start of day
    _dispycos_var = time.localtime()
    _dispycos_var = (int(time.time()) - (_dispycos_var.tm_hour * 3600) -
                     (_dispycos_var.tm_min * 60))
    _dispycos_service_start = _dispycos_service_stop = _dispycos_service_end = None
    if _dispycos_config['service_start']:
        _dispycos_service_start = time.strptime(_dispycos_config.pop('service_start'), '%H:%M')
        _dispycos_service_start = (_dispycos_var + (_dispycos_service_start.tm_hour * 3600) +
                                   (_dispycos_service_start.tm_min * 60))
    if _dispycos_config['service_stop']:
        _dispycos_service_stop = time.strptime(_dispycos_config.pop('service_stop'), '%H:%M')
        _dispycos_service_stop = (_dispycos_var + (_dispycos_service_stop.tm_hour * 3600) +
                                  (_dispycos_service_stop.tm_min * 60))
    if _dispycos_config['service_end']:
        _dispycos_service_end = time.strptime(_dispycos_config.pop('service_end'), '%H:%M')
        _dispycos_service_end = (_dispycos_var + (_dispycos_service_end.tm_hour * 3600) +
                                 (_dispycos_service_end.tm_min * 60))

    if (_dispycos_service_start or _dispycos_service_stop or _dispycos_service_end):
        if not _dispycos_service_start:
            _dispycos_service_start = int(time.time())
        if _dispycos_service_stop:
            if _dispycos_service_start >= _dispycos_service_stop:
                raise Exception('"service_start" must be before "service_stop"')
        if _dispycos_service_end:
            if _dispycos_service_start >= _dispycos_service_end:
                raise Exception('"service_start" must be before "service_end"')
            if _dispycos_service_stop and _dispycos_service_stop >= _dispycos_service_end:
                raise Exception('"service_stop" must be before "service_end"')
        if not _dispycos_service_stop and not _dispycos_service_end:
            raise Exception('"service_stop" or "service_end" must also be given')

    if _dispycos_config['max_file_size']:
        _dispycos_var = re.match(r'(\d+)([kKmMgGtT]?)', _dispycos_config['max_file_size'])
        if not _dispycos_var or len(_dispycos_var.group(0)) != len(_dispycos_config['max_file_size']):
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

    _dispycos_node_auth = hashlib.sha1(os.urandom(20)).hexdigest()

    class _dispycos_Struct(object):

        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def __setattr__(self, name, value):
            if hasattr(self, name):
                self.__dict__[name] = value
            else:
                raise AttributeError('Invalid attribute "%s"' % name)

    _dispycos_spawn_proc = None
    _dispycos_busy_time = multiprocessing.Value('I', 0)
    _dispycos_mp_queue = multiprocessing.Queue()
    _dispycos_servers = [None] * _dispycos_cpus
    for _dispycos_server_id in range(1, _dispycos_cpus + 1):
        _dispycos_var = '%s_proc-%s' % (_dispycos_name, _dispycos_server_id)
        _dispycos_server = _dispycos_Struct(id=_dispycos_server_id, proc=None, task=None,
                                            name=_dispycos_var)
        _dispycos_servers[_dispycos_server_id - 1] = _dispycos_server

    def _dispycos_node_proc(task=None):
        from pycos.dispycos import DispycosNodeAvailInfo, DispycosNodeInfo, MaxPulseInterval
        global _dispycos_servers, _dispycos_config, _dispycos_spawn_proc

        task.register('dispycos_node')
        task_scheduler = pycos.Pycos.instance()
        last_pulse = last_ping = time.time()
        scheduler_task = cur_computation_auth = None
        interval = _dispycos_config['max_pulse_interval']
        ping_interval = _dispycos_config.pop('ping_interval')
        msg_timeout = _dispycos_config['msg_timeout']
        zombie_period = _dispycos_config['zombie_period']
        disk_path = task_scheduler.dest_path
        _dispycos_config['node_task'] = pycos.serialize(task)
        _dispycos_config['node'] = task.location.addr

        def monitor_peers(task=None):
            task.set_daemon()
            while 1:
                msg = yield task.receive()
                if not isinstance(msg, pycos.PeerStatus):
                    continue
                if msg.status == pycos.PeerStatus.Offline:
                    if (scheduler_task and scheduler_task.location == msg.location):
                        _dispycos_node_task.send({'req': 'release', 'auth': cur_computation_auth})

        def mp_queue_server():
            global _dispycos_tcp_ports
            while 1:
                proc_id, proc_task = _dispycos_mp_queue.get(block=True)
                server = _dispycos_servers[proc_id - 1]
                if proc_task:
                    server.task = pycos.deserialize(proc_task)
                    # if not _dispycos_tcp_ports[server.id - 1]:
                    #     _dispycos_tcp_ports[server.id - 1] = server.task.location.port
                else:
                    server.task = None
                    if _dispycos_config['serve']:
                        if scheduler_task and service_available(now):
                            pycos.logger.warning('Server %s terminated', server.name)
                            # _dispycos_start_server(server)
                    elif all(not server.task for server in _dispycos_servers):
                        _dispycos_node_task.send({'req': 'quit', 'auth': _dispycos_node_auth})
                        break

        def service_available(now):
            if not _dispycos_config['serve']:
                return False
            if not _dispycos_service_start:
                return True
            if _dispycos_service_stop:
                if (_dispycos_service_start <= now < _dispycos_service_stop):
                    return True
            else:
                if (_dispycos_service_start <= now < _dispycos_service_end):
                    return True
            return False

        def service_times_proc(task=None):
            global _dispycos_service_start, _dispycos_service_stop, _dispycos_service_end
            task.set_daemon()
            while 1:
                if _dispycos_service_stop:
                    now = int(time.time())
                    yield task.sleep(_dispycos_service_stop - now)
                    for server in _dispycos_servers:
                        if server.task:
                            server.task.send({'req': 'quit', 'node_auth': _dispycos_node_auth})

                if _dispycos_service_end:
                    now = int(time.time())
                    yield task.sleep(_dispycos_service_end - now)
                    for server in _dispycos_servers:
                        if server.task:
                            server.task.send({'req': 'terminate', 'node_auth': _dispycos_node_auth})

                # advance times for next day
                _dispycos_service_start += 24 * 3600
                if _dispycos_service_stop:
                    _dispycos_service_stop += 24 * 3600
                if _dispycos_service_end:
                    _dispycos_service_end += 24 * 3600
                # disable service till next start
                task_scheduler.ignore_peers(True)
                now = int(time.time())
                yield task.sleep(_dispycos_service_start - now)
                task_scheduler.ignore_peers(False)
                task_scheduler.discover_peers(port=_dispycos_config['scheduler_port'])

        def close_computation():
            global _dispycos_spawn_proc

            for server in _dispycos_servers:
                if server.task:
                    server.task.send({'req': 'quit', 'node_auth': _dispycos_node_auth})
            if _dispycos_spawn_proc:
                _dispycos_send_pipe.send({'req': 'quit'})
                if _dispycos_send_pipe.poll(5) and _dispycos_send_pipe.recv() == 'closed':
                    _dispycos_spawn_proc = None
                else:
                    _dispycos_spawn_proc.terminate()
                    _dispycos_spawn_proc = None
                while _dispycos_send_pipe.poll():  # clear pipe
                    _dispycos_send_pipe.recv()
                while _dispycos_recv_pipe.poll():  # clear pipe
                    _dispycos_recv_pipe.recv()
            for name in os.listdir(_dispycos_config['dest_path']):
                if name.startswith('dispycosproc-') or name == 'dispycosscheduler':
                    continue
                name = os.path.join(_dispycos_config['dest_path'], name)
                if os.path.isdir(name):
                    shutil.rmtree(name, ignore_errors=True)
                elif os.path.isfile(name):
                    try:
                        os.remove(name)
                    except:
                        pass
            task_scheduler.discover_peers(port=_dispycos_config['scheduler_port'])

        if _dispycos_service_start:
            pycos.Task(service_times_proc)

        qserver = threading.Thread(target=mp_queue_server)
        qserver.daemon = True
        qserver.start()
        task_scheduler.peer_status(pycos.Task(monitor_peers))
        task_scheduler.discover_peers(port=_dispycos_config['scheduler_port'])
        for peer in _dispycos_config['peers']:
            pycos.Task(task_scheduler.peer, pycos.deserialize(peer))

        # TODO: create new pipe for each computation instead?
        _dispycos_recv_pipe, _dispycos_send_pipe = multiprocessing.Pipe(duplex=True)

        while 1:
            msg = yield task.receive(timeout=interval)
            now = time.time()
            if msg:
                try:
                    req = msg['req']
                except:
                    req = ''

                if req == 'server_setup':
                    try:
                        server = _dispycos_servers[msg['id'] - 1]
                        assert msg['auth'] == cur_computation_auth
                    except:
                        pass
                    else:
                        if not server.task:
                            server.task = msg['task']
                        last_pulse = now

                elif req == 'dispycos_node_info':
                    # request from scheduler
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
                        info = DispycosNodeInfo(_dispycos_name, task.location.addr,
                                                len(_dispycos_servers), platform.platform(), info)
                        client.send(info)

                elif req == 'reserve':
                    # request from scheduler
                    client = msg.get('client', None)
                    cpus = msg.get('cpus', -1)
                    auth = msg.get('auth', None)
                    if (isinstance(client, pycos.Task) and isinstance(cpus, int) and
                        cpus >= 0 and not cur_computation_auth and not scheduler_task and
                        service_available(now) and (len(_dispycos_servers) >= cpus) and auth and
                        isinstance(msg.get('status_task', None), pycos.Task) and
                        isinstance(msg.get('computation_location', None), pycos.Location)):
                        if (yield task_scheduler.peer(msg['computation_location'])):
                            cpus = 0
                        else:
                            close_computation()
                            for server in _dispycos_servers:
                                if server.task:
                                    yield task.sleep(0.1)
                            if not cpus:
                                cpus = len(_dispycos_servers)
                        if ((yield client.deliver(cpus, timeout=min(msg_timeout, interval))) == 1 and
                            cpus):
                            cur_computation_auth = auth
                            last_pulse = now
                            _dispycos_busy_time.value = int(time.time())
                            scheduler_task = msg['status_task']
                    elif isinstance(client, pycos.Task):
                        client.send(0)

                elif req == 'computation':
                    client = msg.get('client', None)
                    computation = msg.get('computation', None)
                    if (cur_computation_auth == msg.get('auth', None) and
                        isinstance(client, pycos.Task) and isinstance(computation, Computation)):
                        interval = computation._pulse_interval
                        last_pulse = now
                        if interval:
                            interval = min(interval, _dispycos_config['max_pulse_interval'])
                        else:
                            interval = _dispycos_config['max_pulse_interval']
                        _dispycos_busy_time.value = int(time.time())
                        _dispycos_config['scheduler_task'] = pycos.serialize(scheduler_task)
                        _dispycos_config['computation_auth'] = computation._auth
                        id_ports = [(server.id, _dispycos_tcp_ports[server.id - 1])
                                    for server in _dispycos_servers if not server.task]
                        args = (_dispycos_config, id_ports, _dispycos_mp_queue, _dispycos_recv_pipe,
                                pycos.serialize(computation) if os.name == 'nt'
                                else computation, msg.get('setup_args', ()))
                        _dispycos_spawn_proc = multiprocessing.Process(target=_dispycos_spawn,
                                                                       args=args)
                        _dispycos_spawn_proc.start()
                        if _dispycos_send_pipe.poll(10):
                            cpus = _dispycos_send_pipe.recv()
                        else:
                            cpus = 0
                        if ((yield client.deliver(cpus)) == 1) and cpus:
                            pass
                        else:
                            close_computation()

                elif req == 'release':
                    auth = msg.get('auth', None)
                    if cur_computation_auth and auth == cur_computation_auth:
                        close_computation()
                        cur_computation_auth = None
                        scheduler_task = None
                        interval = MaxPulseInterval
                        released = 'released'
                    else:
                        released = 'invalid'
                    client = msg.get('client', None)
                    if isinstance(client, pycos.Task):
                        client.send(released)
                    if released == 'released' and _dispycos_config['serve'] > 0:
                        _dispycos_config['serve'] -= 1
                        if not _dispycos_config['serve']:
                            break

                elif req == 'close' or req == 'quit' or req == 'terminate':
                    auth = msg.get('auth', None)
                    if auth == _dispycos_node_auth:
                        close_computation()
                        cur_computation_auth = None
                        scheduler_task = None
                        interval = MaxPulseInterval
                        if req == 'quit' or req == 'terminate':
                            _dispycos_config['serve'] = 0
                            if all(not server.task for server in _dispycos_servers):
                                # _dispycos_mp_queue.close()
                                _dispycos_send_pipe.close()
                                _dispycos_recv_pipe.close()
                                break

                else:
                    pycos.logger.warning('Invalid message %s ignored',
                                         str(msg) if isinstance(msg, dict) else '')

            if scheduler_task:
                stask = scheduler_task  # copy in case scheduler closes meanwhile
                msg = {'status': 'pulse', 'location': task.location}
                if psutil:
                    msg['node_status'] = DispycosNodeAvailInfo(
                        task.location, 100.0 - psutil.cpu_percent(),
                        psutil.virtual_memory().available, psutil.disk_usage(disk_path).free,
                        100.0 - psutil.swap_memory().percent)

                sent = yield stask.deliver(msg, timeout=msg_timeout)
                if sent == 1:
                    last_pulse = now
                elif (now - last_pulse) > (5 * interval):
                    pycos.logger.warning('Scheduler is not reachable; closing computation "%s"',
                                         cur_computation_auth)
                    close_computation()
                    cur_computation_auth = None
                    scheduler_task = None
                    interval = MaxPulseInterval
                    pycos.Task(task_scheduler.close_peer, stask.location)
                    if _dispycos_config['serve'] > 0:
                        _dispycos_config['serve'] -= 1
                        if not _dispycos_config['serve']:
                            break

                if (zombie_period and ((now - _dispycos_busy_time.value) > zombie_period) and
                    cur_computation_auth):
                    pycos.logger.warning('Closing zombie computation "%s"', cur_computation_auth)
                    close_computation()
                    cur_computation_auth = None
                    scheduler_task = None
                    interval = MaxPulseInterval
                    if _dispycos_config['serve'] > 0:
                        _dispycos_config['serve'] -= 1
                        if not _dispycos_config['serve']:
                            break

            if ping_interval and (now - last_ping) > ping_interval and service_available(now):
                task_scheduler.discover_peers(port=_dispycos_config['scheduler_port'])

        try:
            os.remove(_dispycos_node_pid_file)
        except:
            pass
        os.kill(os.getpid(), signal.SIGINT)

    _dispycos_server_config = {}
    for _dispycos_var in ['udp_port', 'tcp_port', 'node', 'ext_ip_addr', 'name',
                          'discover_peers', 'secret', 'certfile', 'keyfile', 'dest_path',
                          'max_file_size']:
        _dispycos_server_config[_dispycos_var] = _dispycos_config.get(_dispycos_var, None)
    _dispycos_server_config['name'] = '%s_proc-0' % _dispycos_name
    _dispycos_server_config['tcp_port'] = _dispycos_tcp_ports.pop(0)
    if _dispycos_config['loglevel']:
        pycos.logger.setLevel(pycos.Logger.DEBUG)
        # pycos.logger.show_ms(True)
    else:
        pycos.logger.setLevel(pycos.Logger.INFO)
    _dispycos_scheduler = pycos.Pycos(**_dispycos_server_config)
    _dispycos_scheduler.dest_path = os.path.join(_dispycos_scheduler.dest_path, 'dispycos')
    _dispycos_node_pid_file = os.path.join(_dispycos_scheduler.dest_path, 'dispycosproc-0.pid')
    if _dispycos_config['clean']:
        try:
            os.remove(_dispycos_node_pid_file)
        except:
            pass
    try:
        _dispycos_var = os.open(_dispycos_node_pid_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0600)
        os.write(_dispycos_var, str(os.getpid()).encode())
        os.close(_dispycos_var)
    except:
        raise Exception('Another dispycosnode seem to be running; '
                        'check no dispycosnode and servers are running and '
                        'remove *.pid files in %s' % _dispycos_scheduler.dest_path)

    _dispycos_config['name'] = _dispycos_name
    _dispycos_config['dest_path'] = _dispycos_scheduler.dest_path
    _dispycos_config['node_auth'] = _dispycos_node_auth
    _dispycos_config['busy_time'] = _dispycos_busy_time
    _dispycos_node_task = pycos.Task(_dispycos_node_proc)
    del _dispycos_server_config, _dispycos_var

    def sighandler(signum, frame):
        if os.path.isfile(_dispycos_node_pid_file):
            _dispycos_node_task.send({'req': 'quit', 'auth': _dispycos_node_auth})
        else:
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

    if _dispycos_daemon:
        while 1:
            try:
                time.sleep(3600)
            except:
                if os.path.exists(_dispycos_node_pid_file):
                    _dispycos_node_task.send({'req': 'quit', 'auth': _dispycos_node_auth})
                break
    else:
        while 1:
            # wait a bit for any output for previous command is done
            time.sleep(0.2)
            try:
                _dispycos_cmd = raw_input(
                        '\nEnter\n'
                        '  "status" to get status\n'
                        '  "close" to stop accepting new jobs and\n'
                        '          close current computation when current jobs are finished\n'
                        '  "quit" to "close" current computation and exit dispycosnode\n'
                        '  "terminate" to kill current jobs and "quit": ')
            except:
                if os.path.exists(_dispycos_node_pid_file):
                    _dispycos_node_task.send({'req': 'quit', 'auth': _dispycos_node_auth})
                break
            else:
                _dispycos_cmd = _dispycos_cmd.strip().lower()
                if not _dispycos_cmd:
                    _dispycos_cmd = 'status'

            print('')
            if _dispycos_cmd == 'status':
                for _dispycos_server in _dispycos_servers:
                    if _dispycos_server.task:
                        _dispycos_server.task.send({'req': _dispycos_cmd,
                                                    'node_auth': _dispycos_node_auth})
                    else:
                        print('  dispycos server "%s" is not currently used' %
                              _dispycos_server.name)
            elif _dispycos_cmd in ('close', 'quit', 'terminate'):
                _dispycos_node_task.send({'req': _dispycos_cmd, 'auth': _dispycos_node_auth})
                break

    try:
        _dispycos_node_task.value()
    except:
        pass
    exit(0)
