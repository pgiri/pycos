"""
This file is part of pycos; see https://pycos.org for details.
"""

import sys
import os
import threading
import json
import cgi
import time
import socket
import ssl
import re
import traceback

import pycos
from pycos.dispycos import DispycosStatus, DispycosNodeAvailInfo, Scheduler
import pycos.dispycos as dispycos

if sys.version_info.major > 2:
    import http.server as BaseHTTPServer
    from urllib.parse import urlparse
else:
    import BaseHTTPServer
    from urlparse import urlparse


__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright 2015, Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "Apache 2.0"
__url__ = "https://pycos.org"

__all__ = ['HTTPServer']


if sys.version_info.major >= 3:
    def dict_iter(arg, iterator):
        return getattr(arg, iterator)()
else:
    def dict_iter(arg, iterator):
        return getattr(arg, 'iter' + iterator)()


class HTTPServer(object):

    NodeStatus = {Scheduler.NodeDiscovered: 'Discovered',
                  Scheduler.NodeInitialized: 'Initialized',
                  Scheduler.NodeClosed: 'Closed',
                  Scheduler.NodeIgnore: 'Ignore',
                  Scheduler.NodeDisconnected: 'Disconnected',
                  Scheduler.NodeSuspended: 'Suspended',
                  Scheduler.NodeResumed: 'Initialized'}
    ServerStatus = {Scheduler.ServerDiscovered: 'Discovered',
                    Scheduler.ServerInitialized: 'Initialized',
                    Scheduler.ServerClosed: 'Closed',
                    Scheduler.ServerDisconnected: 'Disconnected',
                    Scheduler.ServerSuspended: 'Suspended',
                    Scheduler.ServerResumed: 'Initialized'}

    ip_re = re.compile(r'^((\d+\.\d+\.\d+\.\d+)|([0-9a-f:]+))$')
    loc_re = re.compile(r'^((\d+\.\d+\.\d+\.\d+)|([0-9a-f:]+)):(\d+)$')

    class _Node(object):
        def __init__(self, name, addr):
            self.name = name
            self.addr = addr
            self.status = None
            self.servers = {}
            self.update_time = time.time()
            self.tasks_submitted = 0
            self.tasks_done = 0
            self.avail_info = None

    class _Server(object):
        def __init__(self, location):
            self.location = location
            self.status = None
            self.tasks = {}
            self.tasks_submitted = 0
            self.tasks_done = 0

    class _HTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

        def __init__(self, ctx, DocumentRoot, *args):
            self._ctx = ctx
            self.DocumentRoot = DocumentRoot
            BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args)

        def log_message(self, fmt, *args):
            # pycos.logger.debug('HTTP client %s: %s', self.client_address[0], fmt % args)
            return

        @staticmethod
        def json_encode_nodes(arg):
            nodes = [dict(node.__dict__) for node in dict_iter(arg, 'values')]
            for node in nodes:
                node['servers'] = len(node['servers'])
                node['avail_info'] = node['avail_info'].__dict__
            return nodes

        def do_GET(self):
            http_req = self.path[1:]

            if http_req == 'cluster_updates':
                self._ctx._lock.acquire()
                nodes = self.__class__.json_encode_nodes(self._ctx._updates)
                self._ctx._updates.clear()
                self._ctx._lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(nodes).encode())
                return

            elif http_req == 'cluster_status':
                self._ctx._lock.acquire()
                nodes = self.__class__.json_encode_nodes(self._ctx._nodes)
                self._ctx._lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(nodes).encode())
                return

            else:
                path = urlparse(http_req).path
                if not path or path == 'index.html':
                    path = 'cluster.html'
                path = os.path.join(self.DocumentRoot, path)
                try:
                    with open(path) as fd:
                        data = fd.read()
                    if path.endswith('.html'):
                        if path.endswith('.html'):
                            data = data % {'TIMEOUT': str(self._ctx._poll_sec),
                                           'SHOW_TASK_ARGS': 'true' if self._ctx._show_args
                                                                    else 'false'}
                        content_type = 'text/html'
                    elif path.endswith('.js'):
                        content_type = 'text/javascript'
                    elif path.endswith('.css'):
                        content_type = 'text/css'
                    elif path.endswith('.ico'):
                        content_type = 'image/x-icon'
                    self.send_response(200)
                    self.send_header('Content-Type', content_type)
                    if content_type == 'text/css' or content_type == 'text/javascript':
                        self.send_header('Cache-Control', 'private, max-age=86400')
                    self.end_headers()
                    self.wfile.write(data.encode())
                    return
                except Exception:
                    pycos.logger.warning('HTTP client %s: Could not read/send "%s"',
                                         self.client_address[0], path)
                    pycos.logger.debug(traceback.format_exc())
                self.send_error(404)
                return
            pycos.logger.debug('Bad GET request from %s: %s',
                               self.client_address[0], http_req)
            self.send_error(400)
            return

        def do_POST(self):
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers,
                                    environ={'REQUEST_METHOD': 'POST'})
            http_req = self.path[1:]

            if http_req == 'server_info':
                server = None
                max_tasks = 0
                for item in form.list:
                    if item.name == 'location':
                        m = re.match(HTTPServer.loc_re, item.value)
                        if m:
                            node = self._ctx._nodes.get(m.group(1))
                            if node:
                                server = node.servers.get(item.value)
                    elif item.name == 'limit':
                        try:
                            max_tasks = int(item.value)
                        except Exception:
                            pass
                if server:
                    if 0 < max_tasks < len(server.tasks):
                        rtasks = []
                        for i, rtask in enumerate(dict_iter(server.tasks, 'values')):
                            if i >= max_tasks:
                                break
                            rtasks.append(rtask)
                    else:
                        rtasks = server.tasks.values()
                    show_args = self._ctx._show_args
                    rtasks = [{'task': str(rtask.task), 'name': rtask.task.name,
                               'args': ', '.join(str(arg) for arg in rtask.args)
                                       if show_args else '',
                               'kwargs': ', '.join('%s=%s' % (key, val)
                                                   for key, val in rtask.kwargs.items())
                                         if show_args else '',
                               'start_time': rtask.start_time
                               } for rtask in rtasks]
                    info = {'location': str(server.location),
                            'status': server.status, 'tasks_submitted': server.tasks_submitted,
                            'tasks_done': server.tasks_done, 'tasks': rtasks,
                            'update_time': node.update_time}
                else:
                    info = {}
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(info).encode())
                return

            elif http_req == 'node_info':
                addr = None
                for item in form.list:
                    if item.name == 'host':
                        if re.match(HTTPServer.ip_re, item.value):
                            addr = item.value
                        else:
                            try:
                                info = socket.getaddrinfo(item.value, None)[0]
                                ip_addr = info[4][0]
                                if info[0] == socket.AF_INET6:
                                    ip_addr = re.sub(r'^0+', '', ip_addr)
                                    ip_addr = re.sub(r':0+', ':', ip_addr)
                                    ip_addr = re.sub(r'::+', '::', ip_addr)
                                addr = ip_addr
                            except Exception:
                                addr = item.value
                        break
                node = self._ctx._nodes.get(addr)
                if node:
                    info = {'addr': node.addr, 'name': node.name,
                            'status': node.status, 'update_time': node.update_time,
                            'avail_info': node.avail_info.__dict__,
                            'tasks_submitted': node.tasks_submitted, 'tasks_done': node.tasks_done,
                            'servers': [
                                {'location': str(server.location),
                                 'tasks_submitted': server.tasks_submitted,
                                 'tasks_done': server.tasks_done,
                                 'tasks_running': len(server.tasks),
                                 'update_time': node.update_time
                                 } for server in node.servers.values()
                                ]
                            }
                else:
                    info = {}
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(info).encode())
                return

            elif http_req == 'terminate_tasks':
                tasks = []
                for item in form.list:
                    if item.name == 'task':
                        try:
                            tasks.append(item.value)
                        except ValueError:
                            pycos.logger.debug('Terminate: task "%s" is invalid', item.value)

                terminated = []
                self._ctx._lock.acquire()
                for task in tasks:
                    s = task.split('@')
                    if len(s) != 2:
                        continue
                    location = s[1]
                    s = location.split(':')
                    if len(s) != 2:
                        continue
                    node = self._ctx._nodes.get(s[0])
                    if not node:
                        continue
                    server = node.servers.get(location)
                    if not server:
                        continue
                    rtask = server.tasks.get(task)
                    if rtask and rtask.task.terminate() == 0:
                        terminated.append(task)
                self._ctx._lock.release()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(terminated).encode())
                return

            elif http_req == 'update':
                for item in form.list:
                    if item.name == 'timeout':
                        try:
                            timeout = int(item.value)
                            if timeout < 1:
                                timeout = 0
                            self._ctx._poll_sec = timeout
                        except Exception:
                            pycos.logger.warning('HTTP client %s: invalid timeout "%s" ignored',
                                                 self.client_address[0], item.value)
                    elif item.name == 'show_task_args':
                        if item.value == 'true':
                            self._ctx._show_args = True
                        else:
                            self._ctx._show_args = False

                self.send_response(200)
                self.send_header('Content-Type', 'text/html')
                self.end_headers()
                return

            elif (http_req == 'suspend_node' or http_req == 'resume_node'):
                method = getattr(self._ctx.client, http_req)
                if not method:
                    return
                nodes = []
                for item in form.list:
                    if item.name == 'nodes':
                        nodes = json.loads(item.value)
                        break
                for node in nodes:
                    method(node)
                return

            elif (http_req == 'suspend_server' or http_req == 'resume_server'):
                method = getattr(self._ctx.client, http_req)
                if not method:
                    return
                servers = []
                for item in form.list:
                    if item.name == 'servers':
                        servers = json.loads(item.value)
                        break
                for loc in servers:
                    loc = loc.split(':')
                    if len(loc) != 2:
                        continue
                    try:
                        loc = pycos.Location(loc[0], loc[1])
                    except Exception:
                        continue
                    method(loc)
                return

            else:
                pycos.logger.debug('Bad POST request from %s: %s',
                                   self.client_address[0], http_req)
                self.send_error(400)
                return

    def __init__(self, client, host='', port=8181, poll_sec=10, DocumentRoot=None,
                 keyfile=None, certfile=None, show_task_args=True):
        self._lock = threading.Lock()
        if not DocumentRoot:
            DocumentRoot = os.path.join(os.path.dirname(__file__), 'data')
        self._nodes = {}
        self._updates = {}
        if poll_sec < 1:
            pycos.logger.warning('invalid poll_sec value %s; it must be at least 1', poll_sec)
            poll_sec = 1
        self._poll_sec = poll_sec
        self._show_args = bool(show_task_args)
        self._server = BaseHTTPServer.HTTPServer((host, port), lambda *args:
                                  HTTPServer._HTTPRequestHandler(self, DocumentRoot, *args))
        if certfile:
            self._server.socket = ssl.wrap_socket(self._server.socket, keyfile=keyfile,
                                                  certfile=certfile, server_side=True)
        self._httpd_thread = threading.Thread(target=self._server.serve_forever)
        self._httpd_thread.daemon = True
        self._httpd_thread.start()
        self.client = client
        self.status_task = pycos.SysTask(self.status_proc)
        if client.status_task:
            prev_status_task = client.status_task

            def chain_msgs(task=None):
                task.set_daemon()
                while 1:
                    msg = yield task.receive()
                    self.status_task.send(msg)
                    prev_status_task.send(msg)
            client.status_task = pycos.SysTask(chain_msgs)
        else:
            client.status_task = self.status_task
        pycos.logger.info('Started HTTP%s server at %s',
                          's' if certfile else '', str(self._server.socket.getsockname()))

    def status_proc(self, task=None):
        task.set_daemon()
        while True:
            msg = yield task.receive()
            if isinstance(msg, pycos.MonitorStatus):
                if isinstance(msg.info, pycos.Task):
                    rtask = msg.info
                    node = self._nodes.get(rtask.location.addr)
                    if node:
                        server = node.servers.get(str(rtask.location))
                        if server:
                            if server.tasks.pop(str(rtask), None) is not None:
                                server.tasks_done += 1
                                node.tasks_done += 1
                                node.update_time = time.time()
                                self._updates[node.addr] = node
            elif isinstance(msg, DispycosStatus):
                if msg.status == Scheduler.TaskStarted:
                    rtask = msg.info
                    node = self._nodes.get(rtask.task.location.addr)
                    if node:
                        server = node.servers.get(str(rtask.task.location))
                        if server:
                            server.tasks[str(rtask.task)] = rtask
                            server.tasks_submitted += 1
                            node.tasks_submitted += 1
                            node.update_time = time.time()
                            self._updates[node.addr] = node
                elif (msg.status == Scheduler.ServerInitialized or
                      msg.status == Scheduler.ServerResumed):
                    node = self._nodes.get(msg.info.addr)
                    if node:
                        server = node.servers.get(str(msg.info), None)
                        if not server:
                            server = HTTPServer._Server(msg.info)
                            node.servers[str(server.location)] = server
                        server.status = HTTPServer.ServerStatus[msg.status]
                        node.update_time = time.time()
                        self._updates[node.addr] = node
                elif (msg.status == Scheduler.ServerClosed or
                      msg.status == Scheduler.ServerDisconnected or
                      msg.status == Scheduler.ServerSuspended):
                    node = self._nodes.get(msg.info.addr)
                    if node:
                        server = node.servers.get(str(msg.info), None)
                        if server:
                            server.status = HTTPServer.ServerStatus[msg.status]
                            node.update_time = time.time()
                            self._updates[node.addr] = node
                elif (msg.status == Scheduler.NodeInitialized or
                      msg.status == Scheduler.NodeResumed):
                    node = self._nodes.get(msg.info.addr)
                    if not node:
                        node = HTTPServer._Node(msg.info.name, msg.info.addr)
                        node.avail_info = msg.info.avail_info
                        node.avail_info.location = None
                        self._nodes[msg.info.addr] = node
                    node.status = HTTPServer.NodeStatus[msg.status]
                    node.update_time = time.time()
                    self._updates[node.addr] = node
                elif (msg.status == Scheduler.NodeClosed or
                      msg.status == Scheduler.NodeDisconnected or
                      msg.status == Scheduler.NodeSuspended):
                    node = self._nodes.get(msg.info.addr)
                    if node:
                        node.status = HTTPServer.NodeStatus[msg.status]
                        node.update_time = time.time()
                        self._updates[node.addr] = node
            elif isinstance(msg, DispycosNodeAvailInfo):
                node = self._nodes.get(msg.location.addr, None)
                if node:
                    node.avail_info = msg
                    node.avail_info.location = None
                    node.update_time = time.time()
                    self._updates[node.addr] = node
            else:
                pycos.logger.warning('Status message ignored: %s', type(msg))

    def shutdown(self, wait=True):
        """This method should be called by user program to close the
        http server. If 'wait' is True the server waits for poll_sec
        so the http client gets all the updates before server is
        closed.
        """
        if wait:
            pycos.logger.info('HTTP server waiting for %s seconds for client updates '
                              'before quitting', self._poll_sec)
            if pycos.Pycos().cur_task():
                def _shutdown(task=None):
                    yield task.sleep(self._poll_sec + 0.5)
                    self._server.shutdown()
                    self._server.server_close()
                pycos.SysTask(_shutdown)
            else:
                time.sleep(self._poll_sec + 0.5)
                self._server.shutdown()
                self._server.server_close()
        else:
            self._server.shutdown()
            self._server.server_close()
