"""
This file is part of pycos; see https://pycos.org for details.

This module provides framework for concurrent, asynchronous network programming
with tasks, asynchronous completions and message passing. Other modules in pycos
download package provide API for distributed programming, asynchronous pipes,
distributed concurrent communicating processes.
"""

import time
import threading
from functools import partial as partial_func
import socket
import inspect
import traceback
import select
import sys
import types
import struct
import re
import errno
import platform
import ssl
from heapq import heappush, heappop
from bisect import bisect_left
import queue
import atexit
import collections
import pickle
import copy

if platform.system() == 'Windows':
    from errno import WSAEINPROGRESS as EINPROGRESS
    from errno import WSAEWOULDBLOCK as EWOULDBLOCK
    from errno import WSAEINVAL as EINVAL
    if sys.version_info < (3, 3):
        from time import clock as _time
        _time()
    if not hasattr(socket, 'IPPROTO_IPV6'):
        socket.IPPROTO_IPV6 = 41
else:
    from errno import EINPROGRESS
    from errno import EWOULDBLOCK
    from errno import EINVAL
    from time import time as _time

if sys.version_info >= (3, 3):
    from time import perf_counter as _time

from pycos.config import MsgTimeout, PickleProtocolVersion


__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__email__ = "pgiri@yahoo.com"
__copyright__ = "Copyright (c) 2012-2014 Giridhar Pemmasani"
__contributors__ = []
__maintainer__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__license__ = "Apache 2.0"
__url__ = "https://pycos.org"
__status__ = "Production"
__version__ = "4.11.0"

__all__ = ['Task', 'Pycos', 'Lock', 'RLock', 'Event', 'Condition', 'Semaphore',
           'AsyncSocket', 'HotSwapException', 'MonitorStatus', 'Location', 'Channel',
           'CategorizeMessages', 'AsyncThreadPool', 'AsyncDBCursor',
           'Singleton', 'logger', 'serialize', 'deserialize', 'Logger']

# PyPI / pip packaging adjusts assertion below for Python 3.7+
assert sys.version_info.major == 3 and sys.version_info.minor < 7, \
    ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
     (__file__, sys.version_info.major, sys.version_info.minor))
if PickleProtocolVersion is None:
    PickleProtocolVersion = pickle.HIGHEST_PROTOCOL
elif PickleProtocolVersion == 0:
    PickleProtocolVersion = pickle.DEFAULT_PROTOCOL
elif not isinstance(PickleProtocolVersion, int):
    raise Exception('PickleProtocolVersion must be an integer')


def serialize(obj):
    return pickle.dumps(obj, protocol=PickleProtocolVersion)


def deserialize(pkl):
    return pickle.loads(pkl)


class Singleton(type):
    """
    Meta class for singleton instances.
    """

    _memo = {}

    def __call__(cls, *args, **kwargs):
        c = Singleton._memo.get(cls, None)
        if not c:
            c = Singleton._memo[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return c

    @classmethod
    def discard(_, cls):
        """
        Forget singleton instance.
        """
        Singleton._memo.pop(cls, None)


class Logger(object):
    """Simple(r) (and more efficient) version of logging mechanism with limited
    features.
    """

    DEBUG = 10
    INFO = 20
    WARN = WARNING = 30
    ERROR = 40
    CRITICAL = FATAL = 50

    def __init__(self, name, stream=sys.stdout, level=None, log_ms=False):
        """
        'name' is appeneded to timestamp (similar to 'logging' module).
        'stream' (default is sys.stdout) is where log entry is written to.
        'level' is initial log level.
        """
        self.name = name
        self.stream = stream
        self.level = level if level else Logger.INFO
        self.log_ms = log_ms
        self.setLevel(self.level)

    def setLevel(self, level):
        """Set to new log level.
        """
        self.level = level
        if level <= Logger.DEBUG:
            self.debug = self.log
        else:
            self.debug = self.nolog
        if level <= Logger.WARNING:
            self.warn = self.warning = self.log
        else:
            self.warn = self.warning = self.nolog
        if level <= Logger.INFO:
            self.info = self.log
        else:
            self.info = self.nolog
        if level <= Logger.ERROR:
            self.error = self.log
        else:
            self.error = self.nolog
        if level <= Logger.FATAL:
            self.critical = self.fatal = self.log
        else:
            self.critical = self.fatal = self.nolog

    def show_ms(self, flag):
        """If 'flag' is True, milliseconds is shown in timestamp.
        """
        self.log_ms = bool(flag)

    def log(self, message, *args):
        now = time.time()
        if args:
            message = message % args
        if self.log_ms:
            self.stream.write('%s.%03d %s - %s\n' %
                              (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now)),
                               1000 * (now - int(now)), self.name, message))
        else:
            self.stream.write('%s %s - %s\n' %
                              (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now)),
                               self.name, message))

    def nolog(self, message, *args):
        return

    def flush(self):
        self.stream.flush()

    shutdown = flush

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.shutdown()


logger = Logger('pycos')


class _AsyncSocket(object):
    """Base class for use with pycos, for asynchronous I/O completion and pyco
    tasks. This class is for internal use only. Use AsyncSocket, defined below,
    instead.
    """

    __slots__ = ('_rsock', '_keyfile', '_certfile', '_ssl_version', '_fileno', '_timeout',
                 '_timeout_id', '_read_task', '_read_fn', '_read_result', '_write_task',
                 '_write_fn', '_write_result', '_scheduler', '_notifier', '_event',
                 'recvall', 'sendall', 'recv_msg', 'send_msg', '_blocking', 'recv', 'send',
                 'recvfrom', 'sendto', 'accept', 'connect', 'ssl_server_ctx')

    _default_timeout = None
    _MsgLengthSize = struct.calcsize('>L')
    _ssl_protocol = getattr(ssl, 'PROTOCOL_TLS', ssl.PROTOCOL_SSLv23)

    def __init__(self, sock, blocking=False, keyfile=None, certfile=None,
                 ssl_version=None):
        """Setup socket for use wih pycos.

        @blocking=True implies synchronous sockets and blocking=False implies
        asynchronous sockets.

        @keyfile, @certfile and @ssl_version are as per ssl's wrap_socket
        method.

        Only methods without leading underscore should be used; other attributes
        are for internal use only. In addition to usual socket I/O methods,
        AsyncSocket implemnents 'recvall', 'send_msg', 'recv_msg' and 'unwrap'
        methods.
        """

        if isinstance(sock, AsyncSocket):
            logger.warning('Socket %s is already AsyncSocket', sock._fileno)
            for k in sock.__slots__:
                setattr(self, k, getattr(sock, k))
        else:
            self._rsock = sock
            self._keyfile = keyfile
            self._certfile = certfile
            if ssl_version:
                self._ssl_version = ssl_version
            else:
                self._ssl_version = _AsyncSocket._ssl_protocol
            self._fileno = sock.fileno()
            self._timeout = 0
            self._timeout_id = None
            self._read_task = None
            self._read_fn = None
            self._read_result = None
            self._write_task = None
            self._write_fn = None
            self._write_result = None
            self._scheduler = None
            self._notifier = None
            self._event = None
            self.ssl_server_ctx = None

            self.recvall = None
            self.sendall = None
            self.recv_msg = None
            self.send_msg = None

            self._blocking = None
            self.setblocking(blocking)
            # technically, we should set socket to blocking if
            # _default_timeout is None, but ignore this case
            if _AsyncSocket._default_timeout:
                self.settimeout(_AsyncSocket._default_timeout)

    def __getattr__(self, name):
        return getattr(self._rsock, name)

    def setblocking(self, blocking):
        if blocking:
            blocking = True
        else:
            blocking = False
        if self._blocking == blocking:
            return
        self._blocking = blocking
        if self._blocking:
            self._unregister()
            self._rsock.setblocking(1)
            if self._certfile and (not hasattr(self._rsock, 'do_handshake')):
                self._rsock = ssl.wrap_socket(self._rsock, keyfile=self._keyfile,
                                              certfile=self._certfile,
                                              ssl_version=self._ssl_version)
            for name in ['recv', 'send', 'recvfrom', 'sendto', 'accept', 'connect']:
                setattr(self, name, getattr(self._rsock, name))
            if self._rsock.type & socket.SOCK_STREAM:
                self.recvall = self._sync_recvall
                self.sendall = self._sync_sendall
                self.recv_msg = self._sync_recv_msg
                self.send_msg = self._sync_send_msg
                self.accept = self._sync_accept
            self._scheduler = None
            self._notifier = None
        else:
            self._rsock.setblocking(0)
            self.recv = self._async_recv
            self.send = self._async_send
            self.recvfrom = self._async_recvfrom
            self.sendto = self._async_sendto
            self.accept = self._async_accept
            self.connect = self._async_connect
            if self._rsock.type & socket.SOCK_STREAM:
                self.recvall = self._async_recvall
                self.sendall = self._async_sendall
                self.recv_msg = self._async_recv_msg
                self.send_msg = self._async_send_msg
            self._scheduler = Pycos.scheduler()
            if self._scheduler:
                self._notifier = self._scheduler._notifier
                self._register()
            else:
                self._notifier = None

    def _register(self):
        """Internal use only.
        """
        pass

    def _unregister(self):
        """Internal use only.
        """
        if self._notifier:
            self._notifier.unregister(self)
            self._notifier = None

    def close(self):
        """'close' must be called when done with socket.
        """
        self._unregister()
        if self._rsock:
            self._rsock.close()
            self._rsock = None
        self._read_fn = self._write_fn = None
        self._read_task = self._write_task = None

    def unwrap(self):
        """Get rid of AsyncSocket setup and return underlying socket object.
        """
        self._unregister()
        self._notifier = None
        self._read_task = self._write_task = None
        sock, self._rsock = self._rsock, None
        return sock

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trace):
        self.close()

    def __lt__(self, other):
        if isinstance(other, AsyncSocket):
            return self._fileno < other._fileno
        return 1

    @classmethod
    def get_ssl_version(cls):
        """
        Get default SSL version.
        """
        return _AsyncSocket._ssl_protocol

    @classmethod
    def set_ssl_version(cls, version):
        """
        Set default SSL version.
        """
        _AsyncSocket._ssl_protocol = version

    def setdefaulttimeout(self, timeout):
        if isinstance(timeout, (int, float)) and timeout > 0:
            self._rsock.setdefaulttimeout(timeout)
            self._default_timeout = timeout
        else:
            logger.warning('invalid timeout %s ignored', timeout)

    def getdefaulttimeout(self):
        if self._blocking:
            return self._rsock.getdefalttimeout()
        else:
            return _AsyncSocket._default_timeout

    def settimeout(self, timeout):
        if self._blocking:
            if timeout is None:
                pass
            elif not timeout:
                self.setblocking(0)
                self.settimeout(0.0)
            else:
                self._rsock.settimeout(timeout)
        else:
            if self._timeout_id:
                self._notifier._del_timeout(self)
            self._timeout = timeout

    def gettimeout(self):
        if self._blocking:
            return self._rsock.gettimeout()
        else:
            return self._timeout

    def _timed_out(self):
        """Internal use only.
        """
        if self._read_fn:
            buf = None
            if isinstance(self._read_result, bytearray):
                view = self._read_fn.args[1]
                n = len(self._read_result) - len(view)
                if n > 0:
                    buf = bytes(self._read_result[:n])
                if isinstance(view, memoryview):
                    view.release()
            if buf:
                self._read_task._proceed_(buf)
            else:
                self._read_task.throw(socket.timeout('timed out'))
            self._notifier.clear(self, _AsyncPoller._Read)
            self._read_fn = self._read_result = self._read_task = None
        if self._write_fn:
            sent = 0
            if isinstance(self._write_result, memoryview):
                sent = self._write_fn.args[1] - len(self._write_result)
                self._write_result.release()
            if sent:
                self._write_task._proceed_(sent)
            else:
                self._write_task.throw(socket.timeout('timed out'))
            self._notifier.clear(self, _AsyncPoller._Write)
            self._write_fn = self._write_result = self._write_task = None

    def _eof(self):
        """Internal use only.
        """
        if self._read_fn:
            self._read_fn()

    def _async_recv(self, bufsize, *args):
        """Internal use only; use 'recv' with 'yield' instead.

        Asynchronous version of socket recv method.
        """
        def _recv():
            try:
                buf = self._rsock.recv(bufsize, *args)
            except Exception:
                self._read_fn = None
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task.throw(*sys.exc_info())
            else:
                self._read_fn = None
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task._proceed_(buf)

        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
            self._notifier = self._scheduler._notifier
            self._register()
        self._read_task = Pycos.cur_task(self._scheduler)
        self._read_task._await_()
        self._read_fn = _recv
        self._notifier.add(self, _AsyncPoller._Read)
        if self._certfile and self._rsock.pending():
            try:
                buf = self._rsock.recv(bufsize, *args)
            except Exception:
                self._read_fn = None
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task.throw(*sys.exc_info())
            else:
                if buf:
                    self._read_fn = None
                    self._notifier.clear(self, _AsyncPoller._Read)
                    self._read_task._proceed_(buf)

    def _async_recvall(self, bufsize, *args):
        """Internal use only; use 'recvall' with 'yield' instead.

        Receive exactly bufsize bytes. If socket's timeout is set and it expires
        before all the data could be read, it returns partial data read if any
        data has been read at all. If no data has been read before timeout, then
        it causes 'socket.timeout' exception to be thrown.
        """

        def _recvall(self, view):
            try:
                recvd = self._rsock.recv_into(view, len(view), *args)
            except Exception:
                view.release()
                self._read_fn = self._read_result = None
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task.throw(*sys.exc_info())
            else:
                if recvd:
                    view = view[recvd:]
                    if len(view) == 0:
                        view.release()
                        buf = self._read_result
                        self._read_fn = self._read_result = None
                        self._notifier.clear(self, _AsyncPoller._Read)
                        self._read_task._proceed_(buf)
                    else:
                        self._read_fn = partial_func(_recvall, self, view)
                        if self._timeout:
                            self._notifier._del_timeout(self)
                            self._notifier._add_timeout(self)
                else:
                    view.release()
                    self._read_fn = self._read_result = None
                    self._notifier.clear(self, _AsyncPoller._Read)
                    self._read_task._proceed_(b'')

        self._read_result = bytearray(bufsize)
        view = memoryview(self._read_result)
        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
            self._notifier = self._scheduler._notifier
            self._register()
        self._read_task = Pycos.cur_task(self._scheduler)
        self._read_task._await_()
        self._read_fn = partial_func(_recvall, self, view)
        self._notifier.add(self, _AsyncPoller._Read)
        if self._certfile and self._rsock.pending():
            try:
                recvd = self._rsock.recv_into(view, len(view), *args)
            except Exception:
                self._read_fn = self._read_result = None
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task.throw(*sys.exc_info())
            else:
                if recvd == bufsize:
                    view.release()
                    buf = self._read_result
                    self._read_fn = self._read_result = None
                    self._notifier.clear(self, _AsyncPoller._Read)
                    self._read_task._proceed_(buf)
                elif recvd:
                    view = view[recvd:]
                    self._read_fn = partial_func(_recvall, self, view)

    def _sync_recvall(self, bufsize, *args):
        """Internal use only; use 'recvall' instead.

        Synchronous version of async_recvall.
        """
        self._read_result = bytearray(bufsize)
        view = memoryview(self._read_result)
        while len(view) > 0:
            recvd = self._rsock.recv_into(view, *args)
            if not recvd:
                view.release()
                self._read_result = None
                return b''
            view = view[recvd:]
        view.release()
        buf, self._read_result = self._read_result, None
        return buf

    def _async_recvfrom(self, *args):
        """Internal use only; use 'recvfrom' with 'yield' instead.

        Asynchronous version of socket recvfrom method.
        """
        def _recvfrom():
            try:
                buf = self._rsock.recvfrom(*args)
            except Exception:
                self._read_fn = None
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task.throw(*sys.exc_info())
            else:
                self._read_fn = None
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task._proceed_(buf)

        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
            self._notifier = self._scheduler._notifier
            self._register()
        self._read_task = Pycos.cur_task(self._scheduler)
        self._read_task._await_()
        self._read_fn = _recvfrom
        self._notifier.add(self, _AsyncPoller._Read)

    def _async_send(self, *args):
        """Internal use only; use 'send' with 'yield' instead.

        Asynchronous version of socket send method.
        """
        def _send():
            try:
                sent = self._rsock.send(*args)
            except Exception:
                self._write_fn = None
                self._notifier.clear(self, _AsyncPoller._Write)
                self._write_task.throw(*sys.exc_info())
            else:
                self._write_fn = None
                self._notifier.clear(self, _AsyncPoller._Write)
                self._write_task._proceed_(sent)

        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
            self._notifier = self._scheduler._notifier
            self._register()
        self._write_task = Pycos.cur_task(self._scheduler)
        self._write_task._await_()
        self._write_fn = _send
        self._notifier.add(self, _AsyncPoller._Write)

    def _async_sendto(self, *args):
        """Internal use only; use 'sendto' with 'yield' instead.

        Asynchronous version of socket sendto method.
        """
        def _sendto():
            try:
                sent = self._rsock.sendto(*args)
            except Exception:
                self._write_fn = None
                self._notifier.clear(self, _AsyncPoller._Write)
                self._write_task.throw(*sys.exc_info())
            else:
                self._write_fn = None
                self._notifier.clear(self, _AsyncPoller._Write)
                self._write_task._proceed_(sent)

        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
            self._notifier = self._scheduler._notifier
            self._register()
        self._write_task = Pycos.cur_task(self._scheduler)
        self._write_task._await_()
        self._write_fn = _sendto
        self._notifier.add(self, _AsyncPoller._Write)

    def _async_sendall(self, data, *args):
        """Internal use only; use 'sendall' with 'yield' instead.

        Asynchronous version of socket sendall method. If socket's timeout is
        set and it expires before all the data could be sent, it returns the
        length of data sent if any data is sent. If no data has been sent before
        timeout, then it causes 'socket.timeout' exception to be thrown.
        """
        def _sendall(self, data_len):
            try:
                sent = self._rsock.send(self._write_result, *args)
                if sent < 0:
                    self._write_result.release()
                    self._write_fn = self._write_result = None
                    self._notifier.clear(self, _AsyncPoller._Write)
                    self._write_task.throw(*sys.exc_info())
            except socket.error as exc:
                # apparently BSD may raise EAGAIN
                if exc.errno != errno.EAGAIN:
                    self._write_fn = self._write_result = None
                    self._notifier.clear(self, _AsyncPoller._Write)
                    self._write_task.throw(*sys.exc_info())
            except Exception:
                self._write_result.release()
                self._write_fn = self._write_result = None
                self._notifier.clear(self, _AsyncPoller._Write)
                self._write_task.throw(*sys.exc_info())
            else:
                if sent > 0:
                    self._write_result = self._write_result[sent:]
                    if len(self._write_result) == 0:
                        self._write_result.release()
                        self._write_fn = self._write_result = None
                        self._notifier.clear(self, _AsyncPoller._Write)
                        self._write_task._proceed_(None)
                    # elif self._timeout:
                    #     self._notifier._del_timeout(self)
                    #     self._notifier._add_timeout(self)

        self._write_result = memoryview(data)
        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
            self._notifier = self._scheduler._notifier
            self._register()
        self._write_task = Pycos.cur_task(self._scheduler)
        self._write_task._await_()
        self._write_fn = partial_func(_sendall, self, len(data))
        self._notifier.add(self, _AsyncPoller._Write)

    def _sync_sendall(self, data, *args):
        """Internal use only; use 'sendall' instead.

        Synchronous version of async_sendall.
        """
        # TODO: is socket's sendall better?
        buf = memoryview(data)
        while len(buf) > 0:
            sent = self._rsock.send(buf, *args)
            if sent < 0:
                buf.release()
                raise socket.error('hangup')
            buf = buf[sent:]
        buf.release()
        return None

    def _async_accept(self):
        """Internal use only; use 'accept' with 'yield' instead.

        Asynchronous version of socket accept method. Socket in returned pair is
        asynchronous socket (instance of AsyncSocket with blocking=False).
        """
        def _accept():
            try:
                conn, addr = self._rsock.accept()
            except Exception:
                self._read_fn = None
                self._notifier.clear(self, _AsyncPoller._Read)
                self._read_task.throw(*sys.exc_info())
                return

            self._notifier.clear(self, _AsyncPoller._Read)
            self._read_fn = None

            if not self._certfile:
                conn = AsyncSocket(conn, blocking=False)
                self._read_task._proceed_((conn, addr))
                return

            # SSL connection
            if not self.ssl_server_ctx and hasattr(ssl, 'create_default_context'):
                self.ssl_server_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                self.ssl_server_ctx.load_cert_chain(certfile=self._certfile, keyfile=self._keyfile)

            conn = AsyncSocket(conn, blocking=False, keyfile=self._keyfile,
                               certfile=self._certfile, ssl_version=self._ssl_version)
            try:
                if self.ssl_server_ctx:
                    conn._rsock = self.ssl_server_ctx.wrap_socket(conn._rsock, server_side=True,
                                                                  do_handshake_on_connect=False)
                else:
                    conn._rsock = ssl.wrap_socket(conn._rsock, keyfile=self._keyfile,
                                                  certfile=self._certfile,
                                                  ssl_version=self._ssl_version, server_side=True,
                                                  do_handshake_on_connect=False)
            except Exception:
                self._read_task.throw(*sys.exc_info())
                self._read_task = None
                conn.close()
                return

            def _ssl_handshake():
                try:
                    conn._rsock.do_handshake()
                except ssl.SSLError as err:
                    if (err.args[0] == ssl.SSL_ERROR_WANT_READ or
                       err.args[0] == ssl.SSL_ERROR_WANT_WRITE):
                        pass
                    else:
                        conn._read_fn = conn._write_fn = None
                        conn._notifier.clear(conn, _AsyncPoller._Read | _AsyncPoller._Write)
                        conn._read_task.throw(*sys.exc_info())
                        conn.close()
                except Exception:
                    conn._read_fn = conn._write_fn = None
                    conn._notifier.clear(conn, _AsyncPoller._Read | _AsyncPoller._Write)
                    conn._read_task.throw(*sys.exc_info())
                    conn.close()
                else:
                    conn._read_fn = conn._write_fn = None
                    conn._notifier.clear(conn, _AsyncPoller._Read | _AsyncPoller._Write)
                    conn._read_task._proceed_((conn, addr))

            conn._read_task = conn._write_task = self._read_task
            self._read_task = self._read_fn = None
            conn._read_fn = conn._write_fn = _ssl_handshake
            conn._notifier.add(conn, _AsyncPoller._Read | _AsyncPoller._Write)
            conn._read_fn()

        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
            self._notifier = self._scheduler._notifier
            self._register()
        self._read_task = Pycos.cur_task(self._scheduler)
        self._read_task._await_()
        self._read_fn = _accept
        self._notifier.add(self, _AsyncPoller._Read)

    def _sync_accept(self, *args):
        """Internal use only; use 'accept' instead.

        'accept' for synchronous sockets.
        """

        conn, addr = self._rsock.accept(*args)
        conn = AsyncSocket(conn, blocking=True, keyfile=self._keyfile, certfile=self._certfile,
                           ssl_version=self._ssl_version)
        return (conn, addr)

    def _async_connect(self, *args):
        """Internal use only; use 'connect' with 'yield' instead.

        Asynchronous version of socket connect method.
        """
        def _connect():
            err = self._rsock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err:
                self._notifier.clear(self, _AsyncPoller._Write)
                self._write_task.throw(socket.error(err))
                self._write_task = self._write_fn = None
                return

            if not self._certfile:
                self._notifier.clear(self, _AsyncPoller._Write)
                self._write_task._proceed_(0)
                self._write_task = self._write_fn = None
                return

            # SSL connection
            try:
                # TODO: provide 'ca_certs' as special parameter to 'accept'?
                # For now this setup works for self-signed certs
                self._rsock = ssl.wrap_socket(self._rsock, ca_certs=self._certfile,
                                              cert_reqs=ssl.CERT_REQUIRED, server_side=False,
                                              do_handshake_on_connect=False)
            except Exception:
                self._write_task.throw(*sys.exc_info())
                self._write_task = self._write_fn = None
                self.close()
                return

            def _ssl_handshake():
                try:
                    self._rsock.do_handshake()
                except ssl.SSLError as err:
                    if (err.args[0] == ssl.SSL_ERROR_WANT_READ or
                       err.args[0] == ssl.SSL_ERROR_WANT_WRITE):
                        pass
                    else:
                        self._read_fn = self._write_fn = None
                        self._notifier.clear(self, _AsyncPoller._Read | _AsyncPoller._Write)
                        self._write_task.throw(*sys.exc_info())
                        self.close()
                except Exception:
                    self._read_fn = self._write_fn = None
                    self._notifier.clear(self, _AsyncPoller._Read | _AsyncPoller._Write)
                    self._write_task.throw(*sys.exc_info())
                    self.close()
                else:
                    self._read_fn = self._write_fn = None
                    self._notifier.clear(self, _AsyncPoller._Read | _AsyncPoller._Write)
                    self._write_task._proceed_(0)

            self._read_fn = self._write_fn = _ssl_handshake
            self._read_task = self._write_task
            self._notifier.add(self, _AsyncPoller._Read)
            self._write_fn()

        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
            self._notifier = self._scheduler._notifier
            self._register()
        self._write_task = Pycos.cur_task(self._scheduler)
        self._write_task._await_()
        self._write_fn = _connect
        self._notifier.add(self, _AsyncPoller._Write)
        try:
            self._rsock.connect(*args)
        except socket.error as e:
            if e.args[0] == EINPROGRESS or e.args[0] == EWOULDBLOCK:
                pass
            else:
                self._write_task._proceed_(None)
                raise

    def _async_send_msg(self, data):
        """Internal use only; use 'send_msg' with 'yield' instead.

        Messages are tagged with length of the data, so on the receiving side,
        recv_msg knows how much data to receive.
        """
        raise StopIteration((yield self.sendall(struct.pack('>L', len(data)) + data)))

    def _sync_send_msg(self, data):
        """Internal use only; use 'send_msg' instead.

        Synchronous version of async_send_msg.
        """
        return self._sync_sendall(struct.pack('>L', len(data)) + data)

    def _async_recv_msg(self):
        """Internal use only; use 'recv_msg' with 'yield' instead.

        Message is tagged with length of the payload (data). This method
        receives length of payload, then the payload and returns the payload.
        """
        n = AsyncSocket._MsgLengthSize
        try:
            data = yield self.recvall(n)
        except socket.error as err:
            if err.args[0] == 'hangup':
                # raise socket.error(errno.EPIPE, 'Insufficient data')
                raise StopIteration(b'')
            else:
                raise
        if len(data) != n:
            # raise socket.error(errno.EPIPE, 'Insufficient data: %s / %s' % (len(data), n))
            raise StopIteration(b'')
        n = struct.unpack('>L', data)[0]
        # assert n >= 0
        if n:
            try:
                data = yield self.recvall(n)
            except socket.error as err:
                if err.args[0] == 'hangup':
                    # raise socket.error(errno.EPIPE, 'Insufficient data')
                    raise StopIteration(b'')
                else:
                    raise
            if len(data) != n:
                # raise socket.error(errno.EPIPE, 'Insufficient data: %s / %s' % (len(data), n))
                raise StopIteration(b'')
            raise StopIteration(data)
        else:
            raise StopIteration(b'')

    def _sync_recv_msg(self):
        """Internal use only; use 'recv_msg' instead.

        Synchronous version of async_recv_msg.
        """
        n = AsyncSocket._MsgLengthSize
        try:
            data = self._sync_recvall(n)
        except socket.error as err:
            if err.args[0] == 'hangup':
                # raise socket.error(errno.EPIPE, 'Insufficient data')
                return b''
            else:
                raise
        if len(data) != n:
            # raise socket.error(errno.EPIPE, 'Insufficient data: %s / %s' % (len(data), n))
            return b''
        n = struct.unpack('>L', data)[0]
        # assert n >= 0
        if n:
            try:
                data = self._sync_recvall(n)
            except socket.error as err:
                if err.args[0] == 'hangup':
                    # raise socket.error(errno.EPIPE, 'Insufficient data')
                    return b''
                else:
                    raise
            if len(data) != n:
                # raise socket.error(errno.EPIPE, 'Insufficient data: %s / %s' % (len(data), n))
                return b''
            return data
        else:
            return b''

    def create_connection(self, host_port, timeout=None, source_address=None):
        if timeout is not None:
            self.settimeout(timeout)
        if source_address is not None:
            self._rsock.bind(source_address)
        yield self.connect(host_port)


if platform.system() == 'Windows':
    # use IOCP if pywin32 (http://pywin32.sf.net) is installed
    try:
        import pywintypes
        import win32file
        import win32event
        import winerror
    except ImportError:
        logger.warning('Could not load pywin32 for I/O Completion Ports; '
                       'using inefficient polling for sockets')
    else:
        # for UDP we need 'select' polling (pywin32 doesn't yet support UDP);
        # _AsyncPoller below is combination of the other _AsyncPoller for
        # epoll/poll/kqueue/select and _SelectNotifier below. (Un)fortunately,
        # most of it is duplicate code

        class _AsyncPoller(object):
            """Internal use only.
            """

            _Read = 0x1
            _Write = 0x2
            _Error = 0x4

            def __init__(self, iocp_notifier):
                self._poller_name = 'select'
                self._fds = {}
                self._terminate = False
                self.rset = set()
                self.wset = set()
                self.xset = set()
                self.iocp_notifier = iocp_notifier
                self.cmd_rsock, self.cmd_wsock = _AsyncPoller._socketpair()
                self.cmd_rsock.setblocking(0)
                self.cmd_wsock.setblocking(0)
                self.poller = select.select
                self._polling = False
                self._lock = threading.RLock()
                self.poll_thread = threading.Thread(target=self.poll)
                self.poll_thread.daemon = True
                self.poll_thread.start()

            def unregister(self, fd, update=True):
                fid = fd._fileno
                if fd._timeout:
                    self.iocp_notifier._del_timeout(fd)
                self._lock.acquire()
                if update:
                    if self._fds.pop(fid, None) != fd:
                        self._lock.release()
                        logger.debug('fd %s is not registered', fid)
                        return
                    event = fd._event
                    fd._event = None
                else:
                    event = fd._event
                    fd._event = 0
                if event:
                    if event & _AsyncPoller._Read:
                        self.rset.discard(fid)
                    if event & _AsyncPoller._Write:
                        self.wset.discard(fid)
                    if event & _AsyncPoller._Error:
                        self.xset.discard(fid)
                if update and self._polling:
                    self.cmd_wsock.send(b'u')
                self._lock.release()

            def add(self, fd, event):
                fid = fd._fileno
                self._lock.acquire()
                if fd._event:
                    event = event & ~fd._event
                    fd._event |= event
                else:
                    fd._event = event
                    self._fds[fid] = fd
                if event:
                    if event & _AsyncPoller._Read:
                        self.rset.add(fid)
                    if event & _AsyncPoller._Write:
                        self.wset.add(fid)
                    if event & _AsyncPoller._Error:
                        self.xset.add(fid)
                    if fd._timeout:
                        # TODO: add timeout only if not already in timeouts?
                        self.iocp_notifier._add_timeout(fd)
                        self.iocp_notifier.interrupt(fd._timeout)
                self._lock.release()
                if self._polling:
                    self.cmd_wsock.send(b'm')

            def clear(self, fd, event=0):
                fid = fd._fileno
                self._lock.acquire()
                if fd._event:
                    d = fd._event & event
                    if d:
                        if d & _AsyncPoller._Read:
                            self.rset.discard(fid)
                        if d & _AsyncPoller._Write:
                            self.wset.discard(fid)
                        if d & _AsyncPoller._Error:
                            self.xset.discard(fid)
                    fd._event &= ~event
                    if (not fd._event) and fd._timeout_id:
                        self.iocp_notifier._del_timeout(fd)
                    if self._polling:
                        self.cmd_wsock.send(b'm')
                self._lock.release()

            def poll(self):
                self.cmd_rsock = AsyncSocket(self.cmd_rsock)
                self.cmd_rsock._read_fn = lambda: self.cmd_rsock._rsock.recv(128)
                self.add(self.cmd_rsock, _AsyncPoller._Read)
                while 1:
                    self._polling = True
                    rlist, wlist, xlist = self.poller(self.rset, self.wset, self.xset)
                    self._polling = False
                    if self._terminate:
                        break
                    events = {}
                    for fid in rlist:
                        events[fid] = _AsyncPoller._Read
                    for fid in wlist:
                        events[fid] = events.get(fid, 0) | _AsyncPoller._Write
                    for fid in xlist:
                        events[fid] = events.get(fid, 0) | _AsyncPoller._Error

                    self._lock.acquire()
                    events = [(self._fds.get(fid, None), event)
                              for (fid, event) in events.items()]
                    iocp_notify = False
                    for fd, event in events:
                        if fd is None:
                            logger.debug('invalid fd')
                            continue
                        if event & _AsyncPoller._Read:
                            if fd._read_fn:
                                if fd != self.cmd_rsock:
                                    iocp_notify = True
                                fd._read_fn()
                            else:
                                logger.debug('fd %s is not registered for reading!', fd._fileno)
                        if event & _AsyncPoller._Write:
                            if fd._write_fn:
                                iocp_notify = True
                                fd._write_fn()
                            else:
                                logger.debug('fd %s is not registered for writing!', fd._fileno)
                        if event & _AsyncPoller._Error:
                            if fd._read_task:
                                fd._read_task.throw(socket.error(_AsyncPoller._Error))
                            if fd._write_task:
                                fd._write_task.throw(socket.error(_AsyncPoller._Error))
                    self._lock.release()
                    if iocp_notify:
                        self.iocp_notifier.interrupt()

                self.rset = set()
                self.wset = set()
                self.xset = set()
                self.cmd_rsock.close()
                self.cmd_wsock.close()
                self.cmd_rsock = self.cmd_wsock = None

            def terminate(self):
                if not self._terminate:
                    self._terminate = True
                    self.cmd_wsock.send(b'x')
                    self.poll_thread.join(0.2)

            @staticmethod
            def _socketpair():
                if hasattr(socket, 'socketpair'):
                    return socket.socketpair()
                srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                srv_sock.bind(('127.0.0.1', 0))
                srv_sock.listen(1)
                write_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    write_sock.setblocking(False)
                    try:
                        write_sock.connect(srv_sock.getsockname()[:2])
                    except socket.error as e:
                        if e.args[0] in (EINPROGRESS, EWOULDBLOCK):
                            pass
                        else:
                            raise
                    write_sock.setblocking(True)
                    read_sock = srv_sock.accept()[0]
                except Exception:
                    write_sock.close()
                    raise
                finally:
                    srv_sock.close()
                return (read_sock, write_sock)

        class _AsyncNotifier(object):
            """Internal use only.
            """

            _Block = win32event.INFINITE

            def __init__(self):
                self._poller_name = 'IOCP'
                self.iocp = win32file.CreateIoCompletionPort(win32file.INVALID_HANDLE_VALUE,
                                                             None, 0, 0)
                self._timeouts = []
                self.async_poller = _AsyncPoller(self)
                self.cmd_rsock, self.cmd_wsock = _AsyncPoller._socketpair()
                self.cmd_wsock.setblocking(0)
                self.cmd_rsock = AsyncSocket(self.cmd_rsock)
                self.cmd_rsock._notifier = self
                self.cmd_rsock._register()
                self.cmd_rsock_buf = win32file.AllocateReadBuffer(128)
                self.cmd_rsock._read_overlap.object = self.cmd_rsock_recv
                err, n = win32file.WSARecv(self.cmd_rsock._fileno, self.cmd_rsock_buf,
                                           self.cmd_rsock._read_overlap, 0)
                if err and err != winerror.ERROR_IO_PENDING:
                    logger.warning('WSARecv error: %s', err)
                self._lock = threading.RLock()
                self._polling = False

            def cmd_rsock_recv(self, err, n):
                if n == 0:
                    err = winerror.ERROR_CONNECTION_INVALID
                if err:
                    logger.warning('iocp cmd recv error: %s', err)
                err, n = win32file.WSARecv(self.cmd_rsock._fileno, self.cmd_rsock_buf,
                                           self.cmd_rsock._read_overlap, 0)
                if err and err != winerror.ERROR_IO_PENDING:
                    logger.warning('WSARecv error: %s', err)

            def interrupt(self, timeout=None):
                self.cmd_wsock.send(b'i')

            def register(self, handle, event=0):
                win32file.CreateIoCompletionPort(handle, self.iocp, 1, 0)

            def unregister(self, handle):
                pass

            def modify(self, fd, event):
                pass

            def poll(self, timeout):
                self._lock.acquire()
                if timeout == 0:
                    self.poll_timeout = 0
                elif self._timeouts:
                    self.poll_timeout = self._timeouts[0][0] - _time()
                    if self.poll_timeout < 0.0001:
                        self.poll_timeout = 0
                    elif timeout is not None:
                        self.poll_timeout = min(timeout, self.poll_timeout)
                elif timeout is None:
                    self.poll_timeout = _AsyncNotifier._Block
                else:
                    self.poll_timeout = timeout
                timeout = self.poll_timeout
                self._lock.release()
                if timeout and timeout != _AsyncNotifier._Block:
                    timeout = int(timeout * 1000)
                self._polling = True
                err, n, key, overlap = win32file.GetQueuedCompletionStatus(self.iocp, timeout)
                self._polling = False
                while err != winerror.WAIT_TIMEOUT:
                    if overlap and overlap.object:
                        overlap.object(err, n)
                    elif not self.iocp:
                        if (err == winerror.ERROR_INVALID_HANDLE or
                            err == winerror.ERROR_ABANDONED_WAIT_0):
                            pass
                        else:
                            logger.warning('IOCP handle closed error: %d', err)
                        return
                    else:
                        logger.warning('invalid overlap: %s', err)
                        break
                    err, n, key, overlap = win32file.GetQueuedCompletionStatus(self.iocp, 0)

                self._lock.acquire()
                if self._timeouts:
                    now = _time() + 0.0001
                    while self._timeouts and self._timeouts[0][0] <= now:
                        fd_timeout, fd = self._timeouts.pop(0)
                        if fd._timeout_id == fd_timeout:
                            fd._timeout_id = None
                            fd._timed_out()
                self._lock.release()

            def _add_timeout(self, fd):
                if fd._timeout:
                    self._lock.acquire()
                    fd._timeout_id = _time() + fd._timeout + 0.0001
                    i = bisect_left(self._timeouts, (fd._timeout_id, fd))
                    self._timeouts.insert(i, (fd._timeout_id, fd))
                    if self._polling:
                        self.interrupt()
                    self._lock.release()
                else:
                    fd._timeout_id = None

            def _del_timeout(self, fd):
                if fd._timeout_id:
                    self._lock.acquire()
                    i = bisect_left(self._timeouts, (fd._timeout_id, fd))
                    while i < len(self._timeouts):
                        if self._timeouts[i] == (fd._timeout_id, fd):
                            del self._timeouts[i]
                            fd._timeout_id = None
                            break
                        if fd._timeout_id != self._timeouts[i][0]:
                            logger.warning('fd %s with %s is not found',
                                           fd._fileno, fd._timeout_id)
                            break
                        i += 1
                    if self._polling:
                        self.interrupt()
                    self._lock.release()

            def terminate(self):
                if self.iocp:
                    self.async_poller.terminate()
                    self.cmd_rsock.close()
                    self.cmd_wsock.close()
                    self.cmd_rsock_buf = None
                    iocp, self.iocp = self.iocp, None
                    win32file.CloseHandle(iocp)
                    self._timeouts = []
                    self.cmd_rsock = self.cmd_wsock = None

        class AsyncSocket(_AsyncSocket):
            """AsyncSocket with I/O Completion Ports (under Windows). See
            _AsyncSocket above for more details.  UDP traffic is handled by
            _AsyncPoller.
            """

            __slots__ = _AsyncSocket.__slots__ + ('_read_overlap', '_write_overlap')

            def __init__(self, *args, **kwargs):
                self._read_overlap = None
                self._write_overlap = None
                _AsyncSocket.__init__(self, *args, **kwargs)

            def _register(self):
                if not self._blocking:
                    if self._rsock.type & socket.SOCK_STREAM:
                        self._read_overlap = pywintypes.OVERLAPPED()
                        self._write_overlap = pywintypes.OVERLAPPED()
                        self._notifier.register(self._fileno)
                    else:
                        self._notifier = self._notifier.async_poller
                else:
                    _AsyncSocket._register(self)

            def _unregister(self):
                if self._notifier:
                    self._notifier.unregister(self)
                    if self._rsock.type & socket.SOCK_STREAM:
                        if ((self._read_overlap and self._read_overlap.object) or
                           (self._write_overlap and self._write_overlap.object)):
                            def _cleanup_(rc, n):
                                self._read_overlap.object = self._write_overlap.object = None
                                self._read_result = self._write_result = None
                                self._read_task = self._write_task = None
                                self._read_overlap = self._write_overlap = None
                                self._notifier = None
                                # if rc and rc != winerror.ERROR_OPERATION_ABORTED:
                                #     logger.warning('CancelIo failed?: %x', rc)
                            if self._read_overlap and self._read_overlap.object:
                                self._read_overlap.object = _cleanup_
                            if self._write_overlap and self._write_overlap.object:
                                self._read_overlap.object = _cleanup_
                            rc = win32file.CancelIo(self._fileno)
                            if rc:
                                logger.warning('CancelIo request failed: %d', rc)
                        else:
                            self._read_overlap = self._write_overlap = None
                            self._read_result = self._write_result = None
                            self._read_task = self._write_task = None
                            self._notifier = None
                    else:
                        self._notifier = None

            def _timed_out(self):
                if self._rsock and self._rsock.type & socket.SOCK_STREAM:
                    if self._read_overlap or self._write_overlap:
                        win32file.CancelIo(self._fileno)
                if self._read_task:
                    if self._rsock and self._rsock.type & socket.SOCK_DGRAM:
                        self._notifier.clear(self, _AsyncPoller._Read)
                        self._read_fn = None
                    self._read_task.throw(socket.timeout('timed out'))
                    self._read_result = self._read_task = None
                if self._write_task:
                    if self._rsock and self._rsock.type & socket.SOCK_DGRAM:
                        self._notifier.clear(self, _AsyncPoller._Write)
                        self._write_fn = None
                    self._write_task.throw(socket.timeout('timed out'))
                    self._write_result = self._write_task = None

            def setblocking(self, blocking):
                _AsyncSocket.setblocking(self, blocking)
                if not self._blocking and self._rsock.type & socket.SOCK_STREAM:
                    self.recv = self._iocp_recv
                    self.send = self._iocp_send
                    self.recvall = self._iocp_recvall
                    self.sendall = self._iocp_sendall
                    self.connect = self._iocp_connect
                    self.accept = self._iocp_accept

            def _iocp_recv(self, bufsize, *args):
                """Internal use only; use 'recv' with 'yield' instead.
                """
                def _recv(err, n):
                    if self._timeout and self._notifier:
                        self._notifier._del_timeout(self)
                    if err or n == 0:
                        self._read_overlap.object = self._read_result = None
                        if not err:
                            err = winerror.ERROR_CONNECTION_INVALID
                        if self._read_task:
                            if (err == winerror.ERROR_CONNECTION_INVALID or
                                err == winerror.ERROR_OPERATION_ABORTED):
                                self._read_task._proceed_(b'')
                            else:
                                self._read_task.throw(socket.error(err))
                    else:
                        buf = self._read_result[:n].tobytes()
                        self._read_overlap.object = self._read_result = None
                        self._read_task._proceed_(buf)

                if not self._scheduler:
                    self._scheduler = Pycos.scheduler()
                    self._notifier = self._scheduler._notifier
                    self._register()
                if self._timeout:
                    self._notifier._add_timeout(self)
                self._read_overlap.object = _recv
                self._read_result = win32file.AllocateReadBuffer(bufsize)
                self._read_task = Pycos.cur_task(self._scheduler)
                self._read_task._await_()
                err, n = win32file.WSARecv(self._fileno, self._read_result, self._read_overlap, 0)
                if err != winerror.ERROR_IO_PENDING and err:
                    self._read_overlap.object(err, n)

            def _iocp_send(self, buf, *args):
                """Internal use only; use 'send' with 'yield' instead.
                """
                def _send(err, n):
                    if self._timeout and self._notifier:
                        self._notifier._del_timeout(self)
                    if err or n == 0:
                        self._write_overlap.object = self._write_result = None
                        if not err:
                            err = winerror.ERROR_CONNECTION_INVALID
                        if self._write_task:
                            if (err == winerror.ERROR_CONNECTION_INVALID or
                                err == winerror.ERROR_OPERATION_ABORTED):
                                self._write_task._proceed_(0)
                            else:
                                self._write_task.throw(socket.error(err))
                    else:
                        self._write_overlap.object = None
                        self._write_task._proceed_(n)

                if not self._scheduler:
                    self._scheduler = Pycos.scheduler()
                    self._notifier = self._scheduler._notifier
                    self._register()
                if self._timeout:
                    self._notifier._add_timeout(self)
                self._write_overlap.object = _send
                self._write_task = Pycos.cur_task(self._scheduler)
                self._write_task._await_()
                err, n = win32file.WSASend(self._fileno, buf, self._write_overlap, 0)
                if err != winerror.ERROR_IO_PENDING and err:
                    self._write_overlap.object(err, n)

            def _iocp_recvall(self, bufsize, *args):
                """Internal use only; use 'recvall' with 'yield' instead.
                """
                self._read_result = win32file.AllocateReadBuffer(bufsize)
                # buffer is memoryview object
                view = self._read_result

                def _recvall(err, n):
                    nonlocal view
                    if err or n == 0:
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        view.release()
                        self._read_overlap.object = self._read_result = None
                        if not err:
                            err = winerror.ERROR_CONNECTION_INVALID
                        if self._read_task:
                            if (err == winerror.ERROR_CONNECTION_INVALID or
                                err == winerror.ERROR_OPERATION_ABORTED):
                                self._read_task._proceed_(b'')
                            else:
                                self._read_task.throw(socket.error(err))
                    else:
                        view = view[n:]
                        if view:
                            err, n = win32file.WSARecv(self._fileno, view, self._read_overlap, 0)
                            if err != winerror.ERROR_IO_PENDING and err:
                                if self._timeout and self._notifier:
                                    self._notifier._del_timeout(self)
                                view.release()
                                self._read_overlap.object = self._read_result = None
                                self._read_task.throw(socket.error(err))
                        else:
                            buf = self._read_result.tobytes()
                            self._read_result.release()
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._read_overlap.object = self._read_result = None
                            self._read_task._proceed_(buf)

                if not self._scheduler:
                    self._scheduler = Pycos.scheduler()
                    self._notifier = self._scheduler._notifier
                    self._register()
                if self._timeout:
                    self._notifier._add_timeout(self)
                self._read_overlap.object = _recvall
                self._read_task = Pycos.cur_task(self._scheduler)
                self._read_task._await_()
                err, n = win32file.WSARecv(self._fileno, view, self._read_overlap, 0)
                if err != winerror.ERROR_IO_PENDING and err:
                    self._read_overlap.object(err, n)

            def _iocp_sendall(self, data):
                """Internal use only; use 'sendall' with 'yield' instead.
                """
                def _sendall(err, n):
                    if err or n == 0:
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        self._write_overlap.object = self._write_result = None
                        if not err:
                            err = winerror.ERROR_CONNECTION_INVALID
                        if self._write_task:
                            self._write_task.throw(socket.error(err))
                    else:
                        self._write_result = self._write_result[n:]
                        if len(self._write_result) == 0:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._write_result.release()
                            self._write_overlap.object = self._write_result = None
                            self._write_task._proceed_(0)
                        else:
                            err, n = win32file.WSASend(self._fileno, self._write_result,
                                                       self._write_overlap, 0)
                            if err != winerror.ERROR_IO_PENDING and err:
                                if self._timeout and self._notifier:
                                    self._notifier._del_timeout(self)
                                self._write_overlap.object = self._write_result = None
                                self._write_task.throw(socket.error(err))

                if not self._scheduler:
                    self._scheduler = Pycos.scheduler()
                    self._notifier = self._scheduler._notifier
                    self._register()
                if self._timeout:
                    self._notifier._add_timeout(self)
                self._write_overlap.object = _sendall
                self._write_result = memoryview(data)
                self._write_task = Pycos.cur_task(self._scheduler)
                self._write_task._await_()
                err, n = win32file.WSASend(self._fileno, self._write_result, self._write_overlap, 0)
                if err != winerror.ERROR_IO_PENDING and err:
                    self._write_overlap.object(err, n)

            def _iocp_connect(self, host_port):
                """Internal use only; use 'connect' with 'yield' instead.
                """
                def _connect(err, n):
                    if err:
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        self._read_overlap.object = self._read_result = None
                        if err == winerror.ERROR_OPERATION_ABORTED:
                            self._read_task = None
                        elif self._read_task:
                            self._read_task.throw(socket.error(err))
                        return

                    self._rsock.setsockopt(socket.SOL_SOCKET,
                                           win32file.SO_UPDATE_CONNECT_CONTEXT, b'')
                    if not self._certfile:
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        self._read_overlap.object = self._read_result = None
                        self._read_task._proceed_(0)
                        return

                    # SSL connect
                    def _ssl_handshake(err, n):
                        try:
                            self._rsock.do_handshake()
                        except ssl.SSLError as exc:
                            if exc.args[0] == ssl.SSL_ERROR_WANT_READ:
                                self._read_result = win32file.AllocateReadBuffer(0)
                                err, n = win32file.WSARecv(self._fileno, self._read_result,
                                                           self._read_overlap, 0)
                                if err != winerror.ERROR_IO_PENDING and err:
                                    logger.warning('SSL handshake failed (%s)?', err)
                            elif exc.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                                err, n = win32file.WSASend(self._fileno, b'', self._read_overlap, 0)
                                if err != winerror.ERROR_IO_PENDING and err:
                                    logger.warning('SSL handshake failed (%s)?', err)
                            else:
                                if self._timeout and self._notifier:
                                    self._notifier._del_timeout(self)
                                self._read_overlap.object = self._read_result = None
                                task, self._read_task = self._read_task, None
                                self.close()
                                if task:
                                    task.throw(*sys.exc_info())
                        except Exception:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._read_overlap.object = self._read_result = None
                            task, self._read_task = self._read_task, None
                            self.close()
                            if err != winerror.ERROR_OPERATION_ABORTED and task:
                                task.throw(socket.error(err))
                        else:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._read_overlap.object = self._read_result = None
                            self._read_task._proceed_(0)

                    self._rsock = ssl.wrap_socket(self._rsock, ca_certs=self._certfile,
                                                  cert_reqs=ssl.CERT_REQUIRED, server_side=False,
                                                  do_handshake_on_connect=False)
                    self._read_result = win32file.AllocateReadBuffer(0)
                    self._read_overlap.object = _ssl_handshake
                    self._read_overlap.object(None, 0)

                try:
                    if self._rsock.family == socket.AF_INET6:
                        # TODO: is it required to bind to '::'?
                        try:
                            self._rsock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                        except Exception:
                            pass
                        self._rsock.bind(('::', 0))
                    else:
                        self._rsock.bind(('0.0.0.0', 0))
                except socket.error as exc:
                    if exc[0] != EINVAL:
                        raise
                if not self._scheduler:
                    self._scheduler = Pycos.scheduler()
                    self._notifier = self._scheduler._notifier
                    self._register()
                if self._timeout:
                    self._notifier._add_timeout(self)
                self._read_overlap.object = _connect
                self._read_task = Pycos.cur_task(self._scheduler)
                self._read_task._await_()
                err, n = win32file.ConnectEx(self._rsock, host_port, self._read_overlap)
                if err != winerror.ERROR_IO_PENDING and err:
                    self._read_overlap.object(err, n)

            def _iocp_accept(self):
                """Internal use only; use 'accept' with 'yield' instead. Socket
                in returned pair is asynchronous socket (instance of AsyncSocket
                with blocking=False).
                """
                conn = socket.socket(self._rsock.family, self._rsock.type, self._rsock.proto)
                conn = AsyncSocket(conn, keyfile=self._keyfile, certfile=self._certfile,
                                   ssl_version=self._ssl_version)

                def _accept(err, n):
                    if err:
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        self._read_overlap.object = self._read_result = None
                        task, self._read_task = self._read_task, None
                        if err != winerror.ERROR_OPERATION_ABORTED and task:
                            task.throw(socket.error(err))
                        return

                    family, laddr, raddr = win32file.GetAcceptExSockaddrs(conn,
                                                                          self._read_result)
                    # it seems getpeername returns IP address as string, but
                    # GetAcceptExSockaddrs returns bytes, so decode address
                    if family == socket.AF_INET:
                        raddr = (raddr[0].decode('ascii'), raddr[1])
                    # TODO: unpack raddr if family != AF_INET

                    conn._rsock.setsockopt(socket.SOL_SOCKET, win32file.SO_UPDATE_ACCEPT_CONTEXT,
                                           struct.pack('P', self._fileno))

                    if not self._certfile:
                        if self._timeout and self._notifier:
                            self._notifier._del_timeout(self)
                        self._read_overlap.object = self._read_result = None
                        self._read_task._proceed_((conn, raddr))
                        return

                    # accept SSL connection
                    if not self.ssl_server_ctx and hasattr(ssl, 'create_default_context'):
                        self.ssl_server_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                        self.ssl_server_ctx.load_cert_chain(certfile=self._certfile,
                                                            keyfile=self._keyfile)
                    if self.ssl_server_ctx:
                        conn._rsock = self.ssl_server_ctx.wrap_socket(
                            conn._rsock, server_side=True, do_handshake_on_connect=False)
                    else:
                        conn._rsock = ssl.wrap_socket(conn._rsock, certfile=self._certfile,
                                                      keyfile=self._keyfile, server_side=True,
                                                      ssl_version=self._ssl_version,
                                                      do_handshake_on_connect=False)

                    def _ssl_handshake(err, n):
                        try:
                            conn._rsock.do_handshake()
                        except ssl.SSLError as exc:
                            if exc.args[0] == ssl.SSL_ERROR_WANT_READ:
                                self._read_result = win32file.AllocateReadBuffer(0)
                                err, n = win32file.WSARecv(conn._fileno, self._read_result,
                                                           self._read_overlap, 0)
                                if err != winerror.ERROR_IO_PENDING and err:
                                    logger.warning('SSL handshake failed (%s)?', err)
                            elif exc.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                                err, n = win32file.WSASend(conn._fileno, b'', self._read_overlap, 0)
                                if err != winerror.ERROR_IO_PENDING and err:
                                    logger.warning('SSL handshake failed (%s)?', err)
                            else:
                                if self._timeout and self._notifier:
                                    self._notifier._del_timeout(self)
                                self._read_overlap.object = self._read_result = None
                                task, self._read_task = self._read_task, None
                                conn.close()
                                if task:
                                    task.throw(*sys.exc_info())
                        except Exception:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._read_overlap.object = self._read_result = None
                            task, self._read_task = self._read_task, None
                            conn.close()
                            if err != winerror.ERROR_OPERATION_ABORTED and task:
                                task.throw(socket.error(err))
                        else:
                            if self._timeout and self._notifier:
                                self._notifier._del_timeout(self)
                            self._read_overlap.object = self._read_result = None
                            self._read_task._proceed_((conn, raddr))

                    self._read_result = win32file.AllocateReadBuffer(0)
                    self._read_overlap.object = _ssl_handshake
                    self._read_overlap.object(None, 0)

                if not self._scheduler:
                    self._scheduler = Pycos.scheduler()
                    self._notifier = self._scheduler._notifier
                    self._register()
                if self._timeout:
                    self._notifier._add_timeout(self)
                self._read_result = win32file.AllocateReadBuffer(
                    win32file.CalculateSocketEndPointSize(conn._rsock))
                self._read_overlap.object = _accept
                self._read_task = Pycos.cur_task(self._scheduler)
                self._read_task._await_()
                err = win32file.AcceptEx(self._fileno, conn._fileno, self._read_result,
                                         self._read_overlap)
                if err != winerror.ERROR_IO_PENDING and err:
                    self._read_overlap.object(err, 0)


if not hasattr(sys.modules[__name__], '_AsyncNotifier'):
    import os
    try:
        import fcntl
    except ImportError:
        pass

    class _AsyncPoller(object):
        """Internal use only.
        """

        _Read = None
        _Write = None
        _Hangup = None
        _Error = None

        _Block = None

        def __init__(self):
            self.timeout_multiplier = 1

            if hasattr(select, 'epoll'):
                self._poller_name = 'epoll'
                self._poller = select.epoll()
                _AsyncPoller._Read = select.EPOLLIN | select.EPOLLPRI
                _AsyncPoller._Write = select.EPOLLOUT
                _AsyncPoller._Hangup = select.EPOLLHUP
                _AsyncPoller._Error = select.EPOLLERR
                _AsyncPoller._Block = -1
            elif hasattr(select, 'kqueue'):
                self._poller_name = 'kqueue'
                self._poller = _KQueueNotifier()
                # kqueue filter values are negative numbers so using
                # them as flags won't work, so define them as necessary
                _AsyncPoller._Read = 0x01
                _AsyncPoller._Write = 0x02
                _AsyncPoller._Hangup = 0x04
                _AsyncPoller._Error = 0x08
                _AsyncPoller._Block = None
            elif hasattr(select, 'devpoll'):
                self._poller_name = 'devpoll'
                self._poller = select.devpoll()
                _AsyncPoller._Read = select.POLLIN | select.POLLPRI
                _AsyncPoller._Write = select.POLLOUT
                _AsyncPoller._Hangup = select.POLLHUP
                _AsyncPoller._Error = select.POLLERR
                _AsyncPoller._Block = -1
                self.timeout_multiplier = 1000
            elif hasattr(select, 'poll'):
                self._poller_name = 'poll'
                self._poller = select.poll()
                _AsyncPoller._Read = select.POLLIN | select.POLLPRI
                _AsyncPoller._Write = select.POLLOUT
                _AsyncPoller._Hangup = select.POLLHUP
                _AsyncPoller._Error = select.POLLERR
                _AsyncPoller._Block = -1
                self.timeout_multiplier = 1000
            else:
                self._poller_name = 'select'
                self._poller = _SelectNotifier()
                _AsyncPoller._Read = 0x01
                _AsyncPoller._Write = 0x02
                _AsyncPoller._Hangup = 0x04
                _AsyncPoller._Error = 0x08
                _AsyncPoller._Block = None

            self._fds = {}
            self._timeouts = []
            self.cmd_read, self.cmd_write = _AsyncPoller._cmd_read_write_fds(self)
            if hasattr(self.cmd_write, 'getsockname'):
                self.cmd_read = AsyncSocket(self.cmd_read)
                self.cmd_read._read_fn = lambda: self.cmd_read._rsock.recv(128)
                self.cmd_read._notifier = self
                self.interrupt = lambda: self.cmd_write.send(b'I')
            else:
                self.interrupt = lambda: os.write(self.cmd_write._fileno, b'I')
            self.add(self.cmd_read, _AsyncPoller._Read)

        def poll(self, timeout):
            if timeout == 0:
                poll_timeout = timeout
            elif self._timeouts:
                poll_timeout = self._timeouts[0][0] - _time()
                if timeout is not None:
                    poll_timeout = min(timeout, poll_timeout)
                if poll_timeout < 0.0001:
                    poll_timeout = 0
                poll_timeout *= self.timeout_multiplier
            elif timeout is None:
                poll_timeout = _AsyncPoller._Block
            else:
                poll_timeout = timeout * self.timeout_multiplier

            try:
                events = self._poller.poll(poll_timeout)
            except Exception:
                logger.debug(traceback.format_exc())
                # prevent tight loops
                time.sleep(5)
                return

            try:
                for fileno, event in events:
                    fd = self._fds.get(fileno, None)
                    if not fd:
                        if not (event & _AsyncPoller._Hangup):
                            logger.debug('invalid fd %s for event %s', fileno, event)
                        continue
                    if event & _AsyncPoller._Read:
                        if fd._read_fn:
                            fd._read_fn()
                        else:
                            logger.debug('fd %s is not registered for reading!', fd._fileno)
                            self.unregister(fd)
                    elif event & _AsyncPoller._Write:
                        if fd._write_fn:
                            fd._write_fn()
                        else:
                            logger.debug('fd %s is not registered for writing!', fd._fileno)
                            self.unregister(fd)
                    elif event & _AsyncPoller._Hangup:
                        fd._eof()
                    elif event & _AsyncPoller._Error:
                        logger.warning('error on fd %s', fd._fileno)
                        self.unregister(fd)
            except Exception:
                logger.debug(traceback.format_exc())

            if self._timeouts:
                now = _time() + 0.0001
                while self._timeouts and self._timeouts[0][0] <= now:
                    fd_timeout, fd = self._timeouts.pop(0)
                    if fd._timeout_id == fd_timeout:
                        fd._timeout_id = None
                        fd._timed_out()

        def terminate(self):
            if hasattr(self.cmd_write, 'getsockname'):
                self.cmd_write.close()
                self._fds.pop(self.cmd_read._fileno, None)
            self.cmd_read.close()
            for fd in list(self._fds.values()):
                setblocking = getattr(fd, 'setblocking', None)
                if setblocking and fd._rsock:
                    setblocking(True)
                else:
                    try:
                        self._poller.unregister(fd._fileno)
                    except Exception:
                        logger.warning('unregister of %s failed with %s',
                                       fd._fileno, traceback.format_exc())
                fd._notifier = None
            self._fds.clear()
            self._timeouts = []
            if hasattr(self._poller, 'close'):
                self._poller.close()
            if hasattr(self._poller, 'terminate'):
                self._poller.terminate()
            self._poller = None
            self.cmd_read = self.cmd_write = None

        def _add_timeout(self, fd):
            if fd._timeout:
                fd._timeout_id = _time() + fd._timeout + 0.0001
                i = bisect_left(self._timeouts, (fd._timeout_id, fd))
                self._timeouts.insert(i, (fd._timeout_id, fd))
            else:
                fd._timeout_id = None

        def _del_timeout(self, fd):
            if fd._timeout_id:
                i = bisect_left(self._timeouts, (fd._timeout_id, fd))
                # in case of identical timeouts (unlikely?), search for
                # correct index where fd is
                while i < len(self._timeouts):
                    if self._timeouts[i] == (fd._timeout_id, fd):
                        del self._timeouts[i]
                        fd._timeout_id = None
                        break
                    if fd._timeout_id != self._timeouts[i][0]:
                        logger.warning('fd %s with %s is not found', fd._fileno, fd._timeout_id)
                        break
                    i += 1

        def unregister(self, fd):
            if self._fds.pop(fd._fileno, None) is None:
                logger.debug('fd %s is not registered', fd._fileno)
                return
            fd._event = None
            self._poller.unregister(fd._fileno)
            self._del_timeout(fd)

        def add(self, fd, event):
            if fd._event is None:
                self._fds[fd._fileno] = fd
                fd._event = event
                self._poller.register(fd._fileno, event)
            else:
                fd._event |= event
                self._poller.modify(fd._fileno, event)
            if fd._timeout:
                self._add_timeout(fd)
            else:
                fd._timeout_id = None

        def clear(self, fd, event=0):
            cur_event = fd._event
            if cur_event:
                if event:
                    cur_event &= ~event
                else:
                    cur_event = 0
                fd._event = cur_event
                self._poller.modify(fd._fileno, cur_event)
                if not cur_event:
                    self._del_timeout(fd)

        @staticmethod
        def _cmd_read_write_fds(notifier):
            if sys.modules.get('fcntl'):
                class PipeFD(object):

                    def __init__(self, fileno):
                        self._fileno = fileno
                        self._timeout = None
                        self._timeout_id = None
                        self._notifier = notifier
                        self._event = None
                        flags = fcntl.fcntl(self._fileno, fcntl.F_GETFL)
                        fcntl.fcntl(self._fileno, fcntl.F_SETFL, flags | os.O_NONBLOCK)

                    def _read_fn(self):
                        os.read(self._fileno, 128)

                    def close(self):
                        if self._notifier:
                            self._notifier.unregister(self)
                            os.close(self._fileno)
                            self._notifier = None

                    def _eof(self):
                        self._read_fn()

                pipein, pipeout = os.pipe()
                return (PipeFD(pipein), PipeFD(pipeout))
            elif hasattr(socket, 'socketpair'):
                return socket.socketpair()
            else:
                srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                srv_sock.bind(('127.0.0.1', 0))
                srv_sock.listen(1)
                write_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    write_sock.setblocking(False)
                    try:
                        write_sock.connect(srv_sock.getsockname()[:2])
                    except socket.error as e:
                        if e.args[0] in (EINPROGRESS, EWOULDBLOCK):
                            pass
                        else:
                            raise
                    write_sock.setblocking(True)
                    read_sock = srv_sock.accept()[0]
                except Exception:
                    write_sock.close()
                    raise
                finally:
                    srv_sock.close()
                return (read_sock, write_sock)

    class _KQueueNotifier(object):
        """Internal use only.
        """

        def __init__(self):
            if not hasattr(self, 'poller'):
                self.poller = select.kqueue()

        def register(self, fid, event):
            flags = select.KQ_EV_ADD
            if event & _AsyncPoller._Read:
                flags |= select.KQ_EV_ENABLE
            else:
                flags |= select.KQ_EV_DISABLE
            self.poller.control([select.kevent(fid, filter=select.KQ_FILTER_READ, flags=flags)], 0)
            flags = select.KQ_EV_ADD
            if event & _AsyncPoller._Write:
                flags |= select.KQ_EV_ENABLE
            else:
                flags |= select.KQ_EV_DISABLE
            self.poller.control([select.kevent(fid, filter=select.KQ_FILTER_WRITE, flags=flags)], 0)

        def unregister(self, fid):
            flags = select.KQ_EV_DELETE
            self.poller.control([select.kevent(fid, filter=select.KQ_FILTER_READ, flags=flags)], 0)
            self.poller.control([select.kevent(fid, filter=select.KQ_FILTER_WRITE, flags=flags)], 0)

        def modify(self, fid, event):
            if event & _AsyncPoller._Read:
                flags = select.KQ_EV_ENABLE
            else:
                flags = select.KQ_EV_DISABLE
            self.poller.control([select.kevent(fid, filter=select.KQ_FILTER_READ, flags=flags)], 0)
            if event & _AsyncPoller._Write:
                flags = select.KQ_EV_ENABLE
            else:
                flags = select.KQ_EV_DISABLE
            self.poller.control([select.kevent(fid, filter=select.KQ_FILTER_WRITE, flags=flags)], 0)

        def poll(self, timeout):
            kevents = self.poller.control(None, 500, timeout)
            events = [(kevent.ident,
                       _AsyncPoller._Read if kevent.filter == select.KQ_FILTER_READ else
                       _AsyncPoller._Write if kevent.filter == select.KQ_FILTER_WRITE else
                       _AsyncPoller._Hangup if kevent.flags == select.KQ_EV_EOF else
                       _AsyncPoller._Error if kevent.flags == select.KQ_EV_ERROR else 0)
                      for kevent in kevents]
            return events

    class _SelectNotifier(object):
        """Internal use only.
        """

        def __init__(self):
            if not hasattr(self, 'poller'):
                self.poller = select.select
                self.rset = set()
                self.wset = set()
                self.xset = set()

        def register(self, fid, event):
            if event:
                if event & _AsyncPoller._Read:
                    self.rset.add(fid)
                if event & _AsyncPoller._Write:
                    self.wset.add(fid)
                if event & _AsyncPoller._Error:
                    self.xset.add(fid)

        def unregister(self, fid):
            self.rset.discard(fid)
            self.wset.discard(fid)
            self.xset.discard(fid)

        def modify(self, fid, event):
            self.unregister(fid)
            self.register(fid, event)

        def poll(self, timeout):
            rlist, wlist, xlist = self.poller(self.rset, self.wset, self.xset, timeout)
            events = {}
            for fid in rlist:
                events[fid] = _AsyncPoller._Read
            for fid in wlist:
                events[fid] = events.get(fid, 0) | _AsyncPoller._Write
            for fid in xlist:
                events[fid] = events.get(fid, 0) | _AsyncPoller._Error

            return events.items()

        def terminate(self):
            self.rset = set()
            self.wset = set()
            self.xset = set()

    AsyncSocket = _AsyncSocket
    _AsyncNotifier = _AsyncPoller


class Lock(object):
    """'Lock' primitive for tasks.
    """
    def __init__(self):
        self._owner = None
        self._waitlist = []
        self._scheduler = None

    def acquire(self, blocking=True, timeout=-1):
        """Must be used with 'yield' as 'yield lock.acquire()'.
        """
        if not blocking and self._owner is not None:
            raise StopIteration(False)
        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
        task = Pycos.cur_task(self._scheduler)
        if timeout < 0:
            timeout = None
        while self._owner is not None:
            if timeout is not None:
                if timeout <= 0:
                    raise StopIteration(False)
                start = _time()
            self._waitlist.append(task)
            if (yield task._await_(timeout)) is None:
                try:
                    self._waitlist.remove(task)
                except ValueError:
                    pass
            if timeout is not None:
                timeout -= (_time() - start)
        self._owner = task
        raise StopIteration(True)

    def release(self):
        """May be used with 'yield'.
        """
        task = Pycos.cur_task(self._scheduler)
        if self._owner != task:
            raise RuntimeError('"%s"/%s: invalid lock release - not locked' % (task._name, task._id))
        self._owner = None
        if self._waitlist:
            wake = self._waitlist.pop(0)
            wake._proceed_(True)


class RLock(object):
    """'RLock' primitive for tasks.
    """
    def __init__(self):
        self._owner = None
        self._depth = 0
        self._waitlist = []
        self._scheduler = None

    def acquire(self, blocking=True, timeout=-1):
        """Must be used with 'yield' as 'yield rlock.acquire()'.
        """
        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
        task = Pycos.cur_task(self._scheduler)
        if self._owner == task:
            assert self._depth > 0
            self._depth += 1
            raise StopIteration(True)
        if not blocking and self._owner is not None:
            raise StopIteration(False)
        if timeout < 0:
            timeout = None
        while self._owner is not None:
            if timeout is not None:
                if timeout <= 0:
                    raise StopIteration(False)
                start = _time()
            self._waitlist.append(task)
            if (yield task._await_(timeout)) is None:
                try:
                    self._waitlist.remove(task)
                except ValueError:
                    pass
            if timeout is not None:
                timeout -= (_time() - start)
        assert self._depth == 0
        self._owner = task
        self._depth = 1
        raise StopIteration(True)

    def release(self):
        """May be used with 'yield'.
        """
        task = Pycos.cur_task(self._scheduler)
        if self._owner != task:
            raise RuntimeError('"%s"/%s: invalid lock release - owned by "%s"/%s' %
                               (task._name, task._id, self._owner._name, self._owner._id))
        self._depth -= 1
        if self._depth == 0:
            self._owner = None
            if self._waitlist:
                wake = self._waitlist.pop(0)
                wake._proceed_(True)


class Condition(object):
    """'Condition' primitive for tasks.
    """
    def __init__(self):
        """TODO: support lock argument?
        """
        self._owner = None
        self._depth = 0
        self._waitlist = []
        self._notifylist = []
        self._scheduler = None

    def acquire(self, blocking=True, timeout=-1):
        """Must be used with 'yield' as 'yield cv.acquire()'.
        """
        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
        task = Pycos.cur_task(self._scheduler)
        if self._owner == task:
            self._depth += 1
            raise StopIteration(True)
        if not blocking and self._owner is not None:
            raise StopIteration(False)
        if timeout < 0:
            timeout = None
        while self._owner is not None:
            if timeout is not None:
                if timeout <= 0:
                    raise StopIteration(False)
                start = _time()
            self._waitlist.append(task)
            if (yield task._await_(timeout)) is None:
                try:
                    self._waitlist.remove(task)
                except ValueError:
                    pass
            if timeout is not None:
                timeout -= (_time() - start)
        assert self._depth == 0
        self._owner = task
        self._depth = 1
        raise StopIteration(True)

    def release(self):
        """May be used with 'yield'.
        """
        task = Pycos.cur_task(self._scheduler)
        if self._owner != task:
            raise RuntimeError('"%s"/%s: invalid lock release - owned by "%s"/%s' %
                               (task._name, task._id, self._owner._name, self._owner._id))
        self._depth -= 1
        if self._depth == 0:
            self._owner = None
            if self._waitlist:
                wake = self._waitlist.pop(0)
                wake._proceed_(True)

    def notify(self, n=1):
        """May not be used with 'yield'.
        """
        while self._notifylist and n:
            wake = self._notifylist.pop(0)
            wake._proceed_(True)
            n -= 1

    def notify_all(self):
        self.notify(len(self._notifylist))

    notifyAll = notify_all

    def wait(self, timeout=None):
        """Must be used with 'yield' as 'yield cv.wait()'.
        """
        task = Pycos.cur_task(self._scheduler)
        if self._owner != task:
            raise RuntimeError('"%s"/%s: invalid lock release - owned by "%s"/%s' %
                               (task._name, task._id, self._owner._name, self._owner._id))
        assert self._depth > 0
        depth = self._depth
        self._depth = 0
        self._owner = None
        if self._waitlist:
            wake = self._waitlist.pop(0)
            wake._proceed_(True)
        self._notifylist.append(task)
        start = _time()
        if (yield task._await_(timeout)) is None:
            try:
                self._notifylist.remove(task)
            except ValueError:
                pass
            raise StopIteration(False)
        while self._owner is not None:
            self._waitlist.insert(0, task)
            if timeout is not None:
                timeout -= (_time() - start)
                if timeout <= 0:
                    raise StopIteration(False)
                start = _time()
            if (yield task._await_(timeout)) is None:
                try:
                    self._waitlist.remove(task)
                except ValueError:
                    pass
                raise StopIteration(False)
        assert self._depth == 0
        self._owner = task
        self._depth = depth
        raise StopIteration(True)


class Event(object):
    """'Event' primitive for tasks.
    """
    def __init__(self):
        self._flag = False
        self._waitlist = []
        self._scheduler = None

    def set(self):
        """May be used with 'yield'.
        """
        self._flag = True
        for task in self._waitlist:
            task._proceed_(True)
        self._waitlist = []

    def is_set(self):
        """No need to use with 'yield'.
        """
        return self._flag

    isSet = is_set

    def clear(self):
        """No need to use with 'yield'.
        """
        self._flag = False

    def wait(self, timeout=None):
        """Must be used with 'yield' as 'yield event.wait()' .
        """
        if self._flag:
            raise StopIteration(True)
        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
        task = Pycos.cur_task(self._scheduler)
        if timeout is not None:
            if timeout <= 0:
                raise StopIteration(False)
        self._waitlist.append(task)
        if (yield task._await_(timeout)) is None:
            try:
                self._waitlist.remove(task)
            except ValueError:
                pass
            raise StopIteration(False)
        else:
            raise StopIteration(True)


class Semaphore(object):
    """'Semaphore' primitive for tasks.
    """
    def __init__(self, value=1):
        assert value >= 1
        self._waitlist = []
        self._counter = value
        self._scheduler = None

    def acquire(self, blocking=True):
        """Must be used with 'yield' as 'yield sem.acquire()'.
        """
        if blocking:
            if not self._scheduler:
                self._scheduler = Pycos.scheduler()
            task = Pycos.cur_task(self._scheduler)
            while self._counter == 0:
                self._waitlist.append(task)
                yield task._await_()
        elif self._counter == 0:
            raise StopIteration(False)
        self._counter -= 1
        raise StopIteration(True)

    def release(self):
        """May be used with 'yield'.
        """
        self._counter += 1
        assert self._counter > 0
        if self._waitlist:
            wake = self._waitlist.pop(0)
            wake._proceed_()


class HotSwapException(Exception):
    """This exception is used to indicate hot-swap request and response.

    If 'exc' is the exception, then len(exc.args) == 1 and exc.args[0] is the
    new generator method.
    """
    pass


class MonitorStatus(object):
    """This is used to indicate that a task being monitored has finished or terminated.

    'info' is information about status. It status is about a task, then 'info' is that task
    instance. If status is about an exception, then 'info' is contextual information (as a string)
    of that exception, such as location where exception occurred.

    'type' is type of 'value', or type of exception this status is sent.

    'value' is value, such as exit value of a task or exception trace in case of failure. In case
    of exception, 'value' may be a string (e.g., trace) but 'type' would be type of exception.

    """
    def __init__(self, info, type, value=None):
        self.info = info
        self.type = type
        self.value = value


class _Peer(object):
    """Internal use only.
    """
    pass


class _NetRequest(object):
    """Internal use only.
    """
    pass


class SysTask(object):
    """Internal use only.
    """
    pass


class Task(object):
    """Creates task with the given generator function and schedules that task to
    be executed with Pycos. If the function definition has 'task' keyword
    argument set to (default value) None, that argument will be set to the task
    created.
    """

    __slots__ = ('_generator', '_name', '_id', '_state', '_value', '_exceptions', '_callers',
                 '_timeout', '_daemon', '_complete', '_msgs', '_monitors', '_swap_generator',
                 '_hot_swappable', '_location', '_scheduler', '_rid')

    _pycos = None
    _sign = None

    def __init__(self, *args, **kwargs):
        self._generator = Task.__get_generator(self, *args, **kwargs)
        self._name = self._generator.__name__
        self._id = id(self)
        self._state = None
        self._value = None
        self._exceptions = []
        self._callers = []
        self._timeout = None
        self._daemon = False
        self._complete = None
        self._msgs = collections.deque()
        self._monitors = set()
        self._swap_generator = None
        self._hot_swappable = False
        if not Task._pycos:
            Pycos.instance()
        self._scheduler = self.__class__._pycos
        self._location = None
        self._rid = None
        self._scheduler._add(self)

    @property
    def location(self):
        """Get Location instance where this task is running.

        Can also be used on remotely running tasks.
        """
        if self._location:
            return copy.copy(self._location)
        return self._scheduler.location

    @property
    def name(self):
        """Get name of task.

        Can also be used on remotely running tasks.
        """
        return self._name

    @classmethod
    def scheduler(cls):
        return cls._pycos

    @staticmethod
    def locate(name, location=None, timeout=None):
        """Must be used with 'yield' as 'rtask = yield Task.locate("name")'.

        Returns Task instance to task at remote peers so it can be used to
        exchange messages, monitor etc. (those methods explicitly marked as
        callable on remote tasks).
        """
        if not Task._pycos:
            Pycos.instance()
        raise StopIteration((yield Task._locate(name, location=location, timeout=timeout)))

    @staticmethod
    def _locate(name, location, timeout):
        """Internal use only.
        """
        if not location or location in Task._pycos._locations:
            if name[0] == '^':
                SysTask._pycos._lock.acquire()
                rtask = SysTask._pycos._rtasks.get(name, None)
                SysTask._pycos._lock.release()
            else:
                rtask = Task._pycos._rtasks.get(name, None)
            if rtask or location in Task._pycos._locations:
                raise StopIteration(rtask)
        req = _NetRequest('locate_task', kwargs={'name': name}, dst=location, timeout=timeout)
        rtask = yield _Peer.async_request(req)
        raise StopIteration(rtask)

    def register(self, name=None):
        """Register this task so tasks running on a remote (peer) pycos can
        locate it (with 'locate') so they can exchange messages, monitored etc.
        """
        if self._location:
            logger.warning('"register" is not allowed for remote tasks')
            return -1
        if name:
            if not isinstance(name, str) or name[0] == '^':
                logger.warning('Invalid name "%s" to register task ignored', name)
                return -1
            if self._scheduler != Task._pycos:
                name = '^' + name
        else:
            name = self._name
        return self._scheduler._register_task(self, name)

    def unregister(self, name=None):
        """Unregister this task (see 'register' above).
        """
        if self._location:
            return -1
        if name:
            if self._scheduler != Task._pycos:
                name = '^' + name
        else:
            name = self._name
        return self._scheduler._unregister_task(self, name)

    def set_daemon(self, flag=True):
        """Mark task is a daemon process or not.

        Pycos scheduler waits for all non-daemon tasks to terminate before it
        terminates itself.
        """
        return self._scheduler._set_daemon(self, bool(flag))

    def suspend(self, timeout=None, alarm_value=None):
        """Must be used with 'yield' as 'yield task.suspend()'.

        If timeout is a (floating point) number, this task is suspended for that
        many seconds (or fractions of second), unless resumed by another task,
        in which case the value it is resumed with is the return value of this
        suspend.

        If suspend times out (no other task resumes it), Pycos resumes it with
        the value 'alarm_value'.
        """
        return self._scheduler._suspend(self, timeout, alarm_value, Pycos._Suspended)

    sleep = suspend

    def resume(self, update=None):
        """May be used with 'yield'. Resume/wakeup this task and send 'update'
        to it.

        The resuming task gets 'update' for the 'yield' that caused it to
        suspend. If task is currently not suspended/sleeping, resume is ignored.
        """
        return self._scheduler._resume(self, update, Pycos._Suspended)

    wakeup = resume

    def send(self, message):
        """May be used with 'yield'. Sends 'message' to task.

        If task is currently waiting with 'receive', it is resumed with
        'message'. Otherwise, 'message' is queued so that next receive call will
        return message.

        Can also be used on remotely running tasks.
        """
        if self._location:
            kwargs = {'message': message, 'name': self._name, 'task': self._id, 'rid': self._rid}
            request = _NetRequest('send', kwargs=kwargs, dst=self._location, timeout=MsgTimeout)
            # request is queued for asynchronous processing
            if _Peer.send_req(request) != 0:
                logger.warning('remote task at %s may not be valid', self._location)
                return -1
            else:
                return 0
        else:
            return self._scheduler._resume(self, message, Pycos._AwaitMsg_)

    def deliver(self, message, timeout=None):
        """Must be used with 'yield' as 'yield task.deliver(message)'.

        Can also be used on remotely running tasks.

        Return value indicates status of delivering the message: If it is 1,
        then message has been delivered, if it is 0, it couldn't be delivered
        before timeout, and if it is < 0, then the (remote) task is not valid.
        """
        if self._location:
            kwargs = {'message': message, 'name': self._name, 'task': self._id, 'rid': self._rid}
            request = _NetRequest('deliver', kwargs=kwargs, dst=self._location, timeout=timeout)
            request.reply = -1
            reply = yield _Peer.sync_reply(request, alarm_value=0)
            # if reply < 0:
            #     logger.warning('remote task at %s may not be valid', self._location)
        else:
            reply = self._scheduler._resume(self, message, Pycos._AwaitMsg_)
            if reply == 0:
                reply = 1
        raise StopIteration(reply)

    def receive(self, timeout=None, alarm_value=None):
        """Must be used with 'yield' as 'message = yield task.receive()'.
        Gets/waits for message.

        Gets earliest queued message if available (that has been sent earlier
        with 'send'). Otherwise, suspends until 'timeout'. If timeout happens,
        task receives alarm_value.
        """
        return self._scheduler._suspend(self, timeout, alarm_value, Pycos._AwaitMsg_)

    recv = receive

    def throw(self, *args):
        """Throw exception in task. This method must be called from task only.
        """
        if len(args) == 0:
            logger.warning('throw: invalid argument(s)')
            return -1
        if len(args) == 1:
            if isinstance(args[0], tuple) and len(args[0]) > 1:
                args = args[0]
            else:
                args = (type(args[0]), args[0])
        return self._scheduler._throw(self, *args)

    def value(self, timeout=None):
        """Get exit "value" of task.

        NB: This method should _not_ be called from a task! This method is meant
        for main thread in the user program to wait for (main) task(s) it
        creates.

        Once task stops (finishes) executing, the value it exited with
        'raise StopIteration(value)' is returned.
        """
        if not hasattr(self, '_complete'):
            logger.warning('task %s is not suitable for "value"', self)
            return None
        value = None
        self._scheduler._lock.acquire()
        if self._complete is None:
            self._complete = threading.Event()
            self._scheduler._lock.release()
            if self._complete.wait(timeout=timeout) is True:
                value = self._value
        elif self._complete == 0:
            self._scheduler._lock.release()
            value = self._value
        else:
            self._scheduler._lock.release()
            if self._complete.wait(timeout=timeout) is True:
                value = self._value
        return value

    def finish(self, timeout=None):
        """Get exit "value" of task. Must be used in a task with 'yield' as
        'value = yield other_task.finish()'

        Once task stops (finishes) executing, the value it exited with
        'raise StopIteration(value)' is returned.
        """
        if not hasattr(self, '_complete'):
            logger.warning('task %s is not suitable for "finish"', self)
            raise StopIteration(None)
        value = None
        if self._complete is None:
            self._complete = Event()
            if (yield self._complete.wait(timeout=timeout)) is True:
                value = self._value
        elif self._complete == 0:
            value = self._value
        elif isinstance(self._complete, Event):
            if (yield self._complete.wait(timeout=timeout)) is True:
                value = self._value
        else:
            raise RuntimeError('invalid wait on %s/%s: %s' %
                               (self._name, self._id, type(self._complete)))
        raise StopIteration(value)

    __call__ = finish

    def terminate(self):
        """Terminate task.

        If this method called by a thread (and not a task), there is a chance
        that task being terminated is currently running and can interfere with
        GenratorExit exception that will be thrown to task.

        Can also be used on remotely running tasks.
        """
        if self._location:
            kwargs = {'task': self._id, 'name': self._name, 'rid': self._rid}
            request = _NetRequest('terminate_task', kwargs=kwargs, dst=self._location,
                                  timeout=MsgTimeout)
            if _Peer.send_req(request) != 0:
                logger.warning('remote task at %s may not be valid', self._location)
                return -1
            return 0
        else:
            return self._scheduler._terminate_task(self)

    def hot_swappable(self, flag):
        if Pycos.cur_task(self._scheduler) == self:
            if flag:
                self._hot_swappable = True
                if self._swap_generator:
                    return self._scheduler._swap_generator(self)
            else:
                self._hot_swappable = False
            return 0
        else:
            logger.warning('hot_swappable must be called from running task')
            return -1

    def hot_swap(self, *args, **kwargs):
        """Replaces task's generator function with given generator.

        The new generator starts executing from the beginning. If there are any
        pending messages, they will not be reset, so new generator can process
        them (or clear them with successive 'receive' calls with timeout=0 until
        it returns 'alarm_value').
        """
        try:
            generator = Task.__get_generator(self, *args, **kwargs)
        except Exception:
            logger.warning('hot_swap is called with non-generator!')
            return -1
        self._swap_generator = generator
        return self._scheduler._swap_generator(self)

    def monitor(self, observe):
        """Must be used with 'yield' as 'yield task.monitor(observe)', where
        'observe' is a task which will be monitored by 'task'.  When 'observe'
        is finished (raises StopIteration or terminated due to uncaught
        exception), that exception is sent as a message to 'task' (monitor
        task).

        Monitor can inspect the exception and restart observed task if
        necessary. 'observe' can be a remote task.

        Can also be used on remotely running 'observe' task.
        """
        if observe._location:
            # remote task
            kwargs = {'monitor': self, 'name': observe._name,
                      'task': observe._id, 'rid': observe._rid}
            request = _NetRequest('monitor', kwargs=kwargs, dst=observe._location,
                                  timeout=MsgTimeout)
            reply = yield _Peer.sync_reply(request, alarm_value=-1)
        else:
            reply = self._scheduler._monitor(self, observe)
        raise StopIteration(reply)

    def notify(self, monitor):
        """Similar to 'monitor' method, except that it is invoked with local
        task to add argument as monitor.
        """
        if self._location:
            return -1
        else:
            return self._scheduler._monitor(monitor, self)

    def is_alive(self):
        """Returns True if task is known to scheduler; otherwise (e.g., task
        finished) returns False.
        """
        if self._location:
            logger.warning('%s: is_alive for %s is invliad', self._location, self)
        else:
            if self._state is None:
                return False
            return True

    def _await_(self, timeout=None, alarm_value=None):
        """Internal use only.
        """
        return self._scheduler._suspend(self, timeout, alarm_value, Pycos._AwaitIO_)

    def _proceed_(self, update=None):
        """Internal use only.
        """
        return self._scheduler._resume(self, update, Pycos._AwaitIO_)

    @staticmethod
    def __get_generator(task, *args, **kwargs):
        if args:
            target = args[0]
            args = args[1:]
        else:
            target = kwargs.pop('target', None)
            args = kwargs.pop('args', ())
            kwargs = kwargs.pop('kwargs', kwargs)
        if not inspect.isgeneratorfunction(target):
            raise Exception('%s is not a generator!' % target.__name__)
        if target.__defaults__ and \
           'task' in target.__code__.co_varnames[:target.__code__.co_argcount][-len(target.__defaults__):]:
            kwargs['task'] = task
        return target(*args, **kwargs)

    def __getstate__(self):
        if not self._rid:
            self._rid = _time()
        state = {'name': self._name, 'id': str(self._id), 'rid': self._rid}
        if self._location:
            state['location'] = self._location
        else:
            state['location'] = Task._sign
        return state

    def __setstate__(self, state):
        self._name = state['name']
        self._id = state['id']
        self._rid = state['rid']
        self._location = state['location']
        if isinstance(self._location, Location):
            if self._location in Task._pycos._locations:
                self._location = None
        else:
            self._location = _Peer.sign_location(self._location)
            # TODO: is it possible for peer to disconnect during deserialize?
        if self._location:
            self._scheduler = None
        else:
            if isinstance(self._name, str) and len(self._name) > 1:
                self._id = int(self._id)
                if self._name[0] == '^':
                    self._scheduler = SysTask._pycos
                else:
                    self._scheduler = Task._pycos
            else:
                logger.warning('invalid task from remote peer: %s', self._name)
                self._scheduler = None

    def __repr__(self):
        s = '%s/%s' % (self._name, self._id)
        if self._location:
            s = '%s@%s' % (s, self._location)
        return s

    def __eq__(self, other):
        return (isinstance(other, Task) and
                self._id == other._id and self._location == other._location)

    def __ne__(self, other):
        return ((not isinstance(other, Task)) or
                self._id != other._id or self._location != other._location)

    def __hash__(self):
        if self._location:
            return hash(str(self))
        else:
            return id(self)


class Location(object):
    """Distributed pycos, tasks, channels use Location to identify where they
    are running, where to send a message etc.

    Users can create instances (which can be passed to 'locate' methods) 'host'
    is either name or IP address and 'tcp_port' is TCP port where (peer) pycos
    runs network services.
    """

    __slots__ = ('addr', 'port')

    def __init__(self, host, tcp_port):
        if re.match(r'\d+[\.\d]+$', host) or re.match(r'[0-9a-fA-F]*:[:0-9a-fA-F]+$', host):
            self.addr = host
        else:
            if hasattr(Pycos, 'host_ipaddr'):
                self.addr = Pycos.host_ipaddr(host)
            else:
                self.addr = socket.getaddrinfo(host, 0, 0, socket.SOCK_STREAM)[0][4][0]
            if not self.addr:
                logger.warning('host "%s" for Location may not be valid', host)
        self.port = int(tcp_port)

    def __eq__(self, other):
        return (isinstance(other, Location) and
                self.port == other.port and self.addr == other.addr)

    def __ne__(self, other):
        return ((not isinstance(other, Location)) or
                self.port != other.port or self.addr != other.addr)

    def __repr__(self):
        return '%s:%s' % (self.addr, self.port)

    def __hash__(self):
        return hash('%s:%s' % (self.addr, self.port))


class Channel(object):
    """Subscription based channel. Broadcasts a message to all registered
    subscribers, whether they are currently waiting for message or not. Messages
    are queued (buffered) at receiving tasks. To get a message, a task must use
    'yield task.receive()', with timeout and alarm_value, if necessary.

    Channels can be hierarchical, and subscribers can be remote.
    """

    __slots__ = ('_name', '_id', '_location', '_transform', '_subscribers', '_subscribe_event',
                 '_scheduler', '_rid')

    _pycos = None
    _sign = None

    def __init__(self, name, transform=None):
        """'name' must be unique across all channels.

        'transform' is a function that can either filter or transform a
        message. If the function returns 'None', the message is filtered
        (ignored). The function is called with first parameter set to channel
        name and second parameter set to the message.
        """

        if not Channel._pycos:
            Pycos.instance()
        self._scheduler = Channel._pycos
        self._location = None
        self._id = id(self)
        if transform is not None:
            try:
                argspec = inspect.getargspec(transform)
                assert len(argspec.args) == 2
            except Exception:
                logger.warning('invalid "transform" function ignored')
                transform = None
        self._transform = transform
        self._name = name
        if not name[0].isalnum():
            while not name[0].isalnum():
                name = name[1:]
            logger.warning('Channel name "%s" should begin with alpha-numeric character;'
                           'it is changed to "%s"', self._name, name)
            self._name = name
        self._subscribers = set()
        self._subscribe_event = Event()
        self._rid = None
        self._scheduler._lock.acquire()
        # TODO: use name or id for storing channels?
        if self._name in self._scheduler._channels:
            logger.warning('Duplicate channel "%s" ignored!', self._name)
        else:
            self._scheduler._channels[self._name] = self
        self._scheduler._lock.release()

    @property
    def location(self):
        """Get Location instance where this channel is located.

        Can also be used on remote channels.
        """
        if self._location:
            return copy.copy(self._location)
        return self._scheduler.location

    @property
    def name(self):
        """Get name of channel.

        Can also be used on remote channel.
        """
        return self._name

    @staticmethod
    def locate(name, location=None, timeout=None):
        """Must be used with 'yield' as
        'rchannel = yield Channel.locate("name")'.

        Returns Channel instance to registered channel at remote peers so it can
        be used to send/deliver messages..
        """
        if not Channel._pycos:
            Pycos.instance()
        if not location or location in Channel._pycos._locations:
            rchannel = Channel._pycos._channels.get(name, None)
            if rchannel or location in Channel._pycos._locations:
                raise StopIteration(rchannel)
        req = _NetRequest('locate_channel', kwargs={'name': name}, dst=location, timeout=timeout)
        rchannel = yield _Peer.async_request(req)
        raise StopIteration(rchannel)

    def register(self):
        """A registered channel can be located (with 'locate') by a task on a
        remote pycos.
        """
        if self._location:
            return -1
        return self._scheduler._register_channel(self, self._name)

    def unregister(self):
        """Unregister channel (see 'register' above).
        """
        if self._location:
            return -1
        return self._scheduler._unregister_channel(self, self._name)

    def set_transform(self, transform):
        if self._location:
            return -1
        try:
            argspec = inspect.getargspec(transform)
            assert len(argspec.args) == 2
        except Exception:
            logger.warning('invalid "transform" function ignored')
            return -1
        self._transform = transform
        return 0

    def subscribe(self, subscriber, timeout=None):
        """Must be used with 'yield', as, for example,
        'yield channel.subscribe(task)'.

        Subscribe to receive messages. Senders don't need to subscribe. A
        message sent to this channel is delivered to all subscribers.

        Can also be used on remote channels.
        """
        if not isinstance(subscriber, Task) and not isinstance(subscriber, Channel):
            logger.warning('invalid subscriber ignored')
            raise StopIteration(-1)
        if self._location:
            # remote channel
            kwargs = {'channel': self._name, 'id': self._id, 'rid': self._rid,
                      'subscriber': subscriber}
            request = _NetRequest('subscribe', kwargs=kwargs, dst=self._location, timeout=timeout)
            reply = yield _Peer.sync_reply(request, alarm_value=-1)
        else:
            self._subscribers.add(subscriber)
            self._subscribe_event.set()
            reply = 0
        raise StopIteration(reply)

    def unsubscribe(self, subscriber, timeout=None):
        """Must be called with 'yield' as, for example,
        'yield channel.unsubscribe(task)'.

        Future messages will not be delivered after unsubscribing.

        Can also be used on remote channels.
        """
        if not isinstance(subscriber, Task) and not isinstance(subscriber, Channel):
            logger.warning('invalid subscriber ignored')
            raise StopIteration(-1)
        if self._location:
            # remote channel
            kwargs = {'channel': self._name, 'id': self._id, 'rid': self._rid,
                      'subscriber': subscriber}
            request = _NetRequest('unsubscribe', kwargs=kwargs, dst=self._location,
                                  timeout=timeout)
            reply = yield _Peer.sync_reply(request, alarm_value=-1)
        else:
            try:
                self._subscribers.remove(subscriber)
            except KeyError:
                reply = -1
            else:
                reply = 0
        raise StopIteration(reply)

    def send(self, message):
        """Message is sent to currently registered subscribers.

        Can also be used on remote channels.
        """
        if self._location:
            # remote channel
            kwargs = {'channel': self._name, 'id': self._id, 'rid': self._rid, 'message': message}
            request = _NetRequest('send', kwargs=kwargs, dst=self._location, timeout=MsgTimeout)
            # request is queued for asynchronous processing
            if _Peer.send_req(request) != 0:
                logger.warning('remote channel at %s may not be valid', self._location)
                return -1
        else:
            if self._transform:
                try:
                    message = self._transform(self.name, message)
                except Exception:
                    message = None
                if message is None:
                    return 0
            invalid = []
            for subscriber in self._subscribers:
                if subscriber.send(message) != 0:
                    invalid.append(subscriber)
            if invalid:
                def _unsub(self, subscriber, task=None):
                    logger.debug('remote subscriber %s is not valid; unsubscribing it', subscriber)
                    yield self.unsubscribe(subscriber)
                for subscriber in invalid:
                    Task(_unsub, self, subscriber)
        return 0

    def deliver(self, message, timeout=None, n=0):
        """Must be used with 'yield' as 'rcvd = yield channel.deliver(message)'.

        Blocking 'send': Wait until message can be delivered to at least 'n'
        subscribers before timeout. Returns number of end-point recipients
        (tasks) the message is delivered to; i.e., in case of heirarchical
        channels, it is the sum of recipients of all the channels.

        Can also be used on remote channels.
        """
        if not isinstance(n, int) or n < 0:
            raise StopIteration(-1)
        if self._location:
            # remote channel
            kwargs = {'channel': self._name, 'id': self._id, 'rid': self._rid, 'message': message,
                      'n': n}
            request = _NetRequest('deliver', kwargs=kwargs, dst=self._location, timeout=timeout)
            reply = yield _Peer.async_reply(request, alarm_value=0)
            # if reply < 0:
            #     logger.warning('remote channel "%s" at %s may have gone away!',
            #                    self._name, self._location)
            raise StopIteration(reply)
        else:
            if self._transform:
                try:
                    message = self._transform(self.name, message)
                except Exception:
                    message = None
                if message is None:
                    raise StopIteration(0)
            subscribers = list(self._subscribers)
            if n:
                while len(subscribers) < n:
                    start = _time()
                    self._subscribe_event.clear()
                    if (yield self._subscribe_event.wait(timeout)) is False:
                        raise StopIteration(0)
                    if timeout is not None:
                        timeout -= _time() - start
                        if timeout <= 0:
                            raise StopIteration(0)
                    subscribers = list(self._subscribers)

            info = {'reply': 0, 'pending': len(subscribers), 'success': 0,
                    'done': Event(), 'invalid': []}

            def _deliver(subscriber, info, timeout, n, task=None):
                try:
                    reply = yield subscriber.deliver(message, timeout=timeout)
                    if reply > 0:
                        info['reply'] += reply
                        info['success'] += 1
                        if n > 0 and info['success'] >= n:
                            info['done'].set()
                    elif reply < 0:
                        info['invalid'].append(subscriber)
                except Exception:
                    pass
                info['pending'] -= 1
                if info['pending'] == 0:
                    info['done'].set()
            for subscriber in subscribers:
                if isinstance(subscriber, Task) and not subscriber._location:
                    if subscriber.send(message) == 0:
                        info['reply'] += 1
                        info['success'] += 1
                    info['pending'] -= 1
                else:
                    # channel/remote task
                    Task(_deliver, subscriber, info, timeout, n)
            if info['pending'] == 0:
                info['done'].set()
            if n == 0 or info['success'] < n:
                yield info['done'].wait(timeout)

            if info['invalid']:
                def _unsub(self, subscriber, task=None):
                    logger.debug('remote subscriber %s is not valid; unsubscribing it', subscriber)
                    yield self.unsubscribe(subscriber)
                for subscriber in info['invalid']:
                    Task(_unsub, self, subscriber)

            raise StopIteration(info['reply'])

    def close(self):
        if not self._location:
            self.unregister()
            self._subscribers = set()
            self._scheduler._lock.acquire()
            self._scheduler._channels.pop(self._name, None)
            self._scheduler._lock.release()

    def __getstate__(self):
        if not self._rid:
            self._rid = _time()
        state = {'name': self._name, 'id': self._id, 'rid': self._rid}
        if self._location:
            state['location'] = self._location
        else:
            state['location'] = Channel._sign
        return state

    def __setstate__(self, state):
        self._name = state['name']
        self._id = state['id']
        self._rid = state['rid']
        self._transform = None
        self._location = state['location']
        if isinstance(self._location, Location):
            if self._location in Channel._pycos._locations:
                self._location = None
        else:
            self._location = _Peer.sign_location(self._location)
            # TODO: is it possible for peer to disconnect during deserialize?
        if self._location:
            self._scheduler = None
        else:
            if isinstance(self._name, str) and len(self._name) > 1:
                self._scheduler = Channel._pycos
            else:
                logger.warning('invalid scheduler: %s', self._scheduler)
                self._scheduler = None

    def __eq__(self, other):
        return (isinstance(other, Channel) and
                self._name == other._name and self._location == other._location)

    def __ne__(self, other):
        return ((not isinstance(other, Channel)) or
                self._name != other._name or self._location != other._location)

    def __repr__(self):
        if self._location:
            return '%s@%s' % (self._name, self._location)
        else:
            return self._name


class CategorizeMessages(object):
    """Splits messages to task into categories so that they can be processed on
    priority basis, for example.
    """

    def __init__(self, task):
        """Categorize messages to task 'task'.
        """
        self._task = task
        self._categories = {None: collections.deque()}
        self._categorize = []

    def add(self, categorize):
        """Add given method to categorize messages. When a message is received,
        each of the added methods (most recently added method first) is called
        with the message. The method should return a category (any hashable
        object) or None (in which case next recently added method is called with
        the same message). If all the methods return None for a given message,
        the message is queued with category=None, so that 'receive' method here
        works just as Task.receive.
        """
        if inspect.isfunction(categorize):
            argspec = inspect.getargspec(categorize)
            if len(argspec.args) != 1:
                categorize = None
        elif type(categorize) != partial_func:
            categorize = None

        if categorize:
            self._categorize.insert(0, categorize)
        else:
            logger.warning('invalid categorize function ignored')

    def remove(self, categorize):
        """Remove given method (added earlier).
        """
        try:
            self._categorize.remove(categorize)
        except ValueError:
            logger.warning('invalid categorize function')

    def receive(self, category=None, timeout=None, alarm_value=None):
        """Similar to 'receive' of Task, except it retrieves (waiting, if
        necessary) messages in given 'category'.
        """
        # assert Pycos.cur_task() == self._task
        c = self._categories.get(category, None)
        if c:
            msg = c.popleft()
            raise StopIteration(msg)
        if timeout:
            start = _time()
        while 1:
            msg = yield self._task.receive(timeout=timeout, alarm_value=alarm_value)
            if msg == alarm_value:
                raise StopIteration(msg)
            for categorize in self._categorize:
                c = categorize(msg)
                if c == category:
                    raise StopIteration(msg)
                if c is not None:
                    bucket = self._categories.get(c, None)
                    if not bucket:
                        bucket = self._categories[c] = collections.deque()
                    bucket.append(msg)
                    break
            else:
                self._categories[None].append(msg)
            if timeout:
                now = _time()
                timeout -= now - start
                start = now

    recv = receive


class Pycos(object, metaclass=Singleton):
    """Task scheduler.

    The scheduler is created and started automatically (when a task is created,
    for example), so there is no reason to create it explicitly. To use
    distributed programming, Pycos in netpycos module should be used.
    """

    _schedulers = {}

    # waiting for turn to execute, in _scheduled set
    _Scheduled = 1
    # currently executing, in _scheduled set
    _Running = 2
    # waiting for resume
    _Suspended = 3
    # waiting for I/O operation
    _AwaitIO_ = 4
    # waiting for message
    _AwaitMsg_ = 5

    def __init__(self):
        self._notifier = _AsyncNotifier()
        if not Task._pycos:
            Task._pycos = Channel._pycos = self
            logger.info('version %s (Python %s) with %s I/O notifier',
                        __version__, platform.python_version(), self._notifier._poller_name)
        self._locations = set()
        self._location = None
        self._name = ''
        self.__cur_task = None
        self._tasks = {}
        self._scheduled = set()
        self._timeouts = []
        self._quit = False
        self._daemons = 0
        self._channels = {}
        self._rtasks = {}
        self._rchannels = {}
        self._atexit = []
        self._polling = False
        self._lock = threading.RLock()
        self._complete = threading.Event()
        self._complete.set()
        self._scheduler = threading.Thread(target=self._schedule)
        Pycos._schedulers[id(self._scheduler)] = self
        self._scheduler.daemon = True
        self._scheduler.start()
        atexit.register(self.finish)

    @classmethod
    def instance(cls, *args, **kwargs):
        """Returns (singleton) instance of Pycos.
        """
        return cls(*args, **kwargs)

    @property
    def location(self):
        """Get Location instance where this Pycos is running.
        """
        return copy.copy(self._location)

    @property
    def locations(self):
        """Get Location instances where this Pycos is running.
        """
        return [copy.copy(location) for location in self._locations]

    @property
    def name(self):
        """Get name of Pycos.
        """
        return self._name

    @staticmethod
    def scheduler():
        return Pycos._schedulers.get(id(threading.current_thread()), None)

    @staticmethod
    def cur_task(scheduler=None):
        """Must be called from a task only.
        """
        if not scheduler:
            scheduler = Pycos._schedulers.get(id(threading.current_thread()), None)
            if not scheduler:
                return None
        return scheduler.__cur_task

    def _add(self, task):
        """Internal use only. See Task class.
        """
        self._lock.acquire()
        self._tasks[task._id] = task
        self._complete.clear()
        task._state = Pycos._Scheduled
        self._scheduled.add(task)
        if self._polling and len(self._scheduled) == 1:
            self._notifier.interrupt()
        self._lock.release()

    def _remove(self, task):
        """Internal use only.
        """
        self._lock.acquire()
        ret = -1
        if task._state == Pycos._Scheduled or task._state == Pycos._Running:
            if self._tasks.pop(task._id, None) == task:
                self._scheduled.discard(task)
                ret = 0
        self._lock.release()
        return ret

    def _set_daemon(self, task, flag):
        """Internal use only. See set_daemon in Task.
        """
        self._lock.acquire()
        if self.__cur_task != task:
            self._lock.release()
            logger.warning('invalid "set_daemon" - "%s" != "%s"', task, self.__cur_task)
            return -1
        if task._daemon != flag:
            task._daemon = flag
            if flag:
                self._daemons += 1
                if len(self._tasks) == self._daemons:
                    self._complete.set()
            else:
                self._daemons -= 1
                self._complete.clear()
        self._lock.release()
        return 0

    def _monitor(self, monitor, task):
        """Internal use only. See monitor in Task.
        """
        self._lock.acquire()
        tid = task._id
        task = self._tasks.get(tid, None)
        if (not task) or (not isinstance(monitor, Task)):
            self._lock.release()
            logger.warning('monitor: invalid task: %s / %s', tid, type(monitor))
            return -1
        task._monitors.add(monitor)
        self._lock.release()
        return 0

    def _suspend(self, task, timeout, alarm_value, state):
        """Internal use only. See sleep/suspend in Task.
        """
        self._lock.acquire()
        if self.__cur_task != task:
            self._lock.release()
            logger.warning('invalid "suspend" - "%s" != "%s"', task, self.__cur_task)
            return -1
        if state == Pycos._AwaitMsg_ and task._msgs:
            s, update = task._msgs[0]
            if s == state:
                task._msgs.popleft()
                self._lock.release()
                return update
        if timeout is None:
            task._timeout = None
        else:
            if not isinstance(timeout, (float, int)):
                logger.warning('invalid timeout %s', timeout)
                self._lock.release()
                return -1
            if timeout <= 0:
                self._lock.release()
                return alarm_value
            else:
                task._timeout = _time() + timeout + 0.0001
                heappush(self._timeouts, (task._timeout, task._id, alarm_value))
        self._scheduled.discard(task)
        task._state = state
        self._lock.release()
        return 0

    def _resume(self, target, update, state):
        """Internal use only. See resume in Task.
        """
        self._lock.acquire()
        tid = target._id
        task = self._tasks.get(tid, None)
        if not task or task._name != target._name:
            self._lock.release()
            logger.warning('invalid task %s to resume', tid)
            return -1
        if task._state == state:
            task._timeout = None
            task._value = update
            self._scheduled.add(task)
            task._state = Pycos._Scheduled
            if self._polling and len(self._scheduled) == 1:
                self._notifier.interrupt()
        elif state == Pycos._AwaitMsg_:
            task._msgs.append((state, update))
        else:
            logger.warning('ignoring resume for %s: %s', task, task._state)
        self._lock.release()
        return 0

    def _throw(self, task, *args):
        """Internal use only. See throw in Task.
        """
        self._lock.acquire()
        tid = task._id
        task = self._tasks.get(tid, None)
        if ((not task) or task._state not in (Pycos._Scheduled, Pycos._Suspended,
                                              Pycos._AwaitIO_, Pycos._AwaitMsg_)):
            logger.warning('invalid task %s to throw exception', tid)
            self._lock.release()
            return -1
        task._timeout = None
        task._exceptions.append(args)
        if task._state in (Pycos._AwaitIO_, Pycos._Suspended, Pycos._AwaitMsg_):
            self._scheduled.add(task)
            task._state = Pycos._Scheduled
            if self._polling and len(self._scheduled) == 1:
                self._notifier.interrupt()
        self._lock.release()
        return 0

    def _terminate_task(self, task):
        """Internal use only.
        """
        self._lock.acquire()
        tid = task._id
        task = self._tasks.get(tid, None)
        if not task:
            logger.warning('invalid task %s to terminate', tid)
            self._lock.release()
            return -1
        # TODO: if currently waiting I/O or holding locks, warn?
        if task._state == Pycos._Running:
            logger.warning('task to terminate %s is running', task)
        else:
            self._scheduled.add(task)
            task._state = Pycos._Scheduled
            task._timeout = None
            task._callers = []
            if self._polling and len(self._scheduled) == 1:
                self._notifier.interrupt()
        task._exceptions.append((GeneratorExit, GeneratorExit('terminated')))
        self._lock.release()
        return 0

    def _swap_generator(self, task):
        """Internal use only.
        """
        self._lock.acquire()
        tid = task._id
        task = self._tasks.get(tid, None)
        if not task:
            logger.warning('invalid task %s to swap', tid)
            self._lock.release()
            return -1
        if task._callers or not task._hot_swappable:
            logger.debug('postponing hot swapping of %s', str(task))
            self._lock.release()
            return 0
        else:
            task._timeout = None
            # TODO: check that another HotSwapException is not pending?
            if task._state is None:
                task._generator = task._swap_generator
                task._value = None
                if task._complete == 0:
                    task._complete = None
                elif isinstance(task._complete, Event):
                    task._complete.clear()
                self._scheduled.add(task)
                task._state = Pycos._Scheduled
                task._hot_swappable = False
            else:
                task._exceptions.append((HotSwapException, HotSwapException(task._swap_generator)))
                # assert task._state != Pycos._AwaitIO_
                if task._state in (Pycos._Suspended, Pycos._AwaitMsg_):
                    self._scheduled.add(task)
                    task._state = Pycos._Scheduled
            if self._polling and len(self._scheduled) == 1:
                self._notifier.interrupt()
            task._swap_generator = None
        self._lock.release()
        return 0

    def _schedule(self):
        """Internal use only.
        """
        while not self._quit:
            # process I/O events
            self._notifier.poll(0)
            self._lock.acquire()
            if not self._scheduled:
                if self._timeouts:
                    timeout = self._timeouts[0][0] - _time()
                    # pollers may timeout slightly earlier, so give a bit of
                    # slack
                    if timeout <= 0.0001:
                        timeout = 0
                else:
                    timeout = None
                self._polling = True
                self._lock.release()
                self._notifier.poll(timeout)
                self._lock.acquire()
                self._polling = False
            if self._timeouts:
                now = _time() + 0.0001
                while self._timeouts and self._timeouts[0][0] <= now:
                    timeout, tid, alarm_value = heappop(self._timeouts)
                    task = self._tasks.get(tid, None)
                    if not task or task._timeout != timeout:
                        continue
                    task._timeout = None
                    task._state = Pycos._Scheduled
                    self._scheduled.add(task)
                    task._value = alarm_value
            scheduled = list(self._scheduled)
            self._lock.release()

            for task in scheduled:
                task._state = Pycos._Running
                self.__cur_task = task

                try:
                    if task._exceptions:
                        exc = task._exceptions.pop(0)
                        if exc[0] == GeneratorExit:
                            # task._generator.close()
                            raise Exception(exc[1])
                        else:
                            retval = task._generator.throw(*exc)
                    else:
                        retval = task._generator.send(task._value)
                except Exception:
                    self._lock.acquire()
                    exc = sys.exc_info()
                    if exc[0] == StopIteration:
                        v = exc[1].args
                        # assert isinstance(v, tuple)
                        if v:
                            task._value = v[0]
                        else:
                            task._value = None
                        task._exceptions = []
                    elif exc[0] == HotSwapException:
                        v = exc[1].args
                        if (isinstance(v, tuple) and len(v) == 1 and inspect.isgenerator(v[0]) and
                            task._hot_swappable and not task._callers):
                            try:
                                task._generator.close()
                            except Exception:
                                logger.warning('closing %s/%s raised exception: %s',
                                               task._name, task._id, traceback.format_exc())
                            task._generator = v[0]
                            task._name = task._generator.__name__
                            task._exceptions = []
                            task._value = None
                            # task._msgs is not reset, so new
                            # task can process pending messages
                            task._state = Pycos._Scheduled
                        else:
                            logger.warning('invalid HotSwapException from %s/%s ignored',
                                           task._name, task._id)
                        self._lock.release()
                        continue
                    else:
                        v = exc[1].args
                        if isinstance(v, tuple) and len(v) > 0 and type(v[0]) == GeneratorExit:
                            try:
                                task._generator.close()
                            except Exception:
                                logger.debug('closing %s raised exception: %s',
                                             task.name, traceback.format_exc())
                            task._exceptions = [(GeneratorExit, v[0], None)]
                        else:
                            task._exceptions.append(exc)

                    if task._callers:
                        # return to caller
                        caller = task._callers.pop(-1)
                        task._generator = caller[0]
                        if task._swap_generator and not task._callers and task._hot_swappable:
                            task._exceptions.append((HotSwapException,
                                                     HotSwapException(task._swap_generator)))
                            task._swap_generator = None
                            task._state = Pycos._Scheduled
                        elif task._exceptions:
                            # exception in callee, restore saved value
                            task._value = caller[1]
                            self._scheduled.add(task)
                            task._state = Pycos._Scheduled
                        elif task._state == Pycos._Running:
                            task._state = Pycos._Scheduled
                    else:
                        if task._exceptions:
                            if task._exceptions[-1][0] == GeneratorExit:
                                task._exceptions = []
                            else:
                                exc = task._exceptions[0]
                                if len(exc) == 2 or not exc[2]:
                                    exc_trace = ''.join(traceback.format_exception_only(*exc[:2]))
                                else:
                                    exc_trace = ''.join(traceback.format_exception(exc[0], exc[1],
                                                                                   exc[2].tb_next))
                                logger.warning('uncaught exception in %s:\n%s', task, exc_trace)
                                try:
                                    task._generator.close()
                                except Exception:
                                    logger.warning('closing %s raised exception: %s',
                                                   task._name, traceback.format_exc())
                                task._value = MonitorStatus(task, exc[0], exc_trace)

                        # delete this task
                        # if task._state not in (Pycos._Scheduled, Pycos._Running):
                        #     logger.warning('task "%s" is in state: %s', task._name, task._state)
                        monitors = list(task._monitors)
                        for monitor in monitors:
                            if task._exceptions:
                                exc = task._exceptions[0]
                                exc = MonitorStatus(task, exc[0], exc_trace)
                            else:
                                exc = (StopIteration, task._value)
                                if monitor._location:
                                    try:
                                        serialize(task._value)
                                    except Exception as exc:
                                        exc = (type(exc), traceback.format_exc())
                                exc = MonitorStatus(task, exc[0], exc[1])
                            if monitor.send(exc):
                                logger.warning('monitor for %s is not valid!', task.name)
                                task._monitors.discard(monitor)

                        task._msgs.clear()
                        task._monitors.clear()
                        task._exceptions = []
                        if task._daemon is True:
                            self._daemons -= 1
                        self._tasks.pop(task._id, None)
                        task._generator = None
                        task._state = None
                        if task._complete:
                            task._complete.set()
                        else:
                            task._complete = 0
                        self._scheduled.discard(task)
                        if len(self._tasks) == self._daemons:
                            self._complete.set()
                    self._lock.release()
                else:
                    self._lock.acquire()
                    if task._state == Pycos._Running:
                        task._state = Pycos._Scheduled
                        # if this task is suspended, don't update the value;
                        # when it is resumed, it will be updated with the
                        # 'update' value
                        task._value = retval

                    if isinstance(retval, types.GeneratorType):
                        # push current generator onto stack and activate new
                        # generator
                        task._callers.append((task._generator, task._value))
                        task._generator = retval
                        task._value = None
                    self._lock.release()
            self.__cur_task = None

        self._lock.acquire()
        for task in self._tasks.values():
            logger.debug('terminating task %s/%s%s', task._name, task._id,
                         ' (daemon)' if task._daemon else '')
            self.__cur_task = task
            task._state = Pycos._Running
            while task._generator:
                try:
                    task._generator.close()
                except Exception:
                    logger.warning('closing %s raised exception: %s',
                                   task._generator.__name__, traceback.format_exc())
                if task._callers:
                    task._generator, task._value = task._callers.pop(-1)
                else:
                    task._generator = None
            if task._complete:
                task._complete.set()
            else:
                task._complete = 0
        self._scheduled.clear()
        self._tasks.clear()
        self._channels.clear()
        self._timeouts = []
        self._quit = True
        Pycos._schedulers.pop(id(threading.current_thread()))
        self._lock.release()
        self._notifier.terminate()
        if self == Task._pycos:
            logger.debug('pycos terminated')
        else:
            logger.debug('pycos %s terminated', self.location)
        self._complete.set()

    def _exit(self, await_non_daemons, reset):
        """Internal use only.
        """
        if self._quit:
            return
        if await_non_daemons:
            if len(self._tasks) > self._daemons:
                logger.debug('waiting for %s tasks to terminate',
                             (len(self._tasks) - self._daemons))
        else:
            self._lock.acquire()
            for task in self._tasks.values():
                if not task._daemon:
                    task._daemon = True
                    self._daemons += 1
            if len(self._tasks) != self._daemons:
                logger.warning('daemons mismatch: %s != %s', len(self._tasks), self._daemons)
            self._complete.set()
            self._lock.release()

        self._complete.wait()

        if self._atexit:
            while self._atexit:
                priority, func, fargs, fkwargs = self._atexit.pop()
                try:
                    func(*fargs, **fkwargs)
                except Exception:
                    logger.warning('running %s failed:', func.__name__)
                    logger.warning(traceback.format_exc())
            self._complete.wait()
        if self._locations and self == SysTask._pycos:
            _Peer.shutdown()
            self._complete.wait()

        self._lock.acquire()
        if not self._quit:
            self._complete.clear()
            self._quit = True
            # add a dummy timeout so scheduler will not wait for any other
            # timeouts left behind by tasks that may have quit already
            heappush(self._timeouts, (_time() + 0.1, None, None))
            self._lock.release()
            self._notifier.interrupt()
            self._complete.wait()
        else:
            self._lock.release()
        logger.shutdown()
        if reset:
            Task._pycos = Channel._pycos = None
            Singleton.discard(self.__class__)

    def finish(self):
        """Wait until all non-daemon tasks finish and then shutdown the
        scheduler.

        Should be called from main program (or a thread, but _not_ from tasks).
        """
        self._exit(True, True)

    def terminate(self):
        """Kill all non-daemon tasks and shutdown the scheduler.

        Should be called from main program (or a thread, but _not_ from tasks).
        """
        self._exit(False, True)

    def join(self, show_running=False, timeout=None):
        """Wait for currently scheduled tasks to finish. Pycos continues to
        execute, so new tasks can be added if necessary.
        """
        if show_running:
            self._lock.acquire()
            for task in self._tasks.values():
                logger.info('waiting for %s/%s%s', task._name, task._id,
                            ' (daemon)' if task._daemon else '')
            self._lock.release()
        self._complete.wait(timeout)
        if self._complete.is_set():
            return True
        else:
            return False

    def is_alive(self):
        """Returns whether scheduler thread is alive.
        """
        return self._scheduler.is_alive()

    def atexit(self, priority, func, *fargs, **fkwargs):
        """Function 'func' will be called after the scheduler has
        terminated. 'priority' indicates the order in which all queued functions
        will be called; higher priority functions are called before lower
        priority functions.
        """
        item = (priority, func, fargs, fkwargs)
        self._lock.acquire()
        i = bisect_left(self._atexit, item)
        self._atexit.insert(i, item)
        self._lock.release()

    def drop_atexit(self, priority, func):
        """Drop scheduled 'func' at 'priority'. If multiple 'func' at same 'priority' are
        scheduled with different arguments, all of them are dropped.
        """
        item = (priority, func)
        self._lock.acquire()
        i = bisect_left(self._atexit, item)
        while (i < len(self._atexit) and
               (self._atexit[i][0] == priority and self._atexit[i][1] == func)):
            self._atexit.pop(i)
        self._lock.release()

    def _register_channel(self, channel, name):
        """Internal use only.
        """
        self._lock.acquire()
        if name in self._rchannels:
            logger.warning('channel "%s" is already registered', name)
            ret = -1
        elif channel.name not in self._channels:
            logger.warning('channel "%s" is invalid', channel)
            ret = -1
        else:
            self._rchannels[name] = channel
            ret = 0
        self._lock.release()
        return ret

    def _unregister_channel(self, channel, name):
        """Internal use only.
        """
        self._lock.acquire()
        if self._rchannels.get(name, None) is channel:
            self._rchannels.pop(name, None)
            ret = 0
        else:
            # logger.warning('channel "%s" is not registered', name)
            ret = -1
        self._lock.release()
        return ret

    def _register_task(self, task, name):
        """Internal use only.
        """
        self._lock.acquire()
        if name in self._rtasks:
            logger.warning('task "%s" is already registered', name)
            ret = -1
        elif task._id not in self._tasks:
            logger.warning('task "%s" is invalid', task)
            ret = -1
        else:
            self._rtasks[name] = task
            ret = 0
        self._lock.release()
        return ret

    def _unregister_task(self, task, name):
        """Internal use only.
        """
        self._lock.acquire()
        if self._rtasks.get(name, None) is task:
            self._rtasks.pop(name, None)
            ret = 0
        else:
            # logger.warning('task "%s" is not registered', name)
            ret = -1
        self._lock.release()
        return ret

    def __repr__(self):
        return ''


class AsyncThreadPool(object):
    """Schedule synchronous tasks with threads to be executed asynchronously.

    NB: As tasks run in a separate thread, any variables shared between tasks
    and tasks scheduled with thread pool must be protected by thread locking
    (not task locking).
    """

    def __init__(self, num_threads):
        self._scheduler = Pycos.scheduler()
        self._num_threads = num_threads
        self._task_queue = queue.Queue()
        for n in range(num_threads):
            tasklet = threading.Thread(target=self._tasklet)
            tasklet.daemon = True
            tasklet.start()

    def _tasklet(self):
        while 1:
            item = self._task_queue.get(block=True)
            if item is None:
                self._task_queue.task_done()
                break
            task, target, args, kwargs = item
            try:
                val = target(*args, **kwargs)
                task._proceed_(val)
            except Exception:
                task.throw(*sys.exc_info())
            finally:
                self._task_queue.task_done()

    def async_task(self, target, *args, **kwargs):
        """Must be used with 'yield', as
        'val = yield pool.async_task(target, args, kwargs)'.

        @task is task where this method is called.

        @target is function/method that will be executed asynchronously in a
        thread.

        @args and @kwargs are arguments and keyword arguments passed to @target.

        This call effectively returns result of executing
        'target(*args, **kwargs)'.
        """

        if not self._scheduler:
            self._scheduler = Pycos.scheduler()
        task = Pycos.cur_task(self._scheduler)
        # assert isinstance(task, Task)
        # if arguments are passed as per Thread call, get args and kwargs
        if not args and kwargs:
            args = kwargs.pop('args', ())
            kwargs = kwargs.pop('kwargs', kwargs)
        task._await_()
        self._task_queue.put((task, target, args, kwargs))

    def join(self):
        """Wait till all scheduled tasks are completed.
        """
        self._task_queue.join()

    def terminate(self):
        """Wait for all scheduled tasks to complete and terminate
        threads.
        """
        for n in range(self._num_threads):
            self._task_queue.put(None)
        self._task_queue.join()


class AsyncDBCursor(object):
    """Database cursor proxy for asynchronous processing of executions.

    Since connections (and cursors) can't be shared in threads, operations on
    same cursor are run sequentially.
    """

    def __init__(self, thread_pool, cursor):
        self._thread_pool = thread_pool
        self._cursor = cursor
        self._sem = Semaphore()

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def _exec_task(self, func):
        try:
            return func()
        finally:
            self._sem.release()

    def execute(self, query, args=None):
        """Must be used with 'yield' as 'n = yield cursor.execute(stmt)'.
        """
        yield self._sem.acquire()
        self._thread_pool.async_task(self._exec_task,
                                     partial_func(self._cursor.execute, query, args))

    def executemany(self, query, args):
        """Must be used with 'yield' as 'n = yield cursor.executemany(stmt)'.
        """
        yield self._sem.acquire()
        self._thread_pool.async_task(self._exec_task,
                                     partial_func(self._cursor.executemany, query, args))

    def callproc(self, proc, args=()):
        """Must be used with 'yield' as 'yield cursor.callproc(proc)'.
        """
        yield self._sem.acquire()
        self._thread_pool.async_task(self._exec_task,
                                     partial_func(self._cursor.callproc, proc, args))
