"""
This file is part of pycos; see https://pycos.org for details.

This module adds API for distributed programming to Pycos.
"""

import socket
import inspect
import traceback
import os
import sys
import stat
import hashlib
import collections
import copy
import tempfile
import threading
import errno
import ssl
import struct
import re
import platform
try:
    import netifaces
except ImportError:
    netifaces = None

import pycos
from pycos import *
import pycos.config

if os.name == 'nt':
    from errno import WSAEACCES as EADDRINUSE
    from errno import WSAEADDRNOTAVAIL as EADDRNOTAVAIL
else:
    from errno import EADDRINUSE
    from errno import EADDRNOTAVAIL

# PyPI / pip packaging adjusts assertion below for Python 3.7+
assert sys.version_info.major == 3 and sys.version_info.minor < 7, \
    ('"%s" is not suitable for Python version %s.%s; use file installed by pip instead' %
     (__file__, sys.version_info.major, sys.version_info.minor))

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2012-2014 Giridhar Pemmasani"
__license__ = "Apache 2.0"
__url__ = "https://pycos.org"

__version__ = pycos.__version__
__all__ = pycos.__all__ + ['PeerStatus', 'RPS', 'RTI']

MsgTimeout = pycos.config.MsgTimeout


class PeerStatus(object):
    """'peer_status' method of Pycos can be used to be notified of status of
    peers (other Pycos's to communicate for distributed programming). The status
    notifications are sent as messages to the regisered task. Each message is an
    instance of this class.
    """

    Online = 1
    Offline = 0

    def __init__(self, location, name, status):
        self.location = location
        self.name = name
        self.status = status


class Pycos(pycos.Pycos, metaclass=Singleton):
    """This adds network services to pycos.Pycos so it can communicate with
    peers.

    If 'host' is not None, it must be either hostname or IP address, or list of
    hostnames or IP addresses where pycos runs network services.

    'udp_port' is port number where pycos listens for broadcast messages to find
    other peers. Default value for udp_port is set to pycos.config.NetPort.

    'tcp_port' is port number used for TCP services. Its default value is None,
    in which case, port pycos.config.NetPort is used.

    'name' is used in locating peers. They must be unique. If 'name' is not
    given, it is set to string 'host:tcp_port'.

    'ext_ip_addr' is either the IP address or list of IP addresses of NAT
    firewall/gateway if pycos is behind that firewall/gateway. If it is a list,
    each element must correspond to element of 'host' list.

    If 'discover_peers' is True (default), this host broadcasts message to
    detect other peers. If it is False, message is not broadcasted.

    'secret' is string that is used to hash which is used for authentication, so
    only peers that have same secret can communicate.

    'certfile' and 'keyfile' are path names for files containing SSL
    certificates; see Python 'ssl' module.

    'dest_path' is path to directory (folder) where transferred files are
    saved. If path doesn't exist, pycos creates directory with that path.

    'max_file_size' is maximum length of file in bytes allowed for transferred
    files. If it is 0 or None (default), there is no limit.
    """

    _pycos = None
    _pycos_class = pycos.Pycos

    def __init__(self, udp_port=pycos.config.NetPort, tcp_port=None, host=None, ext_ip_addr=None,
                 socket_family=None, ipv4_udp_multicast=False, name=None, discover_peers=True,
                 secret='', certfile=None, keyfile=None, notifier=None,
                 dest_path=None, max_file_size=None):

        Pycos._pycos = Pycos._pycos_class.instance()
        SysTask._pycos = RPS._pycos = _Peer._pycos = self
        super(self.__class__, self).__init__()
        self._rpss = {}
        self._locations = set()
        self._stream_peers = {}
        self._pending_reqs = {}
        self._pending_replies = {}
        self._addrinfos = []

        if not dest_path:
            dest_path = os.path.join(os.sep, tempfile.gettempdir(), 'pycos')
        self.__dest_path = os.path.abspath(os.path.normpath(dest_path))
        self.__dest_path_prefix = dest_path
        # TODO: avoid race condition (use locking to check/create atomically?)
        if not os.path.isdir(self.__dest_path):
            try:
                os.makedirs(self.__dest_path)
            except Exception:
                # likely another pycos created this directory
                if not os.path.isdir(self.__dest_path):
                    logger.warning('failed to create "%s"', self.__dest_path)
                    logger.debug(traceback.format_exc())
        self.max_file_size = max_file_size
        self._secret = secret
        self._certfile = certfile
        self._keyfile = keyfile
        self._ignore_peers = False

        if isinstance(host, list):
            if host:
                hosts = host
            else:
                hosts = [None]
        else:
            hosts = [host]
        if isinstance(ext_ip_addr, list):
            ext_ip_addrs = ext_ip_addr
        else:
            ext_ip_addrs = [ext_ip_addr]

        if not name:
            name = socket.gethostname()
        self.ipv4_udp_multicast = bool(ipv4_udp_multicast)
        location = None
        for i in range(len(hosts)):
            host = hosts[i]
            if len(ext_ip_addrs) > i and ext_ip_addrs[i]:
                ext_ip_addr = Pycos.host_ipaddr(ext_ip_addrs[i])
                if not ext_ip_addr:
                    logger.warning('invalid ext_ip_addr "%s" ignored', ext_ip_addrs[i])
            else:
                ext_ip_addr = None
            addrinfo = Pycos.host_addrinfo(host=host, socket_family=socket_family,
                                           ipv4_multicast=self.ipv4_udp_multicast)
            if not addrinfo:
                logger.warning('Invalid host "%s" ignored', host)
                continue
            addrs = [addrinfo.ip]
            if addrinfo.family == socket.AF_INET6 and addrinfo.ip.startswith('fe80:'):
                # apparently binding to link-local address with OS X may fail in
                # some cases, so try with 'localhost' also
                addrs.append('localhost')
            tcp_sock = socket.socket(addrinfo.family, socket.SOCK_STREAM)
            for addr in addrs:
                if tcp_port is None:
                    tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    try:
                        tcp_sock.bind((addr, pycos.config.NetPort))
                    except socket.error as e:
                        if (e.errno == EADDRINUSE):
                            tcp_sock.bind((addr, 0))
                        elif (addrinfo.ip.startswith('fe80:') and e.errno == EADDRNOTAVAIL):
                            continue
                        else:
                            raise
                else:
                    if tcp_port:
                        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    try:
                        tcp_sock.bind((addr, tcp_port))
                    except socket.error as e:
                        if (addrinfo.ip.startswith('fe80:') and e.errno == EADDRNOTAVAIL):
                            continue
                        else:
                            raise

            location = Location(*(tcp_sock.getsockname()[0:2]))
            addrinfo.ip = location.addr
            if ext_ip_addr:
                location.addr = ext_ip_addr

            tcp_sock.listen(32)
            logger.info('TCP server "%s" @ %s', name if name else '', location)

            addrinfo.tcp_sock = tcp_sock
            addrinfo.location = location
            self._locations.add(location)
            self._addrinfos.append(addrinfo)
            SysTask(self._tcp_proc, addrinfo)

        if not self._addrinfos:
            logger.warning('Could not initialize networking')
            raise Exception('Invalid "host"?')

        if udp_port is None:
            udp_port = pycos.config.NetPort
        udp_addrinfos = {}
        for addrinfo in self._addrinfos:
            udp_addrinfos[addrinfo.bind_addr] = addrinfo
        for bind_addr, addrinfo in udp_addrinfos.items():
            udp_sock = socket.socket(addrinfo.family, socket.SOCK_DGRAM)
            udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, 'SO_REUSEPORT'):
                try:
                    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                except Exception:
                    pass

            udp_sock.bind((bind_addr, udp_port))
            if addrinfo.family == socket.AF_INET:
                if self.ipv4_udp_multicast:
                    mreq = socket.inet_aton(pycos.config.IPV4_MULTICAST_GROUP)
                    mreq += socket.inet_aton(addrinfo.ip)
                    udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            else:  # addrinfo.family == socket.AF_INET6:
                mreq = socket.inet_pton(addrinfo.family, addrinfo.broadcast)
                mreq += struct.pack('@I', addrinfo.ifn)
                udp_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_JOIN_GROUP, mreq)
                try:
                    udp_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                except Exception:
                    pass

            addrinfo.udp_sock = udp_sock
            logger.info('UDP server @ %s:%s', bind_addr, udp_sock.getsockname()[1])
            SysTask(self._udp_proc, location, addrinfo)

        Pycos._pycos._location = self._location = location
        Pycos._pycos._locations = self._locations

        if name:
            self._name = name
        else:
            self._name = str(self._location)
        self._signature = hashlib.sha1(os.urandom(20))
        for location in self._locations:
            self._signature.update(str(location).encode())
        self._signature = self._signature.hexdigest()
        self._auth_code = hashlib.sha1((self._signature + secret).encode()).hexdigest()
        pycos.Task._sign = pycos.Channel._sign = SysTask._sign = RPS._sign = self._signature
        if os.name != 'nt' and '__mp_main__' not in sys.modules:
            sys.modules['__mp_main__'] = sys.modules['__main__']
        if discover_peers:
            self.discover_peers()

    @classmethod
    def instance(cls, *args, **kwargs):
        """Returns (singleton) instance of Pycos.
        """
        return cls(*args, **kwargs)

    @property
    def dest_path(self):
        return self.__dest_path

    @dest_path.setter
    def dest_path(self, path):
        path = os.path.normpath(path)
        if not path.startswith(self.__dest_path_prefix):
            path = os.path.join(self.__dest_path_prefix,
                                os.path.splitdrive(path)[1].lstrip(os.sep))
        if not os.path.isdir(path):
            try:
                os.makedirs(path)
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        self.__dest_path = path

    def _exit(self, await_non_daemons):
        """
        Internal use only.
        """
        if Pycos._pycos:
            Pycos._pycos._exit(await_non_daemons, False)
            super(self.__class__, self)._exit(await_non_daemons, True)
            SysTask._pycos = RPS._pycos = _Peer._pycos = None
            Singleton.discard(self.__class__)
            Pycos._pycos = None

    def finish(self):
        """Wait until all non-daemon tasks finish and then shutdown the
        scheduler.

        Should be called from main program (or a thread, but _not_ from tasks).
        """
        self._exit(True)

    def terminate(self):
        """Kill all non-daemon tasks and shutdown the scheduler.

        Should be called from main program (or a thread, but _not_ from tasks).
        """
        self._exit(False)

    def locate(self, name, timeout=None):
        """Must be used with 'yield' as
        'loc = yield scheduler.locate("peer")'.

        Find and return location of peer with 'name'.
        """
        _Peer._lock.acquire()
        for peer in _Peer.peers.values():
            if peer.name == name:
                loc = peer.location
                break
        else:
            loc = None
        _Peer._lock.release()
        if not loc:
            req = _NetRequest('locate_peer', kwargs={'name': name}, timeout=timeout)
            loc = yield _Peer.async_request(req)
        raise StopIteration(loc)

    def peer(self, loc, udp_port=0, stream_send=False, relay=False, task=None):
        """Must be used with 'yield', as 'status = yield scheduler.peer("loc")'.

        Add pycos running at 'loc' as peer to communicate. Peers on a local
        network can find each other automatically, but if they are on different
        networks, 'peer' can be used so they find each other. 'loc' can be
        either an instance of Location or host name or IP address. If 'loc' is
        Location instance and 'port' is 0, or 'loc' is host name or IP address,
        then all pycoss running at the host will have streaming mode set as per
        'stream_send'.

        If 'stream_send' is True, this pycos uses same connection again and
        again to send messages (i.e., as a stream) to peer 'host' (instead of
        one message per connection).

        If 'relay' is True, the client information is relayed on the
        network of peer. This can be used if client is on remote network and
        needs to communicate with all pycos's available on the network of peer
        (at 'loc').
        """

        if not task:
            task = Pycos.cur_task(self)
        if not isinstance(loc, Location):
            try:
                info = socket.getaddrinfo(loc, None)[0]
                ip_addr = info[4][0]
                if info[0] == socket.AF_INET6:
                    ip_addr = Pycos.host_ipaddr(ip_addr)
                loc = Location(ip_addr, 0)
            except Exception:
                logger.warning('invalid host: "%s"', str(loc))
                raise StopIteration(-1)

        self._lock.acquire()
        if stream_send:
            self._stream_peers[(loc.addr, loc.port)] = True
        else:
            self._stream_peers.pop((loc.addr, loc.port), None)

        if loc.port:
            _Peer._lock.acquire()
            peer = _Peer.peers.get((loc.addr, loc.port), None)
            _Peer._lock.release()
            if peer:
                peer.stream = stream_send
                if not relay:
                    self._lock.release()
                    raise StopIteration(0)
        else:
            _Peer._lock.acquire()
            for (addr, port), peer in _Peer.peers.items():
                if addr == loc.addr:
                    peer.stream = stream_send
                    if not stream_send:
                        self._stream_peers.pop((addr, port), None)
            _Peer._lock.release()
        self._lock.release()

        addrinfo = self._ip_addrinfo_(loc.addr)

        if loc.port:
            req = _NetRequest('signature', kwargs={'version': __version__}, dst=loc)
            sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                               keyfile=self._keyfile, certfile=self._certfile)
            sock.settimeout(2)
            try:
                yield sock.connect((loc.addr, loc.port))
                yield sock.send_msg(serialize(req))
                sign = yield sock.recv_msg()
                ret = yield self._acquaint_({'location': loc, 'signature': sign.decode()},
                                            addrinfo, task=task)
            except Exception:
                ret = -1
            finally:
                sock.close()

            if relay and ret == 0:
                kwargs = {'location': addrinfo.location, 'signature': self._signature,
                          'name': self._name, 'version': __version__}
                _Peer.send_req(_NetRequest('relay_ping', kwargs=kwargs, dst=loc))
            raise StopIteration(ret)
        else:
            if not udp_port:
                udp_port = pycos.config.NetPort
            ping_msg = {'location': addrinfo.location, 'signature': self._signature,
                        'name': self._name, 'version': __version__, 'relay': relay}
            ping_msg = 'ping:'.encode() + serialize(ping_msg)
            ping_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
            ping_sock.settimeout(2)
            ping_sock.bind((addrinfo.ip, 0))
            try:
                yield ping_sock.sendto(ping_msg, (loc.addr, udp_port))
            except Exception:
                pass
            ping_sock.close()
        raise StopIteration(0)

    def discover_peers(self, port=None):
        """This method can be invoked (periodically?) to broadcast message to
        discover peers, if there is a chance initial broadcast message may be
        lost (as these messages are sent over UDP).
        """

        if self._ignore_peers:
            return

        ping_msg = {'signature': self._signature, 'name': self._name, 'version': __version__}

        def _discover(addrinfo, port, task=None):
            ping_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
            ping_sock.settimeout(2)
            ttl_bin = struct.pack('@i', 1)
            if addrinfo.family == socket.AF_INET:
                if self.ipv4_udp_multicast:
                    ping_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl_bin)
                else:
                    ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            else:  # addrinfo.family == socket.AF_INET6
                ping_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS, ttl_bin)
                try:
                    ping_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_IF,
                                         addrinfo.ifn)
                except Exception:
                    pass
            if not port:
                port = addrinfo.udp_sock.getsockname()[1]
            ping_msg['location'] = addrinfo.location
            ping_sock.bind((addrinfo.ip, 0))
            try:
                yield ping_sock.sendto('ping:'.encode() + serialize(ping_msg),
                                       (addrinfo.broadcast, port))
            except Exception:
                pass
            ping_sock.close()

        for addrinfo in self._addrinfos:
            SysTask(_discover, addrinfo, port)

    @property
    def ignore_peers(self):
        return self._ignore_peers

    @ignore_peers.setter
    def ignore_peers(self, ignore):
        """Don't respond to 'ping' from peers if 'ignore=True'. This can be used
        during shutdown, or to limit peers to communicate.
        """
        self._ignore_peers = bool(ignore)

    def peer_status(self, task):
        """This method can be used to be notified of status of peers (other
        Pycos's to communicate for distributed programming). The status
        notifications are sent as messages to the regisered task. Each message
        is an instance of PeerStatus.
        """
        _Peer.peer_status(task)

    def peers(self):
        """Returns list of current peers (as Location instances).
        """
        return _Peer.get_peers()

    def close_peer(self, location, timeout=MsgTimeout):
        """Must be used with 'yield', as
        'yield scheduler.close_peer("loc")'.

        Close peer at 'location'.
        """
        if isinstance(location, Location):
            _Peer._lock.acquire()
            peer = _Peer.peers.get((location.addr, location.port), None)
            _Peer._lock.release()
            if peer:

                def sys_proc(peer, timeout, done, task=None):
                    yield _Peer.close_peer(peer, timeout=timeout, task=task)
                    done.set()

                event = Event()
                SysTask(sys_proc, peer, timeout, event)
                yield event.wait()

    def send_file(self, location, file, dir=None, overwrite=False, timeout=MsgTimeout):
        """Must be used with 'yield' as
        'val = yield scheduler.send_file(location, "file1")'.

        Transfer 'file' to peer at 'location'. If 'dir' is not None, it must be
        a relative path (not absolute path), in which case, file will be saved
        at peer's dest_path + dir. Returns -1 in case of error, 0 if the file is
        transferred, 1 if the same file is already at the destination with same
        size, timestamp and permissions (so file is not transferred) and os.stat
        structure if a file with same name is at the destination with different
        size/timestamp/permissions, but 'overwrite' is False. 'timeout' is max
        seconds to transfer 1MB of data. If return value is 0, the sender may
        want to delete file with 'del_file' later.
        """
        try:
            stat_buf = os.stat(file)
        except Exception:
            logger.warning('send_file: File "%s" is not valid', file)
            raise StopIteration(-1)
        if not ((stat.S_IMODE(stat_buf.st_mode) & stat.S_IREAD) and stat.S_ISREG(stat_buf.st_mode)):
            logger.warning('send_file: File "%s" is not valid', file)
            raise StopIteration(-1)
        if dir:
            if not isinstance(dir, str):
                logger.warning('send_file: path for dir "%s" is not allowed', dir)
                raise StopIteration(-1)
            dir = dir.strip()
            # reject absolute path for dir
            if os.path.join(os.sep, dir) == dir:
                logger.warning('send_file: Absolute path for dir "%s" is not allowed', dir)
                raise StopIteration(-1)
        else:
            dir = None
        peer = _Peer.get_peer(location)
        if peer is None:
            logger.debug('%s is not a valid peer', location)
            raise StopIteration(-1)
        kwargs = {'file': os.path.basename(file), 'stat_buf': stat_buf,
                  'overwrite': overwrite is True, 'dir': dir, 'sep': os.sep}
        req = _NetRequest('send_file', kwargs=kwargs, dst=location, timeout=timeout)
        addrinfo = self._ip_addrinfo_(location.addr)
        sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                           keyfile=self._keyfile, certfile=self._certfile)
        if timeout:
            sock.settimeout(timeout)
        fd = open(file, 'rb')
        try:
            yield sock.connect((location.addr, location.port))
            req.auth = peer.auth
            yield sock.send_msg(serialize(req))
            recvd = yield sock.recv_msg()
            recvd = deserialize(recvd)
            sent = 0
            while sent == recvd:
                data = fd.read(1024000)
                if not data:
                    break
                yield sock.sendall(data)
                sent += len(data)
                recvd = yield sock.recv_msg()
                recvd = deserialize(recvd)
            if recvd == stat_buf.st_size:
                reply = 0
            else:
                reply = -1
        except socket.error as exc:
            reply = -1
            logger.debug('could not send "%s" to %s', req.name, location)
            if len(exc.args) == 1 and exc.args[0] == 'hangup':
                logger.warning('peer "%s" not reachable', location)
                # TODO: remove peer?
        except Exception:
            logger.warning('send_file: Could not send "%s" to %s', file, location)
            reply = -1
        finally:
            sock.close()
            fd.close()
        raise StopIteration(reply)

    def del_file(self, location, file, dir=None, timeout=None):
        """Must be used with 'yield' as
        'loc = yield scheduler.del_file(location, "file1")'.

        Delete 'file' from peer at 'location'. 'dir' must be same as that used
        for 'send_file'.
        """
        if isinstance(dir, str) and dir:
            dir = dir.strip()
            # reject absolute path for dir
            if os.path.join(os.sep, dir) == dir:
                raise StopIteration(-1)
        kwargs = {'file': os.path.basename(file), 'dir': dir}
        req = _NetRequest('del_file', kwargs=kwargs, dst=location, timeout=timeout)
        reply = yield _Peer.sync_reply(req, alarm_value=-1)
        raise StopIteration(reply)

    @staticmethod
    def host_ipaddr(host):
        try:
            info = socket.getaddrinfo(host, None)[0]
        except Exception:
            return None
        ip_addr = info[4][0]
        if info[0] == socket.AF_INET6:
            # canonicalize so different platforms resolve to same string
            ip_addr = ip_addr.split('%')[0]
            ip_addr = re.sub(r'^0+', '', ip_addr)
            ip_addr = re.sub(r':0+', ':', ip_addr)
            ip_addr = re.sub(r'::+', '::', ip_addr)
        return ip_addr

    @staticmethod
    def host_addrinfo(host=None, socket_family=None, ipv4_multicast=False):
        """If 'host' is given (as either host name or IP address), resolve it and
        fill AddrInfo structure. If 'host' is not given, netifaces module is used to
        find appropriate IP address. If 'socket_family' is given, IP address with that
        'socket_family' is used. It should be either 'socket.AF_INET' (for IPv4) or
        'socket.AF_INET6' (for IPv6).
        """

        class AddrInfo(object):
            def __init__(self, family, ip, ifn, broadcast, netmask):
                self.family = family
                self.ip = ip
                self.ifn = ifn
                if family == socket.AF_INET and ipv4_multicast:
                    self.broadcast = pycos.config.IPV4_MULTICAST_GROUP
                else:
                    self.broadcast = broadcast
                self.netmask = netmask
                self.ext_ip_addr = None
                if os.name == 'nt':
                    self.bind_addr = ip
                elif platform.system() in ('Darwin', 'DragonFlyBSD', 'FreeBSD', 'OpenBSD', 'NetBSD'):
                    if family == socket.AF_INET and (not ipv4_multicast):
                        self.bind_addr = ''
                    else:
                        self.bind_addr = self.broadcast
                else:
                    self.bind_addr = self.broadcast

        if socket_family not in (None, socket.AF_INET, socket.AF_INET6):
            return None
        hosts = []
        if host:
            best = None
            for addr in socket.getaddrinfo(host, None):
                if socket_family and addr[0] != socket_family:
                    continue
                if not best or addr[0] == socket.AF_INET:
                    best = addr
            if best:
                socket_family = best[0]
                if best[0] == socket.AF_INET6:
                    addr = Pycos.host_ipaddr(best[-1][0])
                else:
                    addr = best[-1][0]
                hosts.append(addr)
            else:
                return None

        if socket_family:
            socket_families = [socket_family]
        else:
            socket_families = [socket.AF_INET, socket.AF_INET6]

        addrinfos = []
        if netifaces:
            for iface in netifaces.interfaces():
                ifn = 0
                iface_infos = []
                for sock_family in socket_families:
                    for link in netifaces.ifaddresses(iface).get(sock_family, []):
                        netmask = link.get('netmask', None)
                        if sock_family == socket.AF_INET:
                            addr = str(link['addr'])
                            broadcast = link.get('broadcast', '<broadcast>')
                            # Windows seems to have broadcast same as addr
                            if broadcast.startswith(addr):
                                broadcast = '<broadcast>'
                            try:
                                addrs = socket.getaddrinfo(addr, None, sock_family,
                                                           socket.SOCK_STREAM)
                            except Exception:
                                addrs = []
                            for addr in addrs:
                                if hosts and addr[-1][0] not in hosts:
                                    continue
                                addrinfo = AddrInfo(sock_family, addr[-1][0], addr[-1][-1],
                                                    broadcast, netmask)
                                iface_infos.append(addrinfo)
                        else:  # sock_family == socket.AF_INET6
                            addr = str(link['addr'])
                            broadcast = link.get('broadcast', pycos.config.IPV6_MULTICAST_GROUP)
                            if broadcast.startswith(addr):
                                broadcast = pycos.config.IPV6_MULTICAST_GROUP
                            if_sfx = ['']
                            if not ifn and ('%' not in addr.split(':')[-1]):
                                if_sfx.append('%' + iface)
                            for sfx in if_sfx:
                                if ifn and sfx:
                                    break
                                try:
                                    addrs = socket.getaddrinfo(addr + sfx, None, sock_family,
                                                               socket.SOCK_STREAM)
                                except Exception:
                                    continue
                                for addr in addrs:
                                    if addr[-1][-1]:
                                        if ifn and addr[-1][-1] != ifn:
                                            logger.warning('inconsistent scope IDs for %s: %s != %s',
                                                           iface, ifn, addr[-1][-1])
                                        ifn = addr[-1][-1]
                                    if sfx:
                                        continue
                                    addr = Pycos.host_ipaddr(addr[-1][0])
                                    if hosts and addr not in hosts:
                                        continue
                                    addrinfo = AddrInfo(sock_family, addr, ifn, broadcast, netmask)
                                    iface_infos.append(addrinfo)
                            if ifn:
                                for addrinfo in iface_infos:
                                    if not addrinfo.ifn:
                                        addrinfo.ifn = ifn
                addrinfos.extend(iface_infos)

        else:
            if not host:
                host = socket.gethostname()
            netmask = None
            for sock_family in socket_families:
                try:
                    addrs = socket.getaddrinfo(host, None, sock_family, socket.SOCK_STREAM)
                except Exception:
                    continue
                for addr in addrs:
                    ifn = addr[-1][-1]
                    if sock_family == socket.AF_INET:
                        broadcast = '<broadcast>'
                        addr = addr[-1][0]
                    else:  # sock_family == socket.AF_INET6
                        addr = Pycos.host_ipaddr(addr[-1][0])
                        broadcast = pycos.config.IPV6_MULTICAST_GROUP
                        logger.warning('IPv6 may not work without "netifaces" package!')
                    addrinfo = AddrInfo(sock_family, addr, ifn, broadcast, netmask)
                    if hosts:
                        if addrinfo.ip in hosts:
                            return addrinfo
                        else:
                            continue
                    addrinfos.append(addrinfo)

        best = None
        for sock_family in socket_families:
            for addrinfo in addrinfos:
                if addrinfo.ip in hosts:
                    return addrinfo
                if addrinfo.family != sock_family:
                    continue
                if addrinfo.family == socket.AF_INET:
                    if not best or (len(best.ip) < len(addrinfo.ip)) or best.ip.startswith('127'):
                        best = addrinfo
                else:
                    if addrinfo.ip.startswith('fd'):
                        # TODO: How to detect / avoid temporary addresses (privacy extensions)?
                        if addrinfo.ifn:
                            return addrinfo
                    if not best or (len(best.ip) < len(addrinfo.ip)) or best.ip.startswith('fe80:'):
                        best = addrinfo
            if best and best.family == socket.AF_INET and (not best.ip.startswith('127')):
                return best
        return best

    # TODO: is there a better approach to find best suitable address to select
    # (with netmask)?
    def _ip_addrinfo_(self, ip):
        """
        Internal use only.
        """
        best = (0, self._addrinfos[0])
        for addrinfo in self._addrinfos:
            i, n = 0, min(len(ip), len(addrinfo.ip))
            while i < n and ip[i] == addrinfo.ip[i]:
                i += 1
            if i > best[0]:
                best = (i, addrinfo)
        return best[1]

    def _acquaint_(self, ping_info, addrinfo, task=None):
        """
        Internal use only.
        """
        if self._ignore_peers:
            raise StopIteration(-1)
        if ping_info.pop('relay', None):
            SysTask(self._relay_ping_, ping_info, addrinfo)

        peer_location = ping_info.get('location', None)
        if not isinstance(peer_location, Location):
            raise StopIteration(-1)
        peer_signature = ping_info['signature']
        if peer_location in self._locations or peer_signature in _Peer._sign_locations:
            raise StopIteration(0)
        _Peer._lock.acquire()
        peer = _Peer.peers.get((peer_location.addr, peer_location.port), None)
        _Peer._lock.release()
        if peer:
            if self._secret is None:
                auth_code = None
            else:
                auth_code = peer_signature + self._secret
                auth_code = hashlib.sha1(auth_code.encode()).hexdigest()
            if peer.auth == auth_code:
                raise StopIteration(0)
            _Peer.remove(peer_location)

        sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                           keyfile=self._keyfile, certfile=self._certfile)
        sock.settimeout(MsgTimeout)
        req = _NetRequest('peer', kwargs={'signature': self._signature, 'name': self._name,
                                          'from': addrinfo.location, 'version': __version__},
                          dst=peer_location)
        req.auth = hashlib.sha1((peer_signature + self._secret).encode()).hexdigest()
        try:
            yield sock.connect((peer_location.addr, peer_location.port))
            yield sock.send_msg(serialize(req))
            peer_info = yield sock.recv_msg()
            peer_info = deserialize(peer_info)
            assert peer_info['version'] == __version__
            if peer_signature not in _Peer._sign_locations:
                _Peer(peer_info['name'], peer_location, peer_signature,
                      self._keyfile, self._certfile, addrinfo)
            reply = 0
        except Exception:
            logger.debug(traceback.format_exc())
            reply = -1
        sock.close()
        raise StopIteration(reply)

    def _relay_ping_(self, ping_msg, addrinfo, task=None):
        """
        Internal use only.
        """
        ping_msg = 'ping:'.encode() + serialize(ping_msg)
        port = addrinfo.udp_sock.getsockname()[1]
        ping_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
        ping_sock.settimeout(2)
        ttl_bin = struct.pack('@i', 1)
        if addrinfo.family == socket.AF_INET:
            if self.ipv4_udp_multicast:
                ping_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl_bin)
                # ping_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 0)
            else:
                ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        else:  # addrinfo.family == socket.AF_INET6
            ping_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS, ttl_bin)
            # ping_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_LOOP, 0)
            try:
                ping_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_IF,
                                     addrinfo.ifn)
            except Exception:
                pass
        ping_sock.bind((addrinfo.ip, 0))
        try:
            yield ping_sock.sendto(ping_msg, (addrinfo.broadcast, port))
        except Exception:
            pass
        finally:
            ping_sock.close()

    def _udp_proc(self, location, addrinfo, task=None):
        """
        Internal use only.
        """
        task.set_daemon()
        addrinfo.udp_sock = AsyncSocket(addrinfo.udp_sock)
        sock = addrinfo.udp_sock
        while 1:
            try:
                msg, addr = yield sock.recvfrom(1024)
            except GeneratorExit:
                break
            if not msg.startswith(b'ping:'):
                logger.warning('ignoring UDP message from %s:%s', addr[0], addr[1])
                continue
            try:
                ping_info = deserialize(msg[len(b'ping:'):])
                if ping_info.get('version') != __version__:
                    logger.warning('Ignoring ping from %s:%s due to version mismatch (%s / %s)',
                                   addr[0], addr[1], ping_info.get('version'), __version__)
                    continue
            except Exception:
                logger.warning('Ignoring invalid ping from %s:%s', addr[0], addr[1])
                continue
            SysTask(self._acquaint_, ping_info, addrinfo)
        sock.close()

    def _tcp_proc(self, addrinfo, task=None):
        """
        Internal use only.
        """
        task.set_daemon()
        addrinfo.tcp_sock = AsyncSocket(addrinfo.tcp_sock,
                                        keyfile=self._keyfile, certfile=self._certfile)
        sock = addrinfo.tcp_sock
        while 1:
            try:
                conn, addr = yield sock.accept()
            except ssl.SSLError as err:
                logger.debug('SSL connection failed: %s', str(err))
                continue
            except GeneratorExit:
                break
            except Exception:
                logger.debug(traceback.format_exc())
                continue
            SysTask(self._tcp_conn_proc, conn, addrinfo)
        sock.close()

    def _tcp_conn_proc(self, conn, addrinfo, task=None):
        """
        Internal use only.
        """
        while 1:
            try:
                msg = yield conn.recv_msg()
            except Exception:
                break
            if not msg:
                break
            try:
                req = deserialize(msg)
            except Exception:
                logger.debug('%s ignoring invalid message', addrinfo.location)
                break
            if req.auth != self._auth_code:
                if req.name == 'signature':
                    yield conn.send_msg(self._signature.encode())
                else:
                    logger.warning('invalid request "%s" ignored', req.name)
                break

            # if req.dst and req.dst != addrinfo.location:
            #     logger.debug('invalid request "%s" to %s (%s)',
            #                  req.name, req.dst, addrinfo.location)
            #      break

            if req.name == 'send':
                reply = -1
                task = req.kwargs.get('task', None)
                if task:
                    name = req.kwargs.get('name', ' ')
                    if name[0] == '^':
                        task = self._tasks.get(int(task))
                        if task and task._rid == req.kwargs.get('rid') and task._name == name:
                            reply = task.send(req.kwargs['message'])
                        else:
                            logger.warning('ignoring invalid recipient to "send"')
                    else:
                        Task._pycos._lock.acquire()
                        task = Task._pycos._tasks.get(int(task), None)
                        Task._pycos._lock.release()
                        if task and task._rid == req.kwargs.get('rid') and task._name == name:
                            reply = task.send(req.kwargs['message'])
                        else:
                            logger.warning('ignoring invalid recipient to "send"')
                else:
                    channel = req.kwargs.get('channel', None)
                    Channel._pycos._lock.acquire()
                    channel = Channel._pycos._channels.get(channel)
                    Channel._pycos._lock.release()
                    if (channel and channel._id == req.kwargs.get('id') and
                        channel._rid == req.kwargs.get('rid')):
                        reply = channel.send(req.kwargs['message'])
                    else:
                        logger.warning('ignoring invalid recipient to "send"')
                yield conn.send_msg(serialize(reply))

            elif req.name == 'deliver':
                task = req.kwargs.get('task', None)
                if task:
                    reply = -1
                    name = req.kwargs.get('name', ' ')
                    if name[0] == '^':
                        task = self._tasks.get(int(task))
                        if (task and task._rid == req.kwargs.get('rid') and task._name == name and
                            task.send(req.kwargs['message']) == 0):
                            reply = 1
                        else:
                            logger.warning('invalid "deliver" message ignored')
                    else:
                        Task._pycos._lock.acquire()
                        task = Task._pycos._tasks.get(int(task))
                        Task._pycos._lock.release()
                        if (task and task._rid == req.kwargs.get('rid') and task._name == name and
                            task.send(req.kwargs['message']) == 0):
                            reply = 1
                    yield conn.send_msg(serialize(reply))
                else:
                    reply = -1
                    channel = req.kwargs.get('channel')
                    if channel:
                        def async_reply(req, task=None):
                            reply = yield channel.deliver(
                                req.kwargs['message'], timeout=req.timeout, n=req.kwargs['n'])
                            req.name += '-async_reply'
                            req.dst = req.kwargs['reply_location']
                            req.kwargs = {'reply_id': req.kwargs['reply_id'],
                                          'reply_rid': req.kwargs['reply_rid'], 'reply': reply}
                            req.reply = False
                            _Peer.send_req(req)

                        Channel._pycos._lock.acquire()
                        channel = Channel._pycos._channels.get(channel)
                        Channel._pycos._lock.release()
                        if (channel and channel._id == req.kwargs.get('id') and
                            channel._rid == req.kwargs.get('rid')):
                            reply = None
                            SysTask(async_reply, req)
                    if reply == -1:
                        logger.warning('invalid "deliver" message ignored')
                        req.name += '-async_reply'
                        req.dst = req.kwargs.get('reply_location')
                        _Peer.send_req(req)

            elif req.name.endswith('-async_reply'):
                reply = req
                self._lock.acquire()
                req = self._pending_replies.pop(reply.kwargs.get('reply_id'))
                self._lock.release()
                if (req and req.event and reply.name == (req.name + '-async_reply') and
                    req.kwargs['reply_rid'] == reply.kwargs.get('reply_rid')):
                    req.reply = reply.kwargs['reply']
                    req.event.set()
                else:
                    logger.warning('Ignoring invalid reply for %s' % req.name)

            elif req.name == 'run_rps':
                rps = self._rpss.get(req.kwargs['name'], None)
                if rps:
                    args = req.kwargs['args']
                    kwargs = req.kwargs['kwargs']
                    try:
                        monitor = req.kwargs.get('monitor', None)
                        if monitor:
                            Task._pycos._lock.acquire()
                            reply = Task(rps._method, *args, **kwargs)
                            if reply:
                                reply.notify(monitor)
                            Task._pycos._lock.release()
                        else:
                            reply = Task(rps._method, *args, **kwargs)
                    except Exception:
                        reply = Exception(traceback.format_exc())
                else:
                    reply = Exception('RPS "%s" is not registered' % req.kwargs['name'])
                yield conn.send_msg(serialize(reply))

            elif req.name == 'locate_task':
                name = req.kwargs.get('name', ' ')
                if name[0] == '^':
                    task = self._rtasks.get(name, None)
                else:
                    Task._pycos._lock.acquire()
                    task = Task._pycos._rtasks.get(name, None)
                    Task._pycos._lock.release()
                yield conn.send_msg(serialize(task))

            elif req.name == 'locate_channel':
                name = req.kwargs.get('name')
                Channel._pycos._lock.acquire()
                channel = Channel._pycos._rchannels.get(name, None)
                Channel._pycos._lock.release()
                yield conn.send_msg(serialize(channel))

            elif req.name == 'locate_rps':
                rps = self._rpss.get(req.kwargs['name'], None)
                yield conn.send_msg(serialize(rps))

            elif req.name == 'monitor':
                reply = -1
                monitor = req.kwargs.get('monitor', None)
                task = req.kwargs.get('task', None)
                name = req.kwargs.get('name', None)
                if task and name:
                    if name == '^':
                        task = self._tasks.get(int(task), None)
                        if task and task._rid == req.kwargs.get('rid') and task._name == name:
                            reply = self._monitor(monitor, task)
                    else:
                        Task._pycos._lock.acquire()
                        task = Task._pycos._tasks.get(int(task), None)
                        if task and task._rid == req.kwargs.get('rid') and task._name == name:
                            reply = Task._pycos._monitor(monitor, task)
                        Task._pycos._lock.release()
                yield conn.send_msg(serialize(reply))

            elif req.name == 'terminate_task':
                reply = -1
                task = req.kwargs.get('task', None)
                name = req.kwargs.get('name', None)
                if task and name:
                    if name[0] == '^':
                        task = self._tasks.get(int(task), None)
                    else:
                        Task._pycos._lock.acquire()
                        task = Task._pycos._tasks.get(int(task), None)
                        Task._pycos._lock.release()
                    if task and task._rid == req.kwargs.get('rid') and task._name == name:
                        reply = task.terminate()
                yield conn.send_msg(serialize(reply))

            elif req.name == 'subscribe':
                reply = -1
                channel = req.kwargs.get('channel')
                Channel._pycos._lock.acquire()
                channel = Channel._pycos._channels.get(channel, None)
                Channel._pycos._lock.release()
                if (channel and (not channel._location) and channel._id == req.kwargs.get('id') and
                    channel._rid == req.kwargs.get('rid')):
                    subscriber = req.kwargs.get('subscriber', None)
                    if isinstance(subscriber, Task):
                        if not subscriber._location:
                            Task._pycos._lock.acquire()
                            task = Task._pycos._tasks.get(int(subscriber._id), None)
                            Task._pycos._lock.release()
                            if task and task._rid == subscriber._rid:
                                reply = yield channel.subscribe(task)
                        else:
                            reply = yield channel.subscribe(subscriber)
                    elif isinstance(subscriber, Channel):
                        if not subscriber._location:
                            Channel._pycos._lock.acquire()
                            sub_chan = self._channels.get(subscriber._name, None)
                            Channel._pycos._lock.release()
                            if sub_chan and sub_chan._rid == subscriber._rid:
                                reply = yield channel.subscribe(sub_chan)
                        else:
                            reply = yield channel.subscribe(subscriber)
                yield conn.send_msg(serialize(reply))

            elif req.name == 'unsubscribe':
                reply = -1
                channel = req.kwargs.get('channel')
                Channel._pycos._lock.acquire()
                channel = Channel._pycos._channels.get(channel, None)
                Channel._pycos._lock.release()
                if (channel and (not channel._location) and channel._id == req.kwargs.get('id') and
                    channel._rid == req.kwargs.get('rid')):
                    subscriber = req.kwargs.get('subscriber', None)
                    if isinstance(subscriber, Task):
                        if not subscriber._location:
                            Task._pycos._lock.acquire()
                            task = Task._pycos._tasks.get(int(subscriber._id), None)
                            Task._pycos._lock.release()
                            if task and task._rid == subscriber._rid:
                                reply = yield channel.unsubscribe(task)
                        else:
                            reply = yield channel.unsubscribe(subscriber)
                    elif isinstance(subscriber, Channel):
                        if not subscriber._location:
                            Channel._pycos._lock.acquire()
                            sub_chan = self._channels.get(subscriber._name, None)
                            Channel._pycos._lock.release()
                            if sub_chan and sub_chan._rid == subscriber._rid:
                                reply = yield channel.unsubscribe(sub_chan)
                        else:
                            reply = yield channel.unsubscribe(subscriber)
                yield conn.send_msg(serialize(reply))

            elif req.name == 'locate_peer':
                if req.kwargs['name'] == self._name:
                    loc = addrinfo.location
                elif req.dst == addrinfo.location:
                    loc = None
                else:
                    loc = None
                yield conn.send_msg(serialize(loc))

            elif req.name == 'send_file':
                sep = req.kwargs['sep']
                tgt = req.kwargs['file'].split(sep)[-1]
                if req.kwargs['dir']:
                    dir = os.path.join(*(req.kwargs['dir'].split(sep)))
                    if dir:
                        tgt = os.path.join(dir, tgt)
                tgt = os.path.abspath(os.path.join(self.__dest_path, tgt))
                stat_buf = req.kwargs['stat_buf']
                resp = 0
                if self.max_file_size and stat_buf.st_size > self.max_file_size:
                    logger.warning('file "%s" too big (%s) - must be smaller than %s',
                                   req.kwargs['file'], stat_buf.st_size, self.max_file_size)
                    resp = -1
                elif not tgt.startswith(self.__dest_path):
                    resp = -1
                elif os.path.isfile(tgt):
                    sbuf = os.stat(tgt)
                    if abs(stat_buf.st_mtime - sbuf.st_mtime) <= 1 and \
                       stat_buf.st_size == sbuf.st_size and \
                       stat.S_IMODE(stat_buf.st_mode) == stat.S_IMODE(sbuf.st_mode):
                        resp = stat_buf.st_size
                    elif not req.kwargs['overwrite']:
                        resp = -1

                if resp == 0:
                    try:
                        if not os.path.isdir(os.path.dirname(tgt)):
                            os.makedirs(os.path.dirname(tgt))
                        fd = open(tgt, 'wb')
                    except Exception:
                        logger.debug('failed to create "%s" : %s', tgt, traceback.format_exc())
                        resp = -1
                if resp == 0:
                    recvd = 0
                    try:
                        while recvd < stat_buf.st_size:
                            yield conn.send_msg(serialize(recvd))
                            data = yield conn.recvall(min(stat_buf.st_size-recvd, 1024000))
                            if not data:
                                break
                            fd.write(data)
                            recvd += len(data)
                    except Exception:
                        logger.warning('copying file "%s" failed', tgt)
                    finally:
                        fd.close()
                    if recvd == stat_buf.st_size:
                        os.utime(tgt, (stat_buf.st_atime, stat_buf.st_mtime))
                        os.chmod(tgt, stat.S_IMODE(stat_buf.st_mode))
                        resp = recvd
                    else:
                        os.remove(tgt)
                        resp = -1
                yield conn.send_msg(serialize(resp))

            elif req.name == 'del_file':
                tgt = os.path.basename(req.kwargs['file'])
                dir = req.kwargs['dir']
                if isinstance(dir, str) and dir:
                    tgt = os.path.join(dir, tgt)
                tgt = os.path.join(self.__dest_path, tgt)
                if tgt.startswith(self.__dest_path) and os.path.isfile(tgt):
                    os.remove(tgt)
                    d = os.path.dirname(tgt)
                    try:
                        while d > self.__dest_path and os.path.isdir(d):
                            os.rmdir(d)
                            d = os.path.dirname(d)
                    except Exception:
                        # logger.debug(traceback.format_exc())
                        pass
                    reply = 0
                else:
                    reply = -1
                yield conn.send_msg(serialize(reply))

            elif req.name == 'peer':
                if req.kwargs.get('version', None) != __version__:
                    logger.debug('Ignoring peer due to version mismatch: %s != %s',
                                 req.kwargs.get('version', None), __version__)
                    yield conn.send_msg(serialize(-1))
                    break
                yield conn.send_msg(serialize({'version': __version__, 'name': self._name}))
                _Peer._lock.acquire()
                peer = _Peer.peers.get((req.kwargs['from'].addr, req.kwargs['from'].port), None)
                if peer:
                    if req.kwargs['signature'] not in _Peer._sign_locations:
                        _Peer._sign_locations[req.kwargs['signature']] = req.kwargs['from']
                        peer.signature = req.kwargs['signature']
                        peer.auth = hashlib.sha1((req.kwargs['signature'] +
                                                  _Peer._pycos._secret).encode()).hexdigest()
                    _Peer._lock.release()
                    pycos.logger.info('%s: rediscovered peer %s',
                                      addrinfo.location, req.kwargs['from'])
                    msg = PeerStatus(req.kwargs['from'], req.kwargs['name'], PeerStatus.Online)
                    drop = []
                    for tsk in _Peer.status_tasks:
                        if tsk.send(msg):
                            drop.append(tsk)
                    if drop:
                        for tsk in drop:
                            _Peer.status_tasks.discard(tsk)
                else:
                    _Peer._lock.release()
                    _Peer(req.kwargs['name'], req.kwargs['from'], req.kwargs['signature'],
                          self._keyfile, self._certfile, addrinfo)

            elif req.name == 'close_peer':
                peer_loc = req.kwargs.get('location', None)
                if peer_loc:
                    # TODO: remove from _stream_peers?
                    # Pycos._pycos._stream_peers.pop((peer_loc.addr, peer_loc.port))
                    _Peer.remove(peer_loc)
                yield conn.send_msg(serialize(0))
                break

            elif req.name == 'acquaint':
                if req.kwargs.get('version', None) != __version__:
                    logger.debug('Ignoring peer due to version mismatch: %s != %s',
                                 req.kwargs.get('version', None), __version__)
                    yield conn.send_msg(serialize(-1))
                    break
                SysTask(self._acquaint_, req.kwargs, addrinfo)
                yield conn.send_msg(serialize(0))

            elif req.name == 'relay_ping':
                yield conn.send_msg(serialize(0))
                SysTask(self._relay_ping_, req.kwargs, addrinfo)

            else:
                logger.warning('invalid request "%s" ignored', req.name)

        conn.close()

    def __repr__(self):
        s = ', '.join(str(s) for s in self._locations)
        if s == self._name:
            return s
        else:
            return '"%s" @ %s' % (self._name, s)


class RPS(object):
    """Remote Pico/Pycos Service.

    Methods registered with RPS can be executed as tasks on request (by remote clients).
    """

    __slots__ = ('_name', '_location', '_method', '_monitor_task', '_monitors', '_rtasks')

    _pycos = None
    _sign = None

    def __init__(self, method, name=None):
        """'method' must be generator method; this is used to create tasks. If
        'name' is not given, method's function name is used for registering.
        """
        if not inspect.isgeneratorfunction(method):
            raise RuntimeError('RPS method must be generator function')
        self._method = method
        if name:
            self._name = name
        else:
            self._name = method.__name__
        if not RPS._pycos:
            Pycos.instance()
        self._location = None
        self._monitor_task = None
        self._monitors = set()
        self._rtasks = {}

    @property
    def location(self):
        """Get Location instance where this RPS is running.
        """
        if isinstance(self._location, Location):
            return copy.copy(self._location)
        else:
            return None

    @property
    def name(self):
        """Get name of RPS.
        """
        return self._name

    @staticmethod
    def locate(name, location=None, timeout=None):
        """Must be used with 'yield' as 'rps = yield RPS.locate("name")'.

        Returns RPS instance to registered RPS at a remote peer so its method
        can be used to execute tasks at that peer.

        If 'location' is given, RPS is looked up at that specific peer;
        otherwise, all known peers are queried for given name.
        """
        if not RPS._pycos:
            Pycos.instance()
        req = _NetRequest('locate_rps', kwargs={'name': name}, dst=location, timeout=timeout)
        rps = yield _Peer.async_request(req)
        raise StopIteration(rps)

    def register(self):
        """RPS must be registered so it can be located.
        """
        if self._location:
            return -1
        if not inspect.isgeneratorfunction(self._method):
            return -1
        RPS._pycos._lock.acquire()
        if RPS._pycos._rpss.get(self._name, None) is None:
            RPS._pycos._rpss[self._name] = self
            RPS._pycos._lock.release()
            return 0
        else:
            RPS._pycos._lock.release()
            return -1

    def unregister(self):
        """Unregister registered RPS; see 'register' above.
        """
        if self._location:
            return -1
        RPS._pycos._lock.acquire()
        if RPS._pycos._rpss.pop(self._name, None) is None:
            RPS._pycos._lock.release()
            return -1
        else:
            RPS._pycos._lock.release()
            return 0

    def monitor(self, task):
        """Install 'task' (a Task instance) as monitor for tasks created; i.e.,
        'task' receives MonitorStatus messages. If call is successful, the
        result is 0.
        """
        if not isinstance(task, Task):
            return -1
        self._monitors.add(task)
        return 0

    def close(self):
        """Clients should use this method after an rps (obtained with 'RPS.locate')
        is no longer needed. After this call, this RPS can't be used to create
        tasks. The result of this method is 0 in case of success.
        """
        if (not self._name) or (not self._location):
            return -1
        for tsk in self._rtasks:
            tsk.terminate()
        self._rtasks.clear()
        self._name = None
        if self._monitor_task:
            self._monitor_task.terminate()
            self._monitor_task = None
        self._monitors.clear()
        RPS._pycos.drop_atexit(5, self.close)
        return 0

    def __call__(self, *args, **kwargs):
        """Must be used with 'yeild' as 'rtask = yield rps(*args, **kwargs)'.

        Run RPS (at remote location) with args and kwargs. Both args and
        kwargs must be serializable. Result is (remote) Task instance if call
        succeeds, otherwise it is None.
        """
        if not self._monitor_task and isinstance(self._location, Location):
            self._monitor_task = Task(self._monitor_proc)
        req = _NetRequest('run_rps', kwargs={'name': self._name, 'args': args, 'kwargs': kwargs,
                                             'monitor': self._monitor_task},
                          dst=self._location, timeout=MsgTimeout)
        rtask = yield _Peer.sync_reply(req)
        if isinstance(rtask, Task):
            self._rtasks[rtask] = rtask
            setattr(rtask, '_value', None)
            setattr(rtask, '_complete', pycos.Event())
            rtask._complete.clear()
            raise StopIteration(rtask)
        else:
            if isinstance(rtask, Exception):
                logger.warning('RPS call failed: %s', rtask)
            raise StopIteration(None)

    def _monitor_proc(self, task=None):
        """Internal use only.
        """
        task.set_daemon()
        while 1:
            msg = yield task.recv()
            if not isinstance(msg, MonitorStatus):
                logger.warning('RPS: invalid MonitorStatus message ignored: %s', type(msg))
                continue
            if not isinstance(msg.info, Task):
                if isinstance(msg.info, str) and isinstance(msg.value, str):
                    logger.info('RPS: %s: %s with %s', msg.info, msg.type, msg.value)
                else:
                    logger.warning('RPS: invalid MonitorStatus message ignored: %s',
                                   type(msg.info))
                continue
            rtask = self._rtasks.pop(msg.info, None)
            if not rtask:
                for _ in range(10):
                    yield task.sleep(0.1)
                    rtask = self._rtasks.pop(rtask, None)
                    if rtask:
                        break
                else:
                    logger.warning('RPS: rtask %s may not be valid?', rtask)
                    continue
            msg.info = rtask
            if msg.type == StopIteration:
                logger.debug('RPS: rtask %s done: %s', rtask, msg.value)
                rtask._value = msg.value
            else:
                rtask._value = msg
                logger.warning('RPS: rtask %s failed: %s%s', rtask, msg.type,
                               ' with %s' % msg.value if isinstance(msg.value, str) else '')
            rtask._complete.set()
            drop = []
            for monitor in self._monitors:
                if monitor.send(msg):
                    drop.append(monitor)
            if drop:
                for tsk in drop:
                    try:
                        self._monitors.remove(tsk)
                    except KeyError:
                        pass

    def __getstate__(self):
        state = {'name': self._name}
        if self._location:
            state['location'] = self._location
        else:
            state['location'] = RPS._sign
        return state

    def __setstate__(self, state):
        self._name = state['name']
        self._location = state['location']
        if isinstance(self._location, Location):
            if self._location in RPS._pycos._locations:
                self._location = None
        else:
            self._location = _Peer.sign_location(self._location)
            # TODO: is it possible for peer to disconnect during deserialize?
        if isinstance(self._location, Location):
            self._monitors = set()
            self._monitor_task = None
            self._rtasks = {}
            RPS._pycos.atexit(5, self.close)

    def __eq__(self, other):
        return (isinstance(other, RPS) and
                self._name == other._name and self._location == other._location)

    def __ne__(self, other):
        return ((not isinstance(other, RPS)) or
                self._name != other._name or self._location != other._location)

    def __repr__(self):
        s = '%s' % (self._name)
        if self._location:
            s = '%s@%s' % (s, self._location)
        return s

    def __hash__(self):
        return hash(str(self))


class SysTask(pycos.Task):
    """Task meant for reactive components that are always ready to respond to
    events; i.e., takes very little CPU time to process events. Typically such
    tasks process I/O events, timer events etc. and any time consuming
    processing is handed off to Task instances.

    These tasks run in seperate Pycos thread, so if user tasks (Task instances)
    take too much CPU time, SysTask can still respond to such events
    immediately.
    """

    _pycos = None

    def __init__(self, *args, **kwargs):
        if not SysTask._pycos:
            Pycos.instance()
        self._scheduler = SysTask._pycos
        super(SysTask, self).__init__(*args, **kwargs)
        self._name = '^' + self.name

    @staticmethod
    def locate(name, location=None, timeout=None):
        if not SysTask._pycos:
            Pycos.instance()
        raise StopIteration((yield Task._locate('^' + name, location, timeout)))


class _NetRequest(object):
    """Internal use only.
    """

    __slots__ = ('name', 'kwargs', 'dst', 'auth', 'event', 'reply', 'timeout')

    def __init__(self, name, kwargs={}, dst=None, auth=None, timeout=None):
        self.name = name
        self.kwargs = kwargs
        self.dst = dst
        self.auth = auth
        self.event = None
        self.reply = True
        self.timeout = timeout

    def __getstate__(self):
        state = {'name': self.name, 'kwargs': self.kwargs, 'dst': self.dst, 'auth': self.auth,
                 'timeout': self.timeout}
        return state

    def __setstate__(self, state):
        for k, v in state.items():
            setattr(self, k, v)


class _Peer(object):
    """Internal use only.
    """

    __slots__ = ('name', 'location', 'auth', 'keyfile', 'certfile', 'stream', 'conn',
                 'reqs', 'waiting', 'req_task', 'addrinfo', 'signature')

    peers = {}
    status_tasks = set()
    _pycos = None
    _lock = threading.Lock()
    _sign_locations = {}

    def __init__(self, name, location, signature, keyfile, certfile, addrinfo):
        self.name = name
        self.location = location
        self.signature = signature
        self.auth = hashlib.sha1((signature + _Peer._pycos._secret).encode()).hexdigest()
        self.keyfile = keyfile
        self.certfile = certfile
        self.stream = False
        self.conn = None
        self.reqs = collections.deque()
        self.waiting = False
        self.addrinfo = addrinfo
        _Peer._lock.acquire()
        if (location.addr, location.port) in _Peer.peers:
            pycos.logger.debug('Ignoring already known peer %s', location)
            _Peer._lock.release()
            return
        _Peer.peers[(location.addr, location.port)] = self
        _Peer._sign_locations[signature] = location
        _Peer._lock.release()
        self.req_task = SysTask(self.req_proc)

        logger.debug('%s: found peer %s', addrinfo.location, location)
        msg = PeerStatus(location, name, PeerStatus.Online)
        drop = []
        for tsk in _Peer.status_tasks:
            if tsk.send(msg):
                drop.append(tsk)
        if drop:
            for tsk in drop:
                _Peer.status_tasks.discard(tsk)

        _Peer._pycos._lock.acquire()
        if ((location.addr, location.port) in _Peer._pycos._stream_peers or
            (location.addr, 0) in _Peer._pycos._stream_peers):
            self.stream = True

        # send pending (async) requests
        for pending_req in _Peer._pycos._pending_reqs.values():
            if (pending_req.name == 'locate_peer' and pending_req.kwargs['name'] == self.name):
                pending_req.reply = location
                pending_req.event.set()
            elif (not pending_req.dst) or pending_req.dst == location:
                _Peer.send_req(pending_req, dst=location)
        _Peer._pycos._lock.release()

    @staticmethod
    def sign_location(sign):
        _Peer._lock.acquire()
        location = _Peer._sign_locations.get(sign, None)
        _Peer._lock.release()
        return location

    @staticmethod
    def get_peers():
        _Peer._lock.acquire()
        peers = [copy.copy(peer.location) for peer in _Peer.peers.values()]
        _Peer._lock.release()
        return peers

    @staticmethod
    def get_peer(location):
        _Peer._lock.acquire()
        peer = _Peer.peers.get((location.addr, location.port), None)
        _Peer._lock.release()
        return peer

    @staticmethod
    def send_req(req, dst=None):
        if not dst:
            dst = req.dst
        _Peer._lock.acquire()
        peer = _Peer.peers.get((dst.addr, dst.port), None)
        if not peer:
            logger.debug('Ignoring request to invalid peer %s', dst)
            _Peer._lock.release()
            return -1
        peer.reqs.append(req)
        if peer.waiting:
            peer.waiting = False
            peer.req_task.send(1)
        _Peer._lock.release()
        return 0

    @staticmethod
    def sync_reply(req, alarm_value=None):
        req.event = Event()
        if _Peer.send_req(req) != 0:
            raise StopIteration(-1)
        if (yield req.event.wait(req.timeout)) is False:
            raise StopIteration(alarm_value)
        raise StopIteration(req.reply)

    @staticmethod
    def async_request(req, alarm_value=None):
        req.event = Event()
        req_id = id(req)
        _Peer._pycos._lock.acquire()
        _Peer._pycos._pending_reqs[req_id] = req
        _Peer._pycos._lock.release()
        if req.dst:
            _Peer.send_req(req)
        else:
            _Peer._pycos._lock.acquire()
            for peer in _Peer.peers.values():
                _Peer.send_req(req, dst=peer.location)
            _Peer._pycos._lock.release()
        if (yield req.event.wait(req.timeout)) is False:
            req.reply = alarm_value
        _Peer._pycos._lock.acquire()
        _Peer._pycos._pending_reqs.pop(req_id, None)
        _Peer._pycos._lock.release()
        raise StopIteration(req.reply)

    @staticmethod
    def async_reply(req, alarm_value=None):
        req.event = Event()
        req.reply = False
        req_id = id(req)
        _Peer._pycos._lock.acquire()
        req.kwargs['reply_id'] = req_id
        req.kwargs['reply_rid'] = pycos._time()
        _Peer._pycos._pending_replies[req_id] = req
        peer = _Peer.peers.get((req.dst.addr, req.dst.port), None)
        if not peer:
            _Peer._pycos._lock.release()
            raise StopIteration(-1)
        req.kwargs['reply_location'] = peer.addrinfo.location
        peer.reqs.append(req)
        if peer.waiting:
            peer.waiting = False
            peer.req_task.send(1)
        _Peer._pycos._lock.release()
        if (yield req.event.wait(req.timeout)) is False:
            req.reply = alarm_value
            _Peer._pycos._lock.acquire()
            _Peer._pycos._pending_replies.pop(req_id, None)
            _Peer._pycos._lock.release()
        raise StopIteration(req.reply)

    @staticmethod
    def close_peer(peer, timeout, task=None):
        req = _NetRequest('close_peer', kwargs={'location': peer.addrinfo.location},
                          dst=peer.location, timeout=timeout)
        yield _Peer.sync_reply(req)
        if peer.req_task:
            yield peer.req_task.terminate()
            while peer.req_task:
                yield task.sleep(0.1)

    @staticmethod
    def shutdown(timeout=MsgTimeout):
        _Peer._lock.acquire()
        for peer in _Peer.peers.values():
            SysTask(_Peer.close_peer, peer, timeout)
        _Peer._lock.release()

    def req_proc(self, task=None):
        task.set_daemon()
        conn_errors = 0
        req = None
        sock_family = self.addrinfo.family
        while 1:
            _Peer._lock.acquire()
            if self.reqs:
                _Peer._lock.release()
            else:
                self.waiting = True
                _Peer._lock.release()
                if not self.stream and self.conn:
                    self.conn.shutdown(socket.SHUT_WR)
                    self.conn.close()
                    self.conn = None
                try:
                    yield task.receive()
                except GeneratorExit:
                    break
            req = self.reqs.popleft()
            if not self.conn:
                self.conn = AsyncSocket(socket.socket(sock_family, socket.SOCK_STREAM),
                                        keyfile=self.keyfile, certfile=self.certfile)
                if req.timeout:
                    self.conn.settimeout(req.timeout)
                try:
                    yield self.conn.connect((self.location.addr, self.location.port))
                except GeneratorExit:
                    if self.conn:
                        try:
                            self.conn.shutdown(socket.SHUT_WR)
                            self.conn.close()
                        except Exception:
                            pass
                        self.conn = None
                    break
                except Exception:
                    if self.conn:
                        # self.conn.shutdown(socket.SHUT_WR)
                        self.conn.close()
                        self.conn = None
                    req.reply = None
                    if req.event:
                        req.event.set()
                    conn_errors += 1
                    if conn_errors >= pycos.config.MaxConnectionErrors:
                        logger.warning('too many connection errors to %s; removing it',
                                       self.location)
                        break
                    continue
                else:
                    if conn_errors:
                        conn_errors = 0
            else:
                self.conn.settimeout(req.timeout)

            req.auth = self.auth
            try:
                yield self.conn.send_msg(serialize(req))
                if req.reply:
                    req.reply = yield self.conn.recv_msg()
                    req.reply = deserialize(req.reply)
                    if req.event:
                        req.event.set()
            except socket.error as exc:
                logger.debug('%s: Could not send "%s" to %s', _Peer._pycos._location, req.name,
                             self.location)
                # logger.debug(traceback.format_exc())
                if len(exc.args) == 1 and exc.args[0] == 'hangup':
                    logger.warning('peer "%s" not reachable', self.location)
                    # TODO: remove peer?
                try:
                    self.conn.shutdown(socket.SHUT_WR)
                    self.conn.close()
                except Exception:
                    pass
                self.conn = None
                req.reply = None
                if req.event:
                    req.event.set()
            except socket.timeout:
                # logger.debug(traceback.format_exc())
                try:
                    self.conn.shutdown(socket.SHUT_WR)
                    self.conn.close()
                except Exception:
                    pass
                self.conn = None
                req.reply = None
                if req.event:
                    req.event.set()
            except GeneratorExit:
                if self.conn:
                    try:
                        self.conn.shutdown(socket.SHUT_WR)
                        self.conn.close()
                    except Exception:
                        pass
                    self.conn = None
                break
            except Exception:
                # logger.debug(traceback.format_exc())
                if self.conn:
                    try:
                        self.conn.shutdown(socket.SHUT_WR)
                        self.conn.close()
                    except Exception:
                        pass
                    self.conn = None
                req.reply = None
                if req.event:
                    req.event.set()

        if req and isinstance(req.event, Event):
            req.reply = None
            req.event.set()
        for req in self.reqs:
            if isinstance(req.event, Event):
                req.reply = None
                req.event.set()

        self.reqs.clear()
        self.req_task = None
        if self.conn:
            # self.conn.shutdown(socket.SHUT_WR)
            self.conn.close()
            self.conn = None
        _Peer.remove(self.location)
        raise StopIteration(None)

    @staticmethod
    def remove(location):
        _Peer._lock.acquire()
        peer = _Peer.peers.pop((location.addr, location.port), None)
        _Peer._lock.release()
        if peer:
            logger.debug('%s: peer %s terminated', peer.addrinfo.location, peer.location)
            # RPS._peer_closed_(peer.location)
            peer.stream = False
            _Peer._sign_locations.pop(peer.signature, None)
            if peer.req_task:
                peer.req_task.terminate()
            msg = PeerStatus(peer.location, peer.name, PeerStatus.Offline)
            drop = []
            for tsk in _Peer.status_tasks:
                if tsk.send(msg):
                    drop.append(tsk)
            if drop:
                for tsk in drop:
                    _Peer.status_tasks.discard(tsk)

    @staticmethod
    def peer_status(task):
        _Peer._lock.acquire()
        if isinstance(task, Task):
            for peer in _Peer.peers.values():
                try:
                    task.send(PeerStatus(peer.location, peer.name, PeerStatus.Online))
                except Exception:
                    logger.debug(traceback.format_exc())
                    break
            else:
                _Peer.status_tasks.add(task)
        elif task is None:
            _Peer.status_tasks.difference_update([tsk for tsk in _Peer.status_tasks
                                                  if tsk._name[0] != '^'])
        else:
            logger.warning('invalid peer status task ignored')
        _Peer._lock.release()


pycos._NetRequest = _NetRequest
pycos._Peer = _Peer
pycos.SysTask = SysTask
pycos.Pycos = Pycos
pycos.RTI = RTI = pycos.RPS = RPS
pycos.PeerStatus = PeerStatus
