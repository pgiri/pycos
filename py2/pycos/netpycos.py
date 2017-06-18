"""
This file is part of pycos project. See https://pycos.sourceforge.io for details.

This module adds API for distributed programming to Pycos.
"""

import socket
import inspect
import traceback
import os
import stat
import hashlib
import collections
import copy
import tempfile
import threading
import errno
import atexit
import ssl
import struct
import re
try:
    import netifaces
except ImportError:
    netifaces = None

import pycos
from pycos import *

__author__ = "Giridhar Pemmasani (pgiri@yahoo.com)"
__copyright__ = "Copyright (c) 2012-2014 Giridhar Pemmasani"
__license__ = "Apache 2.0"
__url__ = "https://pycos.sourceforge.io"

__version__ = pycos.__version__
__all__ = pycos.__all__ + ['PeerStatus', 'RTI']

# if connections to a peer are not successful consecutively MaxConnectionErrors
# times, peer is assumed dead and removed
MaxConnectionErrors = 10
MsgTimeout = pycos.MsgTimeout


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


class Pycos(pycos.Pycos):
    """This adds network services to pycos.Pycos so it can communicate with
    peers.

    If 'node' is not None, it must be either hostname or IP address, or list of
    hostnames or IP addresses where pycos runs network services. If 'udp_port' is
    not None, it is port number where pycos runs network services. If 'udp_port'
    is 0, the default port number 51350 is used. If multiple instances of pycos
    are to be running on same host, they all can be started with the same
    'udp_port', so that pycos instances automatically find each other.

    'name' is used in locating peers. They must be unique. If 'name' is not
    given, it is set to string 'node:tcp_port'.

    'ext_ip_addr' is either the IP address or list of IP addresses of NAT
    firewall/gateway if pycos is behind that firewall/gateway. If it is a list,
    each element must correspond to element of 'node' list.

    If 'discover_peers' is True (default), this node broadcasts message to
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

    __metaclass__ = Singleton

    _instance = None
    _pycos = pycos.Pycos.instance()

    class AddrInfo(object):
        def __init__(self, family, ip, ifn, broadcast, netmask):
            self.family = family
            self.ip = ip
            self.ifn = ifn
            self.broadcast = broadcast
            self.netmask = netmask
            self.location = None
            self.tcp_sock = None
            self.udp_sock = None

    def __init__(self, udp_port=0, tcp_port=0, node=None, ext_ip_addr=None,
                 socket_family=None, name=None, discover_peers=True,
                 secret='', certfile=None, keyfile=None, notifier=None,
                 dest_path=None, max_file_size=None):

        self.__class__._instance = self
        super(self.__class__, self).__init__()
        self._rtis = {}
        self._locations = set()
        self._stream_peers = {}
        self._pending_reqs = {}
        self._pending_replies = {}
        self._addrinfos = []

        if not udp_port:
            udp_port = 51350
        if not dest_path:
            dest_path = os.path.join(os.sep, tempfile.gettempdir(), 'pycos')
        self.__dest_path = os.path.abspath(os.path.normpath(dest_path))
        self.__dest_path_prefix = dest_path
        # TODO: avoid race condition (use locking to check/create atomically?)
        if not os.path.isdir(self.__dest_path):
            try:
                os.makedirs(self.__dest_path)
            except:
                # likely another pycos created this directory
                if not os.path.isdir(self.__dest_path):
                    logger.warning('failed to create "%s"', self.__dest_path)
                    logger.debug(traceback.format_exc())
        self.max_file_size = max_file_size
        self._secret = secret
        self._certfile = certfile
        self._keyfile = keyfile
        self._ignore_peers = False

        RTI._pycos = _Peer._pycos = SysTask._pycos = self
        pycos.Task._pycos = pycos.Channel._pycos = Pycos._pycos

        if isinstance(node, list):
            if node:
                nodes = node
            else:
                nodes = [None]
        else:
            nodes = [node]
        if isinstance(ext_ip_addr, list):
            ext_ip_addrs = ext_ip_addr
        else:
            ext_ip_addrs = [ext_ip_addr]

        location = None
        for i in range(len(nodes)):
            node = nodes[i]
            if len(ext_ip_addrs) > i:
                ext_ip_addr = ext_ip_addrs[i]
            else:
                ext_ip_addr = None
            addrinfo = Pycos.node_addrinfo(node, socket_family=socket_family)
            if not addrinfo:
                logger.warning('Invalid node "%s" ignored', node)
                continue
            addrinfo = Pycos.AddrInfo(*addrinfo)

            tcp_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                                   keyfile=self._keyfile, certfile=self._certfile)
            if tcp_port:
                if hasattr(socket, 'SO_REUSEADDR'):
                    tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if hasattr(socket, 'SO_REUSEPORT'):
                    tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            tcp_sock.bind((addrinfo.ip, tcp_port))
            location = Location(addrinfo.ip, tcp_sock.getsockname()[1])
            if ext_ip_addr:
                try:
                    info = socket.getaddrinfo(ext_ip_addr, None)[0]
                    ip_addr = info[4][0]
                    if info[0] == socket.AF_INET6:
                        # canonicalize so different platforms resolve to same string
                        ip_addr = re.sub(r'^0+', '', ip_addr)
                        ip_addr = re.sub(r':0+', ':', ip_addr)
                        ip_addr = re.sub(r'::+', '::', ip_addr)
                    location.addr = ip_addr
                except:
                    logger.warning('invalid ext_ip_addr "%s" ignored', ext_ip_addr)

            tcp_sock.listen(32)
            if not name:
                name = socket.gethostname()
            logger.info('TCP server "%s" @ %s', name if name else '', location)

            addrinfo.tcp_sock = tcp_sock
            addrinfo.location = location
            self._locations.add(location)
            self._addrinfos.append(addrinfo)
            SysTask(self._tcp_proc, addrinfo)

        if not self._addrinfos:
            logger.warning('Could not initialize networking')
            raise Exception('Invalid "node"?')

        udp_addrinfos = {}
        for addrinfo in self._addrinfos:
            if addrinfo.broadcast == '<broadcast>':
                bind_addr = ''
            else:
                bind_addr = addrinfo.broadcast
            udp_addrinfos[bind_addr] = addrinfo
        for bind_addr, addrinfo in udp_addrinfos.items():
            udp_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
            if hasattr(socket, 'SO_REUSEADDR'):
                udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, 'SO_REUSEPORT'):
                udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            if addrinfo.family == socket.AF_INET6:
                mreq = socket.inet_pton(addrinfo.family, addrinfo.broadcast)
                mreq += struct.pack('@I', addrinfo.ifn)
                udp_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_JOIN_GROUP, mreq)
                udp_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            udp_sock.bind((bind_addr, udp_port))
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
        pycos.Task._sign = pycos.Channel._sign = SysTask._sign = RTI._sign = self._signature
        if discover_peers:
            self.discover_peers()
        atexit.register(self.finish)

    @classmethod
    def instance(cls, *args, **kwargs):
        """Returns (singleton) instance of Pycos.
        """
        if not cls._instance:
            cls._instance = cls(*args, **kwargs)
        return cls._instance

    @property
    def dest_path(self):
        return self.__dest_path

    @dest_path.setter
    def dest_path(self, path):
        path = os.path.normpath(path)
        if not path.startswith(self.__dest_path_prefix):
            path = os.path.join(self.__dest_path_prefix,
                                os.path.splitdrive(path)[1].lstrip(os.sep))
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
        self.__dest_path = path

    def finish(self):
        if Pycos._instance:
            Pycos._pycos.finish()
            super(self.__class__, self).finish()
            Pycos._instance = None
            for addrinfo in self._addrinfos:
                addrinfo.tcp_sock.close()
                if addrinfo.udp_sock:
                    addrinfo.udp_sock.close()
            self._addrinfos = []
            self._notifier.terminate()
            logger.shutdown()

    def locate(self, name, timeout=None):
        """Must be used with 'yield' as
        'loc = yield scheduler.locate("peer")'.

        Find and return location of peer with 'name'.
        """
        _Peer._lock.acquire()
        for peer in _Peer.peers.itervalues():
            if peer.name == name:
                loc = peer.location
                break
        else:
            loc = None
        _Peer._lock.release()
        if not loc:
            req = _NetRequest('locate_peer', kwargs={'name': name})
            req.event = Event()
            req_id = id(req)
            self._lock.acquire()
            self._pending_reqs[req_id] = req
            self._lock.release()
            _Peer.send_req_to(req, None)
            if (yield req.event.wait(timeout)) is False:
                req.reply = None
            loc = req.reply
            self._lock.acquire()
            self._pending_reqs.pop(req_id, None)
            self._lock.release()
        raise StopIteration(loc)

    def peer(self, loc, udp_port=0, stream_send=False, broadcast=False, task=None):
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

        If 'broadcast' is True, the client information is broadcast on the
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
                    # canonicalize so different platforms resolve to same string
                    ip_addr = re.sub(r'^0+', '', ip_addr)
                    ip_addr = re.sub(r':0+', ':', ip_addr)
                    ip_addr = re.sub(r'::+', '::', ip_addr)
                loc = Location(ip_addr, 0)
            except:
                logger.warning('invalid node: "%s"', str(loc))
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
                if not broadcast:
                    self._lock.release()
                    raise StopIteration(0)
        else:
            _Peer._lock.acquire()
            for (addr, port), peer in _Peer.peers.iteritems():
                if addr == loc.addr:
                    peer.stream = stream_send
                    if not stream_send:
                        self._stream_peers.pop((addr, port), None)
            _Peer._lock.release()
        self._lock.release()

        # TODO: is there a better approach to find best suitable address to
        # select (with netmask)?
        def best_match(ip):
            best = (0, self._addrinfos[0])
            for addrinfo in self._addrinfos:
                i, n = 0, min(len(ip), len(addrinfo.ip))
                while i < n and ip[i] == addrinfo.ip[i]:
                    i += 1
                if i > best[0]:
                    best = (i, addrinfo)
            return best[1]

        addrinfo = best_match(loc.addr)

        if loc.port:
            req = _NetRequest('signature', kwargs={'version': __version__}, dst=loc)
            sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_STREAM),
                               keyfile=self._keyfile, certfile=self._certfile)
            sock.settimeout(2)
            try:
                yield sock.connect((loc.addr, loc.port))
                yield sock.send_msg(serialize(req))
                sign = yield sock.recv_msg()
                ret = yield self._acquaint_(loc, sign, addrinfo, task=task)
            except:
                ret = -1
            finally:
                sock.close()

            if broadcast and ret == 0:
                kwargs = {'location': addrinfo.location, 'signature': self._signature,
                          'name': self._name, 'version': __version__}
                _Peer.send_req_to(_NetRequest('relay_ping', kwargs=kwargs, dst=loc), loc)
            raise StopIteration(ret)
        else:
            if not udp_port:
                udp_port = 51350
            ping_msg = {'location': addrinfo.location, 'signature': self._signature,
                        'name': self._name, 'version': __version__, 'broadcast': broadcast}
            ping_msg = 'ping:'.encode() + serialize(ping_msg)
            ping_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
            ping_sock.settimeout(2)
            if addrinfo.family == socket.AF_INET:
                ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            else:  # addrinfo.family == socket.AF_INET6
                ping_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS,
                                     struct.pack('@i', 1))
                ping_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_IF, addrinfo.ifn)
            ping_sock.bind((addrinfo.ip, 0))
            try:
                yield ping_sock.sendto(ping_msg, (loc.addr, udp_port))
            except:
                pass
            ping_sock.close()
        raise StopIteration(0)

    def discover_peers(self, port=None):
        """This method can be invoked (periodically?) to broadcast message to
        discover peers, if there is a chance initial broadcast message may be
        lost (as these messages are sent over UDP).
        """
        ping_msg = {'signature': self._signature, 'name': self._name, 'version': __version__}

        def _discover(addrinfo, port, task=None):
            ping_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
            ping_sock.settimeout(2)
            if addrinfo.family == socket.AF_INET:
                ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            else:  # addrinfo.family == socket.AF_INET6
                ping_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS,
                                     struct.pack('@i', 1))
                ping_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_IF, addrinfo.ifn)
            ping_sock.bind((addrinfo.ip, 0))
            if not port:
                port = addrinfo.udp_sock.getsockname()[1]
            ping_msg['location'] = addrinfo.location
            try:
                yield ping_sock.sendto('ping:'.encode() + serialize(ping_msg),
                                       (addrinfo.broadcast, port))
            except:
                pass
            ping_sock.close()

        for addrinfo in self._addrinfos:
            SysTask(_discover, addrinfo, port)

    def ignore_peers(self, ignore):
        """Don't respond to 'ping' from peers if 'ignore=True'. This can be used
        during shutdown, or to limit peers to communicate.
        """
        if ignore:
            self._ignore_peers = True
        else:
            self._ignore_peers = False

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
        except:
            logger.warning('send_file: File "%s" is not valid', file)
            raise StopIteration(-1)
        if not ((stat.S_IMODE(stat_buf.st_mode) & stat.S_IREAD) and stat.S_ISREG(stat_buf.st_mode)):
            logger.warning('send_file: File "%s" is not valid', file)
            raise StopIteration(-1)
        if dir:
            if not isinstance(dir, basestring):
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
        sock = AsyncSocket(socket.socket(self.addrinfo.family, socket.SOCK_STREAM),
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
        except:
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
        if isinstance(dir, basestring) and dir:
            dir = dir.strip()
            # reject absolute path for dir
            if os.path.join(os.sep, dir) == dir:
                raise StopIteration(-1)
        kwargs = {'file': os.path.basename(file), 'dir': dir}
        req = _NetRequest('del_file', kwargs=kwargs, dst=location, timeout=timeout)
        reply = yield _Peer._sync_reply(req)
        if reply is None:
            reply = -1
        raise StopIteration(reply)

    @staticmethod
    def node_addrinfo(node, socket_family=None):
        """Given node (either host name or IP address) is resolved to IP address and
        fills in and returns information with AddrInfo structure.
        """
        if node:
            try:
                node = socket.getaddrinfo(node, None)[0]
            except:
                return None
            if not socket_family:
                socket_family = node[0]
            elif node[0] != socket_family:
                node = None
            if node:
                node = node[4][0]
            if not socket_family:
                socket_family = socket.AF_INET
        if not socket_family:
            socket_family = socket.getaddrinfo(socket.gethostname(), None)[0][0]
        assert socket_family in (socket.AF_INET, socket.AF_INET6)

        ifn, addrinfo, netmask, broadcast = 0, None, None, None
        if netifaces:
            for iface in netifaces.interfaces():
                if socket_family == socket.AF_INET:
                    if addrinfo:
                        break
                else:  # socket_family == socket.AF_INET6
                    if ifn and addrinfo:
                        break
                    ifn, addrinfo, netmask = 0, None, None
                for link in netifaces.ifaddresses(iface).get(socket_family, []):
                    netmask = link.get('netmask', None)
                    broadcast = link.get('broadcast', None)
                    if socket_family == socket.AF_INET:
                        if link.get('broadcast', '').startswith(link['addr'].split('.')[0]):
                            if (not node) or (link['addr'] == node):
                                addrinfo = socket.getaddrinfo(link['addr'], None)[0]
                                break
                    else:  # socket_family == socket.AF_INET6
                        if link['addr'].startswith('fe80:'):
                            addr = link['addr']
                            if '%' not in addr.split(':')[-1]:
                                addr = addr + '%' + interface
                            for addrinfo in socket.getaddrinfo(addr, None):
                                if addrinfo[2] == socket.IPPROTO_TCP:
                                    ifn = addrinfo[4][-1]
                                    break
                        elif link['addr'].startswith('fd'):
                            for addrinfo in socket.getaddrinfo(link['addr'], None):
                                if addrinfo[2] == socket.IPPROTO_TCP:
                                    addrinfo = addrinfo
                                    break
        elif socket_family == socket.AF_INET6:
            logger.warning('IPv6 may not work without "netifaces" package!')

        if addrinfo:
            if not node:
                node = addrinfo[4][0]
            if not socket_family:
                socket_family = addrinfo[0]
        if not node:
            node = socket.gethostname()

        node = socket.getaddrinfo(node, None, socket_family, socket.SOCK_STREAM)[0]
        ip_addr = node[4][0]
        if node[0] == socket.AF_INET6:
            # canonicalize so different platforms resolve to same string
            ip_addr = re.sub(r'^0+', '', ip_addr)
            ip_addr = re.sub(r':0+', ':', ip_addr)
            ip_addr = re.sub(r'::+', '::', ip_addr)
            if not broadcast:
                broadcast = 'ff05::1'
        else:
            if not broadcast:
                broadcast = '<broadcast>'

        return (node[0], ip_addr, ifn, broadcast, netmask)

    def _acquaint_(self, peer_location, peer_signature, addrinfo, task=None):
        """
        Internal use only.
        """
        if peer_signature in _Peer._sign_locations:
            raise StopIteration
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
        except:
            logger.debug(traceback.format_exc())
            reply = -1
        sock.close()
        raise StopIteration(0)

    def _udp_proc(self, location, addrinfo, task=None):
        """
        Internal use only.
        """
        task.set_daemon()
        sock = addrinfo.udp_sock
        while 1:
            msg, addr = yield sock.recvfrom(1024)
            if not msg.startswith('ping:'):
                logger.warning('ignoring UDP message from %s:%s', addr[0], addr[1])
                continue
            try:
                ping_info = deserialize(msg[len('ping:'):])
            except:
                continue
            peer_location = ping_info.get('location', None)
            if not isinstance(peer_location, Location) or peer_location in self._locations:
                continue
            if ping_info['version'] != __version__:
                logger.warning('Peer %s version %s is not %s',
                               peer_location, ping_info['version'], __version__)
                continue
            if self._ignore_peers:
                continue
            if self._secret is None:
                auth_code = None
            else:
                auth_code = ping_info.get('signature', '') + self._secret
                auth_code = hashlib.sha1(auth_code.encode()).hexdigest()
            _Peer._lock.acquire()
            peer = _Peer.peers.get((peer_location.addr, peer_location.port), None)
            _Peer._lock.release()
            if peer and peer.auth != auth_code:
                _Peer.remove(peer_location)
                peer = None

            if not peer:
                SysTask(self._acquaint_, peer_location, ping_info['signature'], addrinfo)

    def _tcp_proc(self, addrinfo, task=None):
        """
        Internal use only.
        """
        task.set_daemon()
        sock = addrinfo.tcp_sock
        while 1:
            try:
                conn, addr = yield sock.accept()
            except ssl.SSLError as err:
                logger.debug('SSL connection failed: %s', str(err))
                continue
            except GeneratorExit:
                break
            except:
                logger.debug(traceback.format_exc())
                continue
            SysTask(self._tcp_conn_proc, conn, addrinfo)

    def _tcp_conn_proc(self, conn, addrinfo, task=None):
        """
        Internal use only.
        """
        while 1:
            try:
                msg = yield conn.recv_msg()
            except:
                break
            if not msg:
                break
            try:
                req = deserialize(msg)
            except:
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
                    if name[0] == '~':
                        task = self._tasks.get(int(task))
                        if task and task._name == name:
                            reply = task.send(req.kwargs['message'])
                        else:
                            logger.warning('ignoring invalid recipient to "send"')
                    else:
                        Task._pycos._lock.acquire()
                        task = Task._pycos._tasks.get(int(task), None)
                        Task._pycos._lock.release()
                        if task and task._name == name:
                            reply = task.send(req.kwargs['message'])
                        else:
                            logger.warning('ignoring invalid recipient to "send"')
                else:
                    channel = req.kwargs.get('channel', None)
                    Channel._pycos._lock.acquire()
                    channel = Channel._pycos._channels.get(channel)
                    Channel._pycos._lock.release()
                    if channel:
                        reply = channel.send(req.kwargs['message'])
                    else:
                        logger.warning('ignoring invalid recipient to "send"')
                yield conn.send_msg(serialize(reply))

            elif req.name == 'deliver':
                reply = -1
                task = req.kwargs.get('task', None)
                if task:
                    name = req.kwargs.get('name', ' ')
                    if name[0] == '~':
                        task = self._tasks.get(int(task))
                        if task and task.send(req.kwargs['message']) == 0:
                            reply = 1
                        else:
                            logger.warning('invalid "deliver" message ignored')
                    else:
                        Task._pycos._lock.acquire()
                        task = Task._pycos._tasks.get(int(task))
                        Task._pycos._lock.release()
                        if task and task.send(req.kwargs['message']) == 0:
                            reply = 1
                else:
                    channel = req.kwargs.get('channel')
                    if channel:
                        def async_reply(req, task=None):
                            reply = yield channel.deliver(
                                req.kwargs['message'], timeout=req.timeout, n=req.kwargs['n'])
                            req.event = None
                            req.name += '-reply'
                            reply_location = req.kwargs['reply_location']
                            req.kwargs = {'reply_id': req.kwargs['reply_id'], 'reply': reply}
                            _Peer.send_req_to(req, reply_location)

                        Channel._pycos._lock.acquire()
                        channel = Channel._pycos._channels.get(channel)
                        Channel._pycos._lock.release()
                        if channel:
                            SysTask(async_reply, req)
                    else:
                        logger.warning('invalid "deliver" message ignored')
                    reply = None
                yield conn.send_msg(serialize(reply))

            elif req.name.endswith('deliver-reply'):
                reply = req
                self._lock.acquire()
                req = self._pending_replies.pop(reply.kwargs.get('reply_id', None), None)
                self._lock.release()
                if req and req.event:
                    req.reply = reply.kwargs['reply']
                    req.event.set()
                yield conn.send_msg(serialize(None))

            elif req.name == 'run_rti':
                rti = self._rtis.get(req.kwargs['name'], None)
                if rti:
                    args = req.kwargs['args']
                    kwargs = req.kwargs['kwargs']
                    try:
                        reply = Task(rti._method, *args, **kwargs)
                    except:
                        reply = Exception(traceback.format_exc())
                else:
                    reply = Exception('RTI "%s" is not registered' % req.kwargs['name'])
                yield conn.send_msg(serialize(reply))

            elif req.name == 'locate_task':
                if req.kwargs['name'][0] == '~':
                    task = self._rtasks.get(req.kwargs['name'], None)
                else:
                    Task._pycos._lock.acquire()
                    task = Task._pycos._rtasks.get(req.kwargs['name'], None)
                    Task._pycos._lock.release()
                yield conn.send_msg(serialize(task))

            elif req.name == 'locate_channel':
                Channel._pycos._lock.acquire()
                channel = Channel._pycos._rchannels.get(req.kwargs['name'], None)
                Channel._pycos._lock.release()
                yield conn.send_msg(serialize(channel))

            elif req.name == 'locate_rti':
                rti = self._rtis.get(req.kwargs['name'], None)
                yield conn.send_msg(serialize(rti))

            elif req.name == 'monitor':
                reply = -1
                monitor = req.kwargs.get('monitor', None)
                task = req.kwargs.get('task', None)
                name = req.kwargs.get('name', None)
                if task and name:
                    if name == '~':
                        task = self._tasks.get(int(task), None)
                        if task and task._name == name:
                            reply = self._monitor(monitor, task)
                    else:
                        Task._pycos._lock.acquire()
                        task = Task._pycos._tasks.get(int(task), None)
                        if task and task._name == name:
                            reply = Task._pycos._monitor(monitor, task)
                        Task._pycos._lock.release()
                yield conn.send_msg(serialize(reply))

            elif req.name == 'terminate_task':
                reply = -1
                task = req.kwargs.get('task', None)
                name = req.kwargs.get('name', None)
                if task and name:
                    if name[0] == '~':
                        task = self._tasks.get(int(task), None)
                    else:
                        Task._pycos._lock.acquire()
                        task = Task._pycos._tasks.get(int(task), None)
                        Task._pycos._lock.release()
                if isinstance(task, Task):
                    reply = task.terminate()
                yield conn.send_msg(serialize(reply))

            elif req.name == 'subscribe':
                reply = -1
                channel = req.kwargs.get('channel', ' ')
                Channel._pycos._lock.acquire()
                channel = Channel._pycos._channels.get(channel, None)
                Channel._pycos._lock.release()
                if isinstance(channel, Channel) and not channel._location:
                    subscriber = req.kwargs.get('subscriber', None)
                    if isinstance(subscriber, Task):
                        if not subscriber._location:
                            Task._pycos._lock.acquire()
                            subscriber = Task._pycos._tasks.get(int(subscriber._id), None)
                            Task._pycos._lock.release()
                        reply = yield channel.subscribe(subscriber)
                    elif isinstance(subsriber, Channel):
                        if not subscriber._location:
                            Channel._pycos._lock.acquire()
                            subscriber = self._channels.get(subscriber._name, None)
                            Channel._pycos._lock.release()
                        reply = yield channel.subscribe(subscriber)
                yield conn.send_msg(serialize(reply))

            elif req.name == 'unsubscribe':
                reply = -1
                channel = req.kwargs.get('channel', ' ')
                Channel._pycos._lock.acquire()
                channel = Channel._pycos._channels.get(channel, None)
                Channel._pycos._lock.release()
                if isinstance(channel, Channel) and not channel._location:
                    subscriber = req.kwargs.get('subscriber', None)
                    if isinstance(subscriber, Task):
                        if not subscriber._location:
                            Task._pycos._lock.acquire()
                            subscriber = Task._pycos._tasks.get(int(subscriber._id), None)
                            Task._pycos._lock.release()
                        reply = yield channel.unsubscribe(subscriber)
                    elif isinstance(subsriber, Channel):
                        if not subscriber._location:
                            Channel._pycos._lock.acquire()
                            subscriber = self._channels.get(subscriber._name, None)
                            Channel._pycos._lock.release()
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
                    except:
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
                    except:
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
                if isinstance(dir, basestring) and dir:
                    tgt = os.path.join(dir, tgt)
                tgt = os.path.join(self.__dest_path, tgt)
                if tgt.startswith(self.__dest_path) and os.path.isfile(tgt):
                    os.remove(tgt)
                    d = os.path.dirname(tgt)
                    try:
                        while d > self.__dest_path and os.path.isdir(d):
                            os.rmdir(d)
                            d = os.path.dirname(d)
                    except:
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
                if req.kwargs['signature'] not in _Peer._sign_locations:
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
                peer_location = req.kwargs.get('location', None)
                if not isinstance(peer_location, Location) or peer_location in self._locations:
                    yield conn.send_msg(serialize(-1))
                    break
                _Peer._lock.acquire()
                peer = _Peer.peers.get((peer_location.addr, peer_location.port), None)
                _Peer._lock.release()
                if self._secret is None:
                    auth_code = None
                else:
                    auth = req.kwargs['signature'] + self._secret
                    auth = hashlib.sha1(auth.encode()).hexdigest()
                if peer and peer.auth != auth_code:
                    _Peer.remove(peer_location)
                    peer = None
                if not peer:
                    SysTask(self._acquaint_, peer_location, req.kwargs['signature'], addrinfo)
                yield conn.send_msg(serialize(0))

            elif req.name == 'relay_ping':
                yield conn.send_msg(serialize(0))
                ping_msg = 'ping:'.encode() + serialize(req.kwargs)
                port = addrinfo.udp_sock.getsockname()[1]
                ping_sock = AsyncSocket(socket.socket(addrinfo.family, socket.SOCK_DGRAM))
                ping_sock.settimeout(2)
                if addrinfo.family == socket.AF_INET:
                    ping_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                else:  # addrinfo.family == socket.AF_INET6
                    ping_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS,
                                         struct.pack('@i', 1))
                    ping_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_IF,
                                         addrinfo.ifn)
                ping_sock.bind((addrinfo.ip, 0))
                try:
                    yield ping_sock.sendto(ping_msg, (addrinfo.broadcast, port))
                except:
                    pass
                finally:
                    ping_sock.close()

            else:
                logger.warning('invalid request "%s" ignored', req.name)

        conn.close()

    def __repr__(self):
        s = ', '.join(str(s) for s in self._locations)
        if s == self._name:
            return s
        else:
            return '"%s" @ %s' % (self._name, s)


class RTI(object):
    """Remote Task Invocation.

    Methods registered with RTI can be executed as tasks on request (by remotely
    running tasks).
    """

    __slots__ = ('_name', '_location', '_method')

    _pycos = None
    _sign = None

    def __init__(self, method, name=None):
        """'method' must be generator method; this is used to create tasks. If
        'name' is not given, method's function name is used for registering.
        """
        if not inspect.isgeneratorfunction(method):
            raise RuntimeError('method must be generator function')
        self._method = method
        if name:
            self._name = name
        else:
            self._name = method.__name__
        if not RTI._pycos:
            RTI._pycos = Pycos.instance()
        self._location = None

    @property
    def location(self):
        """Get Location instance where this RTI is running.
        """
        return copy.copy(self._location)

    @property
    def name(self):
        """Get name of RTI.
        """
        return self._name

    @staticmethod
    def locate(name, location=None, timeout=None):
        """Must be used with 'yield' as 'rti = yield RTI.locate("name")'.

        Returns RTI instance to registered RTI at a remote peer so its method
        can be used to execute tasks at that peer.

        If 'location' is given, RTI is looked up at that specific peer;
        otherwise, all known peers are queried for given name.
        """
        if not RTI._pycos:
            RTI._pycos = Pycos.instance()
        req = _NetRequest('locate_rti', kwargs={'name': name}, dst=location, timeout=timeout)
        req.event = Event()
        req_id = id(req)
        RTI._pycos._lock.acquire()
        RTI._pycos._pending_reqs[req_id] = req
        RTI._pycos._lock.release()
        _Peer.send_req_to(req, location)
        if (yield req.event.wait(timeout)) is False:
            req.reply = None
        rti = req.reply
        RTI._pycos._lock.acquire()
        RTI._pycos._pending_reqs.pop(req_id, None)
        RTI._pycos._lock.release()
        raise StopIteration(rti)

    def register(self):
        """RTI must be registered so it can be located.
        """
        if self._location:
            return -1
        if not inspect.isgeneratorfunction(self._method):
            return -1
        RTI._pycos._lock.acquire()
        if RTI._pycos._rtis.get(self._name, None) is None:
            RTI._pycos._rtis[self._name] = self
            RTI._pycos._lock.release()
            return 0
        else:
            RTI._pycos._lock.release()
            return -1

    def unregister(self):
        """Unregister registered RTI; see 'register' above.
        """
        if self._location:
            return -1
        RTI._pycos._lock.acquire()
        if RTI._pycos._rtis.pop(self._name, None) is None:
            RTI._pycos._lock.release()
            return -1
        else:
            RTI._pycos._lock.release()
            return 0

    def __call__(self, *args, **kwargs):
        """Must be used with 'yeild' as 'rtask = yield rti(*args, **kwargs)'.

        Run RTI (method at remote location) with args and kwargs. Both args and
        kwargs must be serializable. Returns (remote) Task instance.
        """
        req = _NetRequest('run_rti', kwargs={'name': self._name, 'args': args, 'kwargs': kwargs},
                          dst=self._location, timeout=MsgTimeout)
        reply = yield _Peer._sync_reply(req)
        if isinstance(reply, Task):
            raise StopIteration(reply)
        elif reply is None:
            raise StopIteration(None)
        else:
            raise Exception(reply)

    def __getstate__(self):
        state = {'name': self._name}
        if self._location:
            state['location'] = self._location
        else:
            state['location'] = RTI._sign
        return state

    def __setstate__(self, state):
        self._name = state['name']
        self._location = state['location']
        if isinstance(self._location, Location):
            if self._location in RTI._pycos._locations:
                self._location = None
        else:
            self._location = _Peer.sign_location(self._location)
            # TODO: is it possible for peer to disconnect during deserialize?

    def __eq__(self, other):
        return (isinstance(other, RTI) and
                self._name == other._name and self._location == other._location)

    def __ne__(self, other):
        return ((not isinstance(other, RTI)) or
                self._name != other._name or self._location != other._location)

    def __repr__(self):
        s = '%s' % (self._name)
        if self._location:
            s = '%s@%s' % (s, self._location)
        return s


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
        self.__class__._pycos = self._scheduler = SysTask._pycos
        super(SysTask, self).__init__(*args, **kwargs)
        self._name = '~' + self._name

    @staticmethod
    def locate(name, location=None, timeout=None):
        if not SysTask._pycos:
            SysTask._pycos = Pycos.instance()
        yield Task._locate('~' + name, location, timeout)


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
        self.reply = None
        self.timeout = timeout

    def __getstate__(self):
        state = {'name': self.name, 'kwargs': self.kwargs, 'dst': self.dst,
                 'auth': self.auth, 'timeout': self.timeout}
        return state

    def __setstate__(self, state):
        for k, v in state.iteritems():
            setattr(self, k, v)


class _Peer(object):
    """Internal use only.
    """

    __slots__ = ('name', 'location', 'auth', 'keyfile', 'certfile', 'stream', 'conn',
                 'reqs', 'waiting', 'req_task', 'addrinfo', 'signature')

    peers = {}
    status_task = None
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
        logger.debug('%s: found peer %s', addrinfo.location, self.location)
        if _Peer.status_task:
            _Peer.status_task.send(PeerStatus(location, name, PeerStatus.Online))

        _Peer._pycos._lock.acquire()
        if ((location.addr, location.port) in _Peer._pycos._stream_peers or
            (location.addr, 0) in _Peer._pycos._stream_peers):
            self.stream = True

        # send pending (async) requests
        for pending_req in _Peer._pycos._pending_reqs.itervalues():
            if (pending_req.name == 'locate_peer' and pending_req.kwargs['name'] == self.name):
                pending_req.reply = location
                pending_req.event.set()

            if pending_req.dst:
                if pending_req.dst == location:
                    _Peer.send_req(pending_req)
            else:
                _Peer.send_req_to(pending_req, location)
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
        peers = [copy.copy(peer.location) for peer in _Peer.peers.itervalues()]
        _Peer._lock.release()
        return peers

    @staticmethod
    def get_peer(location):
        _Peer._lock.acquire()
        peer = _Peer.peers.get((location.addr, location.port), None)
        _Peer._lock.release()
        return peer

    @staticmethod
    def send_req(req):
        _Peer._lock.acquire()
        peer = _Peer.peers.get((req.dst.addr, req.dst.port), None)
        if not peer:
            logger.debug('Ignoring request to invalid peer %s', req.dst)
            _Peer._lock.release()
            return -1
        peer.reqs.append(req)
        if peer.waiting:
            peer.waiting = False
            peer.req_task.send(1)
        _Peer._lock.release()
        return 0

    @staticmethod
    def send_req_to(req, dst):
        if dst:
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
        else:
            _Peer._lock.acquire()
            for peer in _Peer.peers.itervalues():
                peer.reqs.append(req)
                if peer.waiting:
                    peer.waiting = False
                    peer.req_task.send(1)
            _Peer._lock.release()
        return 0

    @staticmethod
    def _sync_reply(req, alarm_value=None):
        req.event = Event()
        if _Peer.send_req(req) != 0:
            raise StopIteration(-1)
        if (yield req.event.wait(req.timeout)) is False:
            raise StopIteration(alarm_value)
        raise StopIteration(req.reply)

    @staticmethod
    def _async_reply(req, alarm_value=None):
        req.event = Event()
        req.kwargs['reply_id'] = id(req)
        req.kwargs['reply_location'] = _Peer._pycos._location
        _Peer._pycos._lock.acquire()
        _Peer._pycos._pending_replies[id(req)] = req
        _Peer._pycos._lock.release()
        if _Peer.send_req(req) != 0:
            raise StopIteration(-1)
        if (yield req.event.wait(req.timeout)) is False:
            raise StopIteration(alarm_value)
        raise StopIteration(req.reply)

    @staticmethod
    def close_peer(peer, timeout, task=None):
        req = _NetRequest('close_peer', kwargs={'location': peer.addrinfo.location},
                          dst=peer.location, timeout=timeout)
        yield _Peer._sync_reply(req)
        if peer.req_task:
            yield peer.req_task.terminate()
            while peer.req_task:
                yield task.sleep(0.1)

    @staticmethod
    def shutdown(timeout=MsgTimeout):
        _Peer._lock.acquire()
        for peer in _Peer.peers.itervalues():
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
                        except:
                            pass
                        self.conn = None
                    break
                except:
                    if self.conn:
                        # self.conn.shutdown(socket.SHUT_WR)
                        self.conn.close()
                        self.conn = None
                    req.reply = None
                    if req.event:
                        req.event.set()
                    conn_errors += 1
                    if conn_errors >= MaxConnectionErrors:
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
                reply = yield self.conn.recv_msg()
                reply = deserialize(reply)
                if req.event:
                    if reply is not None or (req.dst == self.location and
                                             'reply_id' not in req.kwargs):
                        req.reply = reply
                        req.event.set()
                else:
                    req.reply = reply
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
                except:
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
                except:
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
                    except:
                        pass
                    self.conn = None
                break
            except:
                # logger.debug(traceback.format_exc())
                if self.conn:
                    try:
                        self.conn.shutdown(socket.SHUT_WR)
                        self.conn.close()
                    except:
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
            peer.stream = False
            _Peer._sign_locations.pop(peer.signature, None)
            if peer.req_task:
                peer.req_task.terminate()
            if _Peer.status_task:
                _Peer.status_task.send(PeerStatus(peer.location, peer.name, PeerStatus.Offline))

    @staticmethod
    def peer_status(task):
        _Peer._lock.acquire()
        if isinstance(task, Task):
            # if there is another status_task, add or replace?
            for peer in _Peer.peers.itervalues():
                try:
                    task.send(PeerStatus(peer.location, peer.name, PeerStatus.Online))
                except:
                    logger.debug(traceback.format_exc())
                    break
            else:
                _Peer.status_task = task
        elif task is None:
            _Peer.status_task = None
        else:
            logger.warning('invalid peer status task ignored')
        _Peer._lock.release()


pycos._NetRequest = _NetRequest
pycos._Peer = _Peer
pycos.SysTask = SysTask
pycos.Pycos = Pycos
pycos.RTI = RTI
pycos.PeerStatus = PeerStatus
