#+
# Copyright 2014 iXsystems, Inc.
# All rights reserved
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted providing that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
#####################################################################

from __future__ import print_function
import array
import os
import errno
import paramiko
import socket
import select
import time
import logging
from threading import RLock, Event
from freenas.utils import xrecvmsg, xsendmsg
from freenas.utils.spawn_thread import spawn_thread
from ws4py.client.threadedclient import WebSocketClient
import struct


MAXFDS = 128
CMSGCRED_SIZE = struct.calcsize('iiiih16i')
_debug_log_file = None
_client_transports = {}
_server_transports = {}


def debug_log(message, *args):
    """ Write messages to the debug log.

    Args:
        message (str): The message to write.
        args (tuple): Variables whose values will be formatted to the debug message.
    """
    global _debug_log_file

    if os.getenv('DISPATCHER_TRANSPORT_DEBUG'):
        if not _debug_log_file:
            try:
                _debug_log_file = open('/var/tmp/dispatchertransport.{0}.log'.format(os.getpid()), 'w')
            except OSError:
                pass

        print(message.format(*args), file=_debug_log_file)
        _debug_log_file.flush()


def _patched_exec_command(
    self, command, bufsize=-1,
    timeout=None, get_pty=False, stdin_binary=True,
    stdout_binary=False, stderr_binary=False
):
    chan = self._transport.open_session()
    if get_pty:
        chan.get_pty()
    chan.settimeout(timeout)
    chan.exec_command(command)
    stdin = chan.makefile('wb' if stdin_binary else 'w', bufsize)
    stdout = chan.makefile('rb' if stdout_binary else 'r', bufsize)
    stderr = chan.makefile_stderr('rb' if stderr_binary else 'r', bufsize)
    return stdin, stdout, stderr


paramiko.SSHClient.exec_command = _patched_exec_command


def client_transport(*schemas):
    def wrapper(c):
        for i in schemas:
            _client_transports[i] = c

        return c

    return wrapper


def server_transport(*schemas):
    def wrapper(c):
        for i in schemas:
            _server_transports[i] = c

        return c

    return wrapper


class ClientTransport(object):
    def __new__(cls, *args, **kwargs):
        if cls is ClientTransport:
            scheme = args[0]
            try:
                impl = _client_transports[scheme]
            except KeyError:
                raise ValueError('Unknown transport for scheme {0}'.format(scheme))

            i = object.__new__(impl)
            return i
        else:
            super(ClientTransport, cls).__new__(cls)

    def connect(self, url, parent, **kwargs):
        raise NotImplementedError()

    @property
    def address(self):
        return None

    def send(self, message, fds):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()


class ServerTransport(object):
    def __new__(cls, *args, **kwargs):
        if cls is ServerTransport:
            scheme = args[0]
            try:
                impl = _server_transports[scheme]
            except KeyError:
                raise ValueError('Unknown transport for scheme {0}'.format(scheme))

            i = object.__new__(impl)
            return i
        else:
            super(ServerTransport, cls).__new__(cls)

    def serve_forever(self, server):
        raise NotImplementedError()


@client_transport('ws', 'http')
class ClientTransportWS(ClientTransport):
    class WebSocketHandler(WebSocketClient):
        def __init__(self, url, parent):
            super(ClientTransportWS.WebSocketHandler, self).__init__(url)
            self.parent = parent

        def opened(self):
            debug_log('Connection opened, local address {0}', self.local_address)
            self.parent.opened.set()
            self.parent.parent.on_open()

        def closed(self, code, reason=None):
            debug_log('Connection closed, code {0}', code)
            self.parent.opened.clear()
            self.parent.parent.on_close('Going away')

        def received_message(self, message):
            self.parent.parent.on_message(message.data)

    def __init__(self, scheme):
        self.parent = None
        self.scheme_default_port = None
        self.ws = None
        self.hostname = None
        self.username = None
        self.port = None
        self.opened = Event()

    def connect(self, url, parent, **kwargs):
        """ Open a connection.

        Args:
            url (ParseResult): The url to open.
            parent (Connection): The connection wrapper class object.

        Kwargs:
            username (str): The username to login with.
            hostname (str): The hostname to connect to.
            port (int): The port to connect to.

        Raises:
            RuntimeError, ValueError
        """
        self.scheme_default_port = 80
        self.parent = parent
        self.username = url.username
        self.port = url.port

        if url.hostname:
            self.hostname = url.hostname
        elif url.netloc:
            self.hostname = url.netloc
            if '@' in self.hostname:
                temp, self.hostname = self.hostname.split('@')
        elif url.path:
            self.hostname = url.path

        if not self.parent:
            raise RuntimeError('ClientTransportWS can be only created inside of a class')

        if not self.username:
                self.username = kwargs.get('username', None)
        else:
            if 'username' in kwargs:
                raise ValueError('Username cannot be delared in both url and arguments.')
        if self.username:
            raise ValueError('Username cannot be delared at this state for ws transport type.')

        if not self.hostname:
            self.hostname = kwargs.get('hostname', "127.0.0.1")
        else:
            if 'hostname' in kwargs:
                raise ValueError('Host name cannot be delared in both url and arguments.')

        if not self.port:
            self.port = kwargs.get('port', self.scheme_default_port)
        else:
            if 'port' in kwargs:
                raise ValueError('Port cannot be delared in both url and arguments.')

        ws_url = 'ws://{0}:{1}/dispatcher/socket'.format(self.hostname, self.port)
        self.ws = self.WebSocketHandler(ws_url, self)
        self.ws.connect()
        self.opened.wait()

    @property
    def address(self):
        return self.ws.local_address

    def send(self, message, fds):
        try:
            self.ws.send(message)
        except OSError as err:
            if err.errno == errno.EPIPE:
                debug_log('Socket is closed. Closing connection')
                self.close()

    def close(self):
        self.ws.close()

    def wait_forever(self):
        if os.getenv("DISPATCHERCLIENT_TYPE") == "GEVENT":
            import gevent
            while True:
                gevent.sleep(60)
        else:
            self.ws.run_forever()

    @property
    def connected(self):
        return self.opened.is_set()


@client_transport('ssh', 'ws+ssh')
class ClientTransportSSH(ClientTransport):
    def __init__(self, scheme):
        self.ssh = None
        self.channel = None
        self.url = None
        self.parent = None
        self.hostname = None
        self.username = None
        self.password = None
        self.port = None
        self.pkey = None
        self.timeout = None
        self.key_filename = None
        self.terminated = False
        self.stdin = None
        self.stdout = None
        self.stderr = None
        self.host_key_file = None
        self.look_for_keys = True
        self.connected = False

    def connect(self, url, parent, **kwargs):
        self.url = url
        self.parent = parent
        self.username = url.username
        self.port = url.port

        if not self.parent:
            raise RuntimeError('ClientTransportSSH can be only created inside of a class')

        if url.hostname:
            self.hostname = url.hostname
        elif url.netloc:
            self.hostname = url.netloc
            if '@' in self.hostname:
                temp, self.hostname = self.hostname.split('@')
        elif url.path:
            self.hostname = url.path

        if not self.username:
                self.username = kwargs.get('username', None)
        else:
            if 'username' in kwargs:
                raise ValueError('Username cannot be delared in both url and arguments.')
        if not self.username:
            raise ValueError('Username is not declared.')

        if not self.hostname:
                self.hostname = kwargs.get('hostname', None)
        else:
            if 'hostname' in kwargs:
                raise ValueError('Hostname cannot be delared in both url and arguments.')
        if not self.hostname:
            raise ValueError('Hostname is not declared.')

        if not self.port:
                self.port = kwargs.get('port', 22)
        else:
            if 'port' in kwargs:
                raise ValueError('Port cannot be delared in both url and arguments.')

        self.password = kwargs.get('password', None)
        self.pkey = kwargs.get('pkey', None)
        self.key_filename = kwargs.get('key_filename', None)
        if not self.pkey and not self.password and not self.key_filename:
            raise ValueError('No password, key_filename nor pkey for authentication declared.')

        self.host_key_file = kwargs.get('host_key_file', None)

        self.timeout = kwargs.get('timeout', 30)

        debug_log('Trying to connect to {0}', self.hostname)

        try:
            self.ssh = paramiko.SSHClient()
            logging.getLogger("paramiko").setLevel(logging.WARNING)
            if self.host_key_file:
                self.look_for_keys = False
                try:
                    self.ssh.load_host_keys(self.host_key_file)
                except IOError:
                    debug_log('Cannot read host key file: {0}. SSH transport is closing.', self.host_key_file)
                    self.close()
                    raise
            else:
                self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            self.ssh.connect(
                self.hostname,
                port=self.port,
                username=self.username,
                password=self.password,
                pkey=self.pkey,
                look_for_keys=self.look_for_keys,
                key_filename=self.key_filename,
                timeout=self.timeout
            )

            self.connected = True
            debug_log('Connected to {0}', self.hostname)

        except paramiko.AuthenticationException as err:
            debug_log('Authentication exception: {0}', err)
            raise

        except paramiko.BadHostKeyException as err:
            debug_log('Bad host key exception: {0}', err)
            raise

        except paramiko.SSHException as err:
            debug_log('SSH exception: {0}', err)
            raise

        except socket.error as err:
            debug_log('Socket exception: {0}', err)
            raise

        self.stdin, self.stdout, self.stderr = self.ssh.exec_command(
            "sh /usr/local/libexec/dispatcher/ssh_transport_catcher",
            bufsize=0
        )

        self.channel = self.ssh.get_transport().open_session()

        spawn_thread(self.recv)

    def send(self, message, fds):
        if self.terminated is False:
            header = struct.pack('II', 0xdeadbeef, len(message))
            message = header + message.encode('utf-8')
            try:
                self.stdin.write(message)
                self.stdin.flush()
                debug_log("Sent data: {0}", message)
            except OSError:
                self.closed()

    def recv(self):
        while self.terminated is False:
            header = self.stdout.read(8)
            if header == b'' or len(header) != 8:
                self.closed()
                break

            magic, length = struct.unpack('II', header)
            if magic == 0xbadbeef0:
                self.close()
                raise PermissionError('Permission denied')
            if magic != 0xdeadbeef:
                debug_log('Message with wrong magic dropped')
                continue

            message = self.stdout.read(length)
            if message == b'' or len(message) != length:
                self.closed()
            else:
                debug_log("Received data: {0}", message)
                self.parent.on_message(message, None)

    def closed(self):
        debug_log("Transport connection has been closed abnormally.")
        self.terminated = True
        self.connected = False
        self.ssh.close()

        self.parent.drop_pending_calls()
        if self.parent.error_callback is not None:
            from freenas.dispatcher.client import ClientError
            self.parent.error_callback(ClientError.CONNECTION_CLOSED)
        out = self.stderr.readlines()
        if len(out):
            debug_log('Error in transport catcher')
            self.closed()
            raise RuntimeError(out)

    def close(self):
        debug_log("Transport connection closed by client.")
        self.terminated = True
        self.ssh.close()

    @property
    def address(self):
        return self.hostname

    @property
    def host_keys(self):
        return self.ssh.get_host_keys()


@client_transport('fd')
class ClientTransportFD(ClientTransport):
    def __init__(self, scheme):
        self.wlock = RLock()
        self.parent = None
        self.fd = -1
        self.fobj = None
        self.connected = True

    @property
    def address(self):
        return str(self.fd)

    def connect(self, url, parent, **kwargs):
        self.parent = parent
        if 'fobj' in kwargs:
            self.fobj = kwargs.pop('fobj')
        else:
            self.fd = int(url.hostname)
            self.fobj = os.fdopen(self.fd, 'w+b', 0)

        spawn_thread(self.recv)

    def send(self, message, fds):
        with self.wlock:
            try:
                header = struct.pack('II', 0xdeadbeef, len(message))
                message = message.encode('utf-8')
                self.fobj.write(header + message)
            except (OSError, ValueError) as err:
                debug_log("Send failed: {0}".format(err))
                self.doclose()
            else:
                debug_log("Sent data: {0}", message)

    def recv(self):
        while True:
            try:
                header = self.fobj.read(8)
                if header == b'' or len(header) != 8:
                    break

                magic, length = struct.unpack('II', header)
                if magic != 0xdeadbeef:
                    debug_log('Message with wrong magic dropped (magic {0:x})'.format(magic))
                    continue

                message = self.fobj.read(length)
                if message == b'' or len(message) != length:
                    break

                debug_log("Received data: {0}", message)
                self.parent.on_message(message)
            except OSError:
                break

        self.doclose()

    def doclose(self):
        try:
            os.close(self.fd)
        except OSError:
            pass
        finally:
            self.connected = False

    def close(self):
        self.doclose()


@client_transport('unix')
class ClientTransportUnix(ClientTransport):
    def __init__(self, scheme):
        self.path = '/var/run/dispatcher.sock'
        self.sock = None
        self.parent = None
        self.terminated = False
        self.creds_sent = False
        self.connected = False
        self.close_lock = RLock()
        self.wlock = RLock()

    def connect(self, url, parent, **kwargs):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.parent = parent
        if not self.parent:
            raise RuntimeError('ClientTransportUnix can be only created inside of a class')

        timeout = kwargs.get('timeout', 30)

        if url.path:
            self.path = url.path

        try:
            while True:
                try:
                    self.sock.connect(self.path)
                    self.connected = True
                    debug_log('Connected to {0}', self.path)
                    break
                except (socket.error, OSError) as err:
                    if err.errno == errno.EPERM:
                        raise

                    if timeout:
                        timeout -= 1
                        time.sleep(1)
                        continue
                    else:
                        self.close()
                        debug_log('Socket connection exception: {0}', err)
                    raise
        except KeyboardInterrupt:
            self.close()
            self.sock.close()
            raise

        spawn_thread(self.recv)

    @property
    def address(self):
        return self.path

    def send(self, message, fds):
        if self.terminated is False:
            with self.wlock:
                try:
                    header = struct.pack('II', 0xdeadbeef, len(message))
                    message = message.encode('utf-8')
                    ancdata = []

                    if not self.creds_sent:
                        ancdata.append((socket.SOL_SOCKET, socket.SCM_CREDS, bytearray(CMSGCRED_SIZE)))

                    if fds:
                        ancdata.append((socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [i.fd for i in fds])))

                    xsendmsg(self.sock, header + message, ancdata)
                    self.creds_sent = True

                    for i in fds:
                        if i.close:
                            os.close(i.fd)

                except (OSError, ValueError) as err:
                    debug_log("Send failed: {0}".format(err))
                    self.connected = False
                    self.sock.shutdown(socket.SHUT_RDWR)
                else:
                    debug_log("Sent data: {0}", message)

    def recv(self):
        while self.terminated is False:
            try:
                fds = array.array('i')
                header, ancdata = xrecvmsg(
                    self.sock, 8,
                    socket.CMSG_SPACE(MAXFDS * fds.itemsize) + socket.CMSG_SPACE(CMSGCRED_SIZE)
                )

                if header == b'' or len(header) != 8:
                    break

                magic, length = struct.unpack('II', header)
                if magic != 0xdeadbeef:
                    debug_log('Message with wrong magic dropped (magic {0:x})'.format(magic))
                    continue

                message, _, = xrecvmsg(self.sock, length)
                if message == b'' or len(message) != length:
                    break

                debug_log("Received data: {0}", message)
                for cmsg_level, cmsg_type, cmsg_data in ancdata:
                    if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_CREDS:
                        pid, uid, euid, gid = struct.unpack('iiii', cmsg_data[:struct.calcsize('iiii')])
                        self.parent.credentials = {
                            'pid': pid,
                            'uid': uid,
                            'euid': euid,
                            'gid': gid
                        }

                    if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
                        fds.fromstring(cmsg_data[:len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])

                self.parent.on_message(message, fds=fds)
            except OSError:
                break

        try:
            self.sock.close()
        except OSError:
            pass

        if self.terminated is False:
            self.closed()

    def close(self):
        debug_log("Disconnected.")
        self.terminated = True
        self.connected = False
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def closed(self):
        with self.close_lock:
            if self.terminated is False:
                debug_log("Transport socket connection terminated abnormally.")
                self.terminated = True
                self.parent.on_close('Going away')


@server_transport('unix')
class ServerTransportUnix(ServerTransport):
    class UnixSocketHandler(object):
        def __init__(self, server, connfd, address):
            self.connfd = connfd
            self.address = address
            self.server = server
            self.client_address = ("unix", 0)
            self.conn = None
            self.creds_sent = False
            self.wlock = RLock()

        def send(self, message, fds=None):
            if fds is None:
                fds = []

            with self.wlock:
                data = message.encode('utf-8')
                header = struct.pack('II', 0xdeadbeef, len(data))
                try:
                    fd = self.connfd.fileno()
                    ancdata = []
                    if fd == -1:
                        return

                    if not self.creds_sent:
                        ancdata.append((socket.SOL_SOCKET, socket.SCM_CREDS, bytearray(CMSGCRED_SIZE)))

                    if fds:
                        ancdata.append((socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [i.fd for i in fds])))

                    r, w, x = select.select([], [fd], [], 10)
                    if fd not in w:
                        raise OSError(errno.ETIMEDOUT, 'Operation timed out')

                    xsendmsg(self.connfd, header + data, ancdata)
                    self.creds_sent = True

                    for i in fds:
                        if i.close:
                            try:
                                os.close(i.fd)
                            except OSError:
                                pass

                except (OSError, ValueError, socket.timeout) as err:
                    self.server.logger.info('Send failed: {0}; closing connection'.format(str(err)))
                    if err.errno not in (errno.EBADF, errno.EPIPE):
                        self.connfd.shutdown(socket.SHUT_RDWR)

        def handle_connection(self):
            self.conn.on_open()

            while True:
                try:
                    fds = array.array('i')
                    header, ancdata = xrecvmsg(
                        self.connfd, 8,
                        socket.CMSG_SPACE(MAXFDS * fds.itemsize) + socket.CMSG_SPACE(CMSGCRED_SIZE)
                    )

                    if header == b'' or len(header) != 8:
                        if len(header) > 0:
                            self.server.logger.info('Short read (len {0})'.format(len(header)))
                        break

                    magic, length = struct.unpack('II', header)
                    if magic != 0xdeadbeef:
                        self.server.logger.info('Message with wrong magic dropped (magic {0:x})'.format(magic))
                        break

                    msg, _ = xrecvmsg(self.connfd, length)
                    if msg == b'' or len(msg) != length:
                        self.server.logger.info('Message with wrong length dropped; closing connection')
                        break

                    for cmsg_level, cmsg_type, cmsg_data in ancdata:
                        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_CREDS:
                            pid, uid, euid, gid = struct.unpack('iiii', cmsg_data[:struct.calcsize('iiii')])
                            self.client_address = ('unix', pid)
                            self.conn.credentials = {
                                'pid': pid,
                                'uid': uid,
                                'euid': euid,
                                'gid': gid
                            }

                        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
                            fds.fromstring(cmsg_data[:len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])

                except (OSError, ValueError) as err:
                    self.server.logger.info('Receive failed: {0}; closing connection'.format(str(err)), exc_info=True)
                    break

                self.conn.on_message(msg, fds=fds)

            self.close()

        def close(self):
            if self.conn:
                self.conn.on_close('Bye bye')
                self.conn = None
                try:
                    self.connfd.shutdown(socket.SHUT_RDWR)
                    self.connfd.close()
                except OSError:
                    pass

    def __init__(self, scheme, parsed_url, permissions=0o775):
        self.path = parsed_url.path
        self.sockfd = None
        self.backlog = 50
        self.permissions = permissions
        self.logger = logging.getLogger('UnixSocketServer')
        self.connections = []

    def broadcast_event(self, event, args):
        for i in self.connections:
            i.emit_event(event, args)

    def serve_forever(self, server):
        try:
            if os.path.exists(self.path):
                os.unlink(self.path)

            self.sockfd = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sockfd.bind(self.path)
            os.chmod(self.path, self.permissions)
            self.sockfd.listen(self.backlog)
        except OSError as err:
            self.logger.error('Cannot start socket server: {0}'.format(str(err)))
            return

        while True:
            try:
                fd, addr = self.sockfd.accept()
            except OSError as err:
                if err.errno == errno.ECONNABORTED:
                    break

                self.logger.error('accept() failed: {0}'.format(str(err)))
                break

            handler = self.UnixSocketHandler(self, fd, addr)
            handler.conn = server.on_connection(handler)
            spawn_thread(handler.handle_connection, threadpool=True)

        self.sockfd.close()

    def close(self):
        self.sockfd.shutdown(socket.SHUT_RDWR)
