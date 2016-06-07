#
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

import errno
from urllib.parse import urlsplit
from freenas.dispatcher.rpc import RpcContext, RpcException, RpcStreamingResponse
from freenas.dispatcher.client import Connection
from freenas.dispatcher.transport import ServerTransport
from freenas.utils.spawn_thread import spawn_thread


class PendingIterator(object):
    def __init__(self, iter):
        self.iter = iter
        self.seqno = 0

    def advance(self):
        try:
            val = next(self.iter)
        except StopIteration:
            raise StopIteration(self.seqno + 1)

        self.seqno += 1
        return val, self.seqno

    def close(self):
        self.iter.close()


class ServerConnection(Connection):
    def __init__(self, parent):
        super(ServerConnection, self).__init__()
        self.parent = parent
        self.streaming = False
        self.pending_iterators = {}
        self.rpc = None

    def on_open(self):
        self.parent.connections.append(self)

    def on_close(self, reason):
        for i in self.pending_iterators.values():
            i.close()

        self.parent.connections.remove(self)

    def on_rpc_call(self, id, data):
        if self.rpc is None:
            self.send_error(id, errno.EINVAL, 'Server functionality is not supported')
            return

        if not isinstance(self.rpc, RpcContext):
            self.send_error(id, errno.EINVAL, 'Incompatible context')
            return

        if 'method' not in data or 'args' not in data:
            self.send_error(id, errno.EINVAL, 'Malformed request')
            return

        def run_async(id, args):
            try:
                result = self.rpc.dispatch_call(
                    args['method'],
                    args['args'],
                    sender=self,
                    streaming=self.streaming
                )
            except RpcException as err:
                self.trace('RPC error: id={0} code={0} message={1} extra={2}'.format(
                    id,
                    err.code,
                    err.message,
                    err.extra
                ))

                self.send_error(id, err.code, err.message)
            else:
                if isinstance(result, RpcStreamingResponse):
                    self.pending_iterators[id] = PendingIterator(result)
                    try:
                        first, seqno = self.pending_iterators[id].advance()
                        self.trace('RPC response fragment: id={0} seqno={1} result={2}'.format(id, seqno, first))
                        self.send_fragment(id, seqno, first)
                    except StopIteration as stp:
                        self.trace('RPC response end: id={0}'.format(id))
                        self.send_end(id, stp.args[0])
                        del self.pending_iterators[id]
                        return
                else:
                    self.trace('RPC response: id={0} result={1}'.format(id, result))
                    self.send_response(id, result)

        self.trace('RPC call: id={0} method={1} args={2}'.format(id, data['method'], data['args']))
        spawn_thread(run_async, id, data, threadpool=True)
        return

    def on_rpc_continue(self, id, data):
        self.trace('RPC continuation: id={0}, seqno={1}'.format(id, data))

        if id not in self.pending_iterators:
            self.trace('RPC pending call {0} not found'.format(id))
            self.send_error(id, errno.ENOENT, 'Invalid call')
            return

        try:
            fragment, seqno = self.pending_iterators[id].advance()
            self.trace('RPC response fragment: id={0} seqno={1} result={2}'.format(id, seqno, fragment))
            self.send_fragment(id, seqno, fragment)
        except StopIteration as stp:
            self.trace('RPC response end: id={0} seqno={1}'.format(id, stp.args[0]))
            self.send_end(id, stp.args[0])
            del self.pending_iterators[id]
            return

    def on_rpc_abort(self, id, data):
        self.trace('RPC abort: id={0}'.format(id))

        if id not in self.pending_iterators:
            self.trace('RPC pending call {0} not found'.format(id))
            self.send_error(id, errno.ENOENT, 'Invalid call')
            return

        try:
            self.pending_iterators[id].close()
            del self.pending_iterators[id]
        except BaseException as err:
            pass


class Server(object):
    def __init__(self, context=None, connection_class=ServerConnection):
        self.server_transport = None
        self.connection_class = connection_class
        self.parsed_url = None
        self.scheme = None
        self.transport = None
        self.rpc = None
        self.context = context or RpcContext()
        self.connections = []

    def parse_url(self, url):
        self.parsed_url = urlsplit(url)
        self.scheme = self.parsed_url.scheme

    def start(self, url=None, transport_options=None):
        self.parse_url(url)
        self.transport = ServerTransport(
            self.parsed_url.scheme,
            self.parsed_url,
            **(transport_options or {})
        )

    def serve_forever(self):
        self.transport.serve_forever(self)

    def on_connection(self, handler):
        conn = self.connection_class(self)
        conn.transport = handler
        if not conn.rpc:
            conn.rpc = self.rpc

        return conn

    def broadcast_event(self, event, args):
        for i in self.connections:
            i.send_event(event, args)
