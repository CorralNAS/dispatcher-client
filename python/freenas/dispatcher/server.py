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

from urllib.parse import urlsplit
from freenas.dispatcher.rpc import RpcContext
from freenas.dispatcher.client import Connection
from freenas.dispatcher.transport import ServerTransport


class ServerConnection(Connection):
    def __init__(self, parent):
        super(ServerConnection, self).__init__()
        self.parent = parent
        self.streaming = False

    def on_open(self):
        self.parent.connections.append(self)

    def on_close(self, reason):
        for i in self.pending_iterators.values():
            i.close()

        self.parent.connections.remove(self)


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
