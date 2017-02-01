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

import fnmatch
import re
import contextlib
import threading
from urllib.parse import urlsplit
from freenas.dispatcher.rpc import RpcContext
from freenas.dispatcher.client import Connection
from freenas.dispatcher.transport import ServerTransport


def match_event(name, pat):
    if isinstance(pat, str):
        return fnmatch.fnmatch(name, pat)

    if isinstance(pat, re._pattern_type):
        return pat.match(name) is not None


class ServerConnection(Connection):
    def __init__(self, parent):
        super(ServerConnection, self).__init__()
        self.parent = parent
        self.streaming = False
        self.event_masks = set()
        self.event_subscription_lock = threading.Lock()

    def on_open(self):
        if self.parent.channel_serializer:
            self.channel_serializer = self.parent.channel_serializer

        super(ServerConnection, self).on_open()
        self.parent.connections.append(self)

    def on_close(self, reason):
        super(ServerConnection, self).on_close(reason)
        self.drop_pending_calls()

        with contextlib.suppress(ValueError):
            self.parent.connections.remove(self)

    def on_events_subscribe(self, id, event_masks):
        if not isinstance(event_masks, list):
            return

        with self.event_subscription_lock:
            self.event_masks = set.union(self.event_masks, set(event_masks))

    def on_events_unsubscribe(self, id, event_masks):
        if not isinstance(event_masks, list):
            return

        with self.event_subscription_lock:
            self.event_masks = set.difference(self.event_masks, set(event_masks))

    def emit_event(self, name, params):
        if any(match_event(name, i) for i in list(self.event_masks)):
            super(ServerConnection, self).emit_event(name, params)


class Server(object):
    def __init__(self, context=None, connection_class=ServerConnection):
        self.server_transport = None
        self.connection_class = connection_class
        self.parsed_url = None
        self.scheme = None
        self.streaming = False
        self.transport = None
        self.rpc = None
        self.channel_serializer = None
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

    def close(self):
        self.transport.close()

    def on_connection(self, handler):
        conn = self.connection_class(self)
        conn.transport = handler
        if not conn.rpc:
            conn.rpc = self.rpc

        if self.streaming:
            conn.streaming = self.streaming

        return conn

    def broadcast_event(self, event, args):
        for i in self.connections:
            i.emit_event(event, args)
