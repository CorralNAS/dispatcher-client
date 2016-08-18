#
# Copyright 2016 iXsystems, Inc.
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

from urllib.parse import urlparse
from freenas.dispatcher.transport import ClientTransport


class Bridge(object):
    class Wrapper(object):
        def __init__(self, parent, index):
            self.opposite_transport = parent.transports[(index + 1) % 2]

        def on_message(self, msg, fds=None):
            self.opposite_transport.send(msg.decode('utf-8'), fds or [])

        def on_close(self, reason):
            self.opposite_transport.close()

    def __init__(self):
        self.wrappers = None
        self.transports = None

    def start(self, uri1, uri2):
        """ Opens a bridge between two endpoints

        Args:
            uri1 (str): The uri for the first endpoint
            uri2 (str): The uri for the second endpoint
        """
        uris = [urlparse(uri1), urlparse(uri2)]
        self.transports = [
            ClientTransport(uris[0].scheme),
            ClientTransport(uris[1].scheme)
        ]
        self.wrappers = [
            self.Wrapper(self, 0),
            self.Wrapper(self, 1)
        ]

        for idx, i in enumerate(self.transports):
            i.connect(uris[idx], self.wrappers[idx])
