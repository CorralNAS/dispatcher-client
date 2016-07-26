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

import os
import time
import socket
import unittest
import threading
from freenas.dispatcher.rpc import RpcService, RpcContext
from freenas.dispatcher.client import Client
from freenas.dispatcher.server import Server


class TestService(RpcService):
    def initialize(self, context):
        pass

    def hello(self, arg):
        return 'Hello World, {0}'.format(arg)


class TestClientServer(unittest.TestCase):
    def test_unix_server(self):
        sockpath = os.path.join(os.getcwd(), 'test.{0}.sock'.format(os.getpid()))
        sockurl = 'unix://' + sockpath

        context = RpcContext()
        context.register_service('test', TestService)
        server = Server()
        server.rpc = context
        server.start(sockurl)
        threading.Thread(target=server.serve_forever, daemon=True).start()

        # Spin until server is ready
        while not os.path.exists(sockpath):
            time.sleep(0.5)

        client = Client()
        client.connect(sockurl)
        self.assertEqual(client.call_sync('test.hello', 'freenas'), 'Hello World, freenas')

    def test_back_to_back(self):
        a, b = socket.socketpair()
        c1 = Client()
        c1.standalone_server = True
        c1.enable_server()
        c1.register_service('test', TestService())
        c1.connect('fd://{0}'.format(a.fileno()))
        c2 = Client()
        c2.connect('fd://{0}'.format(b.fileno()))
        self.assertEqual(c2.call_sync('test.hello', 'freenas'), 'Hello World, freenas')
        c2.disconnect()


if __name__ == '__main__':
    unittest.main()
