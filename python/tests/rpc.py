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
import socket
import unittest
import logging
from freenas.dispatcher.rpc import RpcService, generator
from freenas.dispatcher.client import Client, StreamingResultIterator


class TestService(RpcService):
    def hello(self, arg):
        return 'Hello World, {0}'.format(arg)

    @generator
    def iterator(self, count):
        return (i * 2 for i in range(0, count))

    @generator
    def maybe_iterator(self, value):
        pass

    @generator
    def generator(self, count):
        pass


class TestClientServer(unittest.TestCase):
    def setup_back_to_back(self, streaming=False):
        a, b = socket.socketpair()
        self.assertGreaterEqual(a.fileno(), 0)
        self.assertGreaterEqual(b.fileno(), 0)

        c1 = Client()
        c1._s = a
        c1.enable_server()
        c1.standalone_server = True

        if streaming:
            c1.streaming = True
            c1.rpc.streaming_enabled = True
        c1.register_service('test', TestService())
        c1.connect('fd://{0}'.format(a.fileno()))
        self.assertTrue(c1.connected)

        c2 = Client()
        c2._s = b
        c2.streaming = True
        c2.connect('fd://{0}'.format(b.fileno()))
        self.assertTrue(c2.connected)

        return c1, c2

    def test_hello(self):
        c1, c2 = self.setup_back_to_back()
        result = c2.call_sync('test.hello', 'freenas')
        self.assertEqual(result, 'Hello World, freenas')

    def test_iterator(self):
        c1, c2 = self.setup_back_to_back(True)
        result = c2.call_sync('test.iterator', 10)
        self.assertIsInstance(result, StreamingResultIterator)
        self.assertEqual(list(result), [0, 2, 4, 6, 8, 10, 12, 14, 16, 18])

    def test_iterator_compat(self):
        c1, c2 = self.setup_back_to_back(False)
        result = c2.call_sync('test.iterator', 10)
        self.assertIsInstance(result, list)
        self.assertEqual(result, [0, 2, 4, 6, 8, 10, 12, 14, 16, 18])


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
