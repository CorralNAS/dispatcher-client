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

from __future__ import print_function
import os
import enum
import uuid
import errno
import time
from .jsonenc import dumps, loads
from threading import RLock, Event
from queue import Queue
from freenas.dispatcher import rpc
from freenas.utils.spawn_thread import spawn_thread
from freenas.dispatcher.client_transport import ClientTransportBuilder
from freenas.dispatcher.fd import FileDescriptor, replace_fds, collect_fds
from ws4py.compat import urlsplit


class ClientError(enum.Enum):
    INVALID_JSON_RESPONSE = 1
    CONNECTION_TIMEOUT = 2
    CONNECTION_CLOSED = 3
    RPC_CALL_TIMEOUT = 4
    RPC_CALL_ERROR = 5
    SPURIOUS_RPC_RESPONSE = 6
    LOGOUT = 7
    OTHER = 8


_debug_log_file = None


def debug_log(message, *args):
    global _debug_log_file

    if os.getenv('DISPATCHER_CLIENT_DEBUG'):
        if not _debug_log_file:
            try:
                _debug_log_file = open('/var/tmp/dispatcherclient.{0}.log'.format(os.getpid()), 'w')
            except OSError:
                pass

        print(message.format(*args), file=_debug_log_file)
        _debug_log_file.flush()


class Client(object):
    class StreamingResultIterator(object):
        def __init__(self, queue):
            self.q = queue

        def __iter__(self):
            return self

        def __next__(self):
            v = self.q.get()
            if not v:
                raise StopIteration

            return v

    class PendingCall(object):
        def __init__(self, id, method, args=None):
            self.id = id
            self.method = method
            self.args = list(args) if args is not None else None
            self.result = None
            self.error = None
            self.completed = Event()
            self.callback = None
            self.streaming = False
            self.queue = None

    class SubscribedEvent(object):
        def __init__(self, name, *filters):
            self.name = name
            self.refcount = 0
            self.filters = filters

        def match(self, name, args):
            if self.name != name:
                return False

            if self.filters:
                return match(args, *self.filters)

    def __init__(self):
        self.pending_calls = {}
        self.pending_events = []
        self.event_handlers = {}
        self.rpc = None
        self.event_callback = None
        self.error_callback = None
        self.rpc_callback = None
        self.receive_thread = None
        self.token = None
        self.event_distribution_lock = RLock()
        self.event_emission_lock = RLock()
        self.default_timeout = 20
        self.scheme = None
        self.transport = None
        self.parsed_url = None
        self.last_event_burst = None
        self.use_bursts = False
        self.event_cv = Event()
        self.event_thread = None

    def __pack(self, namespace, name, args, id=None):
        fds = list(collect_fds(args))
        return dumps({
            'namespace': namespace,
            'name': name,
            'args': args,
            'id': str(id if id is not None else uuid.uuid4())
        }), fds

    def __call_timeout(self, call):
        pass

    def __call(self, pending_call, call_type='call', custom_payload=None):
        if custom_payload is None:
            payload = {
                'method': pending_call.method,
                'args': pending_call.args,
            }
        else:
            payload = custom_payload

        if pending_call.streaming:
            pending_call.queue = Queue()
            pending_call.result = self.StreamingResultIterator(pending_call.queue)

        self.__send(*self.__pack(
            'rpc',
            call_type,
            payload,
            pending_call.id
        ))

    def __send_event(self, name, params):
        self.__send(*self.__pack(
            'events',
            'event',
            {'name': name, 'args': params}
        ))

    def __send_event_burst(self):
        with self.event_emission_lock:
            self.__send(*self.__pack(
                'events',
                'event_burst',
                {'events': list([{'name': t[0], 'args': t[1]} for t in self.pending_events])},
            ))

            del self.pending_events[:]

    def __send_error(self, id, errno, msg, extra=None):
        payload = {
            'code': errno,
            'message': msg
        }

        if extra is not None:
            payload.update(extra)

        self.__send(*self.__pack('rpc', 'error', id=id, args=payload))

    def __send_response(self, id, resp):
        self.__send(*self.__pack('rpc', 'response', id=id, args=resp))

    def __send(self, data, fds=None):
        if not fds:
            fds = []

        debug_log('<- {0} [{1}]', data, fds)
        self.transport.send(data, fds)

    def recv(self, message, fds):
        if isinstance(message, bytes):
            message = message.decode('utf-8')
        debug_log('-> {0}', message)
        try:
            msg = loads(message)
        except ValueError as err:
            if self.error_callback is not None:
                self.error_callback(ClientError.INVALID_JSON_RESPONSE, err)
            return

        self.decode(msg, fds)

    def __process_event(self, name, args):
        with self.event_distribution_lock:
            if name in self.event_handlers:
                for h in self.event_handlers[name]:
                    h(args)

            if self.event_callback:
                self.event_callback(name, args)

    def __event_emitter(self):
        while True:
            self.event_cv.wait()

            while len(self.pending_events) > 0:
                time.sleep(0.1)
                with self.event_emission_lock:
                    self.__send_event_burst()

    def wait_forever(self):
        while True:
            time.sleep(60)

    def wait_for_call(self, call, timeout=None):
        elapsed = 0
        while timeout is None or elapsed < timeout:
            if call.completed.wait(1):
                return True

            elapsed += 1

        return False

    def drop_pending_calls(self):
        message = "Connection closed"
        for key, call in list(self.pending_calls.items()):
            call.result = None
            call.error = {
                "code":  errno.ECONNABORTED,
                "message": message
            }
            call.completed.set()
            del self.pending_calls[key]

    def decode(self, msg, fds):
        replace_fds(msg, fds)

        if 'namespace' not in msg:
            self.error_callback(ClientError.INVALID_JSON_RESPONSE)
            return

        if 'name' not in msg:
            self.error_callback(ClientError.INVALID_JSON_RESPONSE)
            return

        if msg['namespace'] == 'events' and msg['name'] == 'event':
            args = msg['args']
            spawn_thread(self.__process_event, args['name'], args['args'], threadpool=True)
            return

        if msg['namespace'] == 'events' and msg['name'] == 'event_burst':
            args = msg['args']
            for i in args['events']:
                spawn_thread(self.__process_event, i['name'], i['args'], threadpool=True)
            return

        if msg['namespace'] == 'events' and msg['name'] == 'logout':
            self.error_callback(ClientError.LOGOUT)
            return

        if msg['namespace'] == 'rpc':
            if msg['name'] == 'call':
                if self.rpc is None:
                    self.__send_error(msg['id'], errno.EINVAL, 'Server functionality is not supported')
                    return

                if 'args' not in msg:
                    self.__send_error(msg['id'], errno.EINVAL, 'Malformed request')
                    return

                args = msg['args']
                if 'method' not in args or 'args' not in args:
                    self.__send_error(msg['id'], errno.EINVAL, 'Malformed request')
                    return

                def run_async(msg, args):
                    try:
                        result = self.rpc.dispatch_call(args['method'], args['args'], sender=self)
                    except rpc.RpcException as err:
                        self.__send_error(msg['id'], err.code, err.message)
                    else:
                        self.__send_response(msg['id'], result)

                spawn_thread(run_async, msg, args, threadpool=True)
                return

            if msg['name'] == 'response':
                if msg['id'] in self.pending_calls.keys():
                    call = self.pending_calls[msg['id']]
                    call.result = msg['args']
                    call.completed.set()
                    if call.callback is not None:
                        call.callback(msg['args'])

                    del self.pending_calls[str(call.id)]
                else:
                    if self.error_callback is not None:
                        self.error_callback(ClientError.SPURIOUS_RPC_RESPONSE, msg['id'])

            if msg['name'] == 'response_fragment':
                if msg['id'] in self.pending_calls.keys():
                    call = self.pending_calls[msg['id']]
                    if call.streaming:
                        for i in msg['args']:
                            call.queue.put(i)
                    else:
                        if call.result is None:
                            call.result = []

                        call.result += msg['args']

                    if call.streaming and call.callback:
                        call.callback(msg['args'])

            if msg['name'] == 'response_end':
                if msg['id'] in self.pending_calls.keys():
                    call = self.pending_calls[msg['id']]
                    if call.streaming:
                        call.queue.put(None)

                    if call.callback:
                        if call.streaming:
                            call.callback(None)
                        else:
                            call.callback(call.result)

                    call.completed.set()
                    del self.pending_calls[str(call.id)]

            if msg['name'] == 'error':
                if msg['id'] in self.pending_calls.keys():
                    call = self.pending_calls[msg['id']]
                    call.result = None
                    call.error = msg['args']
                    call.completed.set()
                    if call.callback is not None:
                        call.callback(rpc.RpcException(obj=call.error))

                    del self.pending_calls[str(call.id)]

                if self.error_callback is not None:
                    self.error_callback(ClientError.RPC_CALL_ERROR)

    def parse_url(self, url):
        self.parsed_url = urlsplit(url, scheme="http")
        self.scheme = self.parsed_url.scheme

    def connect(self, url, **kwargs):
        self.parse_url(url)
        if not self.scheme:
            self.scheme = kwargs.get('scheme', "ws")
        else:
            if 'scheme' in kwargs:
                raise ValueError('Connection scheme cannot be delared in both url and arguments.')
        if self.scheme is "http":
            self.scheme = "ws"

        builder = ClientTransportBuilder()
        self.transport = builder.create(self.scheme)
        self.transport.connect(self.parsed_url, self, **kwargs)
        debug_log('Connection opened, local address {0}', self.transport.address)

        if self.use_bursts:
            self.event_thread = spawn_thread(self.__event_emitter)

    def login_user(self, username, password, timeout=None, check_password=False, resource=None):
        call = self.PendingCall(uuid.uuid4(), 'auth')
        self.pending_calls[str(call.id)] = call
        self.__call(call, call_type='auth', custom_payload={
            'username': username,
            'password': password,
            'check_password': check_password,
            'resource': resource
        })
        self.wait_for_call(call, timeout)
        if call.error:
            raise rpc.RpcException(obj=call.error)

        self.token = call.result[0]

    def login_service(self, name, timeout=None):
        call = self.PendingCall(uuid.uuid4(), 'auth')
        self.pending_calls[str(call.id)] = call
        self.__call(call, call_type='auth_service', custom_payload={'name': name})
        if call.error:
            raise rpc.RpcException(obj=call.error)

        self.wait_for_call(call, timeout)

    def login_token(self, token, timeout=None):
        call = self.PendingCall(uuid.uuid4(), 'auth')
        self.pending_calls[str(call.id)] = call
        self.__call(call, call_type='auth_token', custom_payload={'token': token})
        self.wait_for_call(call, timeout)
        if call.error:
            raise rpc.RpcException(obj=call.error)

        self.token = call.result[0]

    def disconnect(self):
        debug_log('Closing connection, local address {0}', self.transport.address)
        self.drop_pending_calls()
        self.transport.close()

    def enable_server(self):
        self.rpc = rpc.RpcContext()

    def on_event(self, callback):
        self.event_callback = callback

    def on_call(self, callback):
        self.rpc_callback = callback

    def on_error(self, callback):
        self.error_callback = callback

    def subscribe_events(self, *masks):
        self.__send(*self.__pack('events', 'subscribe', masks))

    def unsubscribe_events(self, *masks):
        self.__send(*self.__pack('events', 'unsubscribe', masks))

    def register_service(self, name, impl):
        if self.rpc is None:
            raise RuntimeError('Call enable_server() first')

        self.rpc.register_service_instance(name, impl)
        self.call_sync('plugin.register_service', name)

    def unregister_service(self, name):
        if self.rpc is None:
            raise RuntimeError('Call enable_server() first')

        self.rpc.unregister_service(name)
        self.call_sync('plugin.unregister_service', name)

    def resume_service(self, name):
        if self.rpc is None:
            raise RuntimeError('Call enable_server() first')

        self.call_sync('plugin.resume_service', name)

    def register_schema(self, name, schema):
        if self.rpc is None:
            raise RuntimeError('Call enable_server() first')

        self.call_sync('plugin.register_schema', name, schema)

    def unregister_schema(self, name):
        if self.rpc is None:
            raise RuntimeError('Call enable_server() first')

        self.call_sync('plugin.unregister_schema', name)

    def call_async(self, name, callback, *args, **kwargs):
        call = self.PendingCall(uuid.uuid4(), name, args)
        call.callback = callback
        call.streaming = kwargs.pop('streaming', False)
        self.pending_calls[str(call.id)] = call
        self.__call(call)
        return call

    def call_sync(self, name, *args, **kwargs):
        timeout = kwargs.pop('timeout', self.default_timeout)
        call = self.PendingCall(uuid.uuid4(), name, args)
        call.streaming = kwargs.pop('streaming', False)
        self.pending_calls[str(call.id)] = call
        self.__call(call)

        if call.streaming:
            return call.result

        if not self.wait_for_call(call, timeout):
            if self.error_callback:
                self.error_callback(ClientError.RPC_CALL_TIMEOUT, method=call.method, args=call.args)

            raise rpc.RpcException(errno.ETIMEDOUT, 'Call timed out')

        if call.result is None and call.error is not None:
            raise rpc.RpcException(obj=call.error)

        return call.result

    def call_task_sync(self, name, *args, timeout=3600):
        tid = self.call_sync('task.submit', name, list(args))
        self.call_sync('task.wait', tid, timeout=timeout)
        return self.call_sync('task.status', tid)

    def call_task_async(self, name, *args, timeout=3600, callback=None):
        def wait_on_complete(tid):
            self.call_sync('task.wait', tid, timeout=timeout)
            callback(self.call_sync('task.status', tid))

        tid = self.call_sync('task.submit', name, list(args))
        if callback:
            spawn_thread(wait_on_complete, tid)

        return tid

    def submit_task(self, name, *args):
        return self.call_sync('task.submit', name, list(args))

    def emit_event(self, name, params):
        if not self.use_bursts:
            self.__send_event(name, params)
        else:
            self.pending_events.append((name, params))
            self.event_cv.set()
            self.event_cv.clear()

    def register_event_handler(self, name, handler):
        if name not in self.event_handlers:
            self.event_handlers[name] = []

        self.event_handlers[name].append(handler)
        self.subscribe_events(name)
        return handler

    def unregister_event_handler(self, name, handler):
        self.event_handlers[name].remove(handler)

    def exec_and_wait_for_event(self, event, match_fn, fn, timeout=None):
        done = Event()
        self.subscribe_events(event)
        with self.event_distribution_lock:
            try:
                fn()
            except:
                raise

            def handler(args):
                if match_fn(args):
                    done.set()

            self.register_event_handler(event, handler)

        done.wait(timeout=timeout)
        self.unregister_event_handler(event, handler)

    def test_or_wait_for_event(self, event, match_fn, initial_condition_fn, timeout=None):
        done = Event()
        self.subscribe_events(event)
        with self.event_distribution_lock:
            if initial_condition_fn():
                return

            def handler(args):
                if match_fn(args):
                    done.set()

            self.register_event_handler(event, handler)

        done.wait(timeout=timeout)
        self.unregister_event_handler(event, handler)

    def get_lock(self, name):
        self.call_sync('lock.init', name)
        return rpc.ServerLockProxy(self, name)
