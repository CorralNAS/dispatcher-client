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
import logging
from .jsonenc import dumps, loads
from threading import RLock, Event, Condition
from queue import Queue
from freenas.dispatcher import rpc
from freenas.utils.spawn_thread import spawn_thread
from freenas.dispatcher.transport import ClientTransport
from freenas.dispatcher.fd import replace_fds, collect_fds
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


class StreamingResultIterator(object):
    def __init__(self, client, call):
        self.client = client
        self.call = call
        self.q = call.queue

    def __iter__(self):
        return self

    def __next__(self):
        with self.call.cv:
            # Wait for initial response
            self.call.cv.wait_for(lambda: self.call.seqno > 0)

        if self.q.empty():
            # Request new fragment
            self.client.call_continue(self.call.id, True)

        v = self.q.get()
        if not v:
            raise StopIteration

        return v


class Connection(object):
    class PendingCall(object):
        def __init__(self, id, method, args=None):
            self.id = id
            self.method = method
            self.args = list(args) if args is not None else None
            self.result = None
            self.error = None
            self.ready = Event()
            self.callback = None
            self.queue = Queue()
            self.seqno = 0
            self.cv = Condition()

    def __init__(self):
        self.transport = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.rlock = RLock()
        self.rpc = None
        self.token = None
        self.pending_iterators = {}
        self.pending_calls = {}
        self.default_timeout = 20
        self.event_callback = None
        self.error_callback = None
        self.rpc_callback = None
        self.pending_events = []
        self.event_handlers = {}
        self.event_distribution_lock = RLock()
        self.event_emission_lock = RLock()
        self.last_event_burst = None
        self.event_cv = Event()
        self.event_thread = None
        self.streaming = False

    def __process_event(self, name, args):
        with self.event_distribution_lock:
            if name in self.event_handlers:
                for h in self.event_handlers[name]:
                    h(args)

            if self.event_callback:
                self.event_callback(name, args)

    def trace(self, msg):
        pass

    def pack(self, namespace, name, args=None, id=None):
        fds = list(collect_fds(args))
        try:
            result = dumps({
                'namespace': namespace,
                'name': name,
                'args': args,
                'id': str(id if id is not None else uuid.uuid4())
            })
            return result, fds
        except UnicodeEncodeError:
            raise

    def wait_for_call(self, call, timeout=None):
        elapsed = 0
        while timeout is None or elapsed < timeout:
            if call.ready.wait(1):
                return True

            elapsed += 1

        return False

    def call(self, pending_call, call_type='call', custom_payload=None):
        if custom_payload is None:
            payload = {
                'method': pending_call.method,
                'args': pending_call.args,
            }
        else:
            payload = custom_payload

        self.send(
            'rpc',
            call_type,
            payload,
            pending_call.id
        )

    def send_event(self, name, params):
        self.send(
            'events',
            'event',
            {'name': name, 'args': params}
        )

    def send_error(self, id, errno, msg, extra=None):
        payload = {
            'code': errno,
            'message': msg
        }

        if extra is not None:
            payload.update(extra)

        self.send('rpc', 'error', id=id, args=payload)

    def send_call(self, id, method, args):
        self.send('rpc', 'call', id=id, args={'method': method, 'args': args})

    def send_response(self, id, resp):
        self.send('rpc', 'response', id=id, args=resp)

    def send_fragment(self, id, seqno, fragment):
        self.send('rpc', 'fragment', id=id, args={'seqno': seqno, 'fragment': fragment})

    def send_end(self, id, seqno):
        self.send('rpc', 'end', id=id, args=seqno)

    def send_continue(self, id, seqno):
        self.send('rpc', 'continue', id=id, args=seqno)

    def send_abort(self, id):
        self.send('rpc', 'abort', id=id)

    def send(self, *args, **kwargs):
        self.send_raw(*self.pack(*args, **kwargs))

    def send_raw(self, data, fds=None):
        if not fds:
            fds = []

        debug_log('<- {0} [{1}]', data, fds)
        with self.rlock:
            self.transport.send(data, fds)

    def on_open(self):
        pass

    def on_close(self, reason):
        pass

    def on_message(self, message, *args, **kwargs):
        fds = kwargs.pop('fds', [])
        debug_log('-> {0}', str(message))

        if not type(message) is bytes:
            return

        try:
            message = loads(message.decode('utf-8'))
        except ValueError:
            self.send_error(None, errno.EINVAL, 'Request is not valid JSON')
            return

        replace_fds(message, fds)

        if 'namespace' not in message or 'name' not in message:
            self.send_error(None, errno.EINVAL, 'Invalid request')
            return

        try:
            method = getattr(self, "on_{}_{}".format(message["namespace"], message["name"]))
        except AttributeError:
            self.send_error(None, errno.EINVAL, 'Invalid request')
            return

        method(message["id"], message["args"])

    def on_rpc_response(self, id, data):
        self.trace('RPC response: id={0}, data={1}'.format(id, data))
        if id in self.pending_calls.keys():
            call = self.pending_calls[id]
            call.result = data
            call.ready.set()
            if call.callback is not None:
                call.callback(data)

            del self.pending_calls[str(call.id)]
        else:
            if self.error_callback is not None:
                self.error_callback(ClientError.SPURIOUS_RPC_RESPONSE, id)

    def on_rpc_fragment(self, id, data):
        seqno = data['seqno']
        data = data['fragment']

        self.trace('RPC fragment: id={0}, seqno={1}, data={2}'.format(id, seqno, data))

        if id in self.pending_calls.keys():
            call = self.pending_calls[id]
            call.result = StreamingResultIterator(self, call)

            with call.cv:
                for i in data:
                    call.queue.put(i)
                call.seqno = seqno
                call.cv.notify()
                call.ready.set()

            if call.callback:
                if call.callback(data):
                    self.call_continue(id)

    def on_rpc_end(self, id, data):
        self.trace('RPC end: id={0}'.format(id))
        if id in self.pending_calls.keys():
            call = self.pending_calls[id]
            with call.cv:
                call.seqno = data
                call.queue.put(None)
                call.cv.notify()

            if call.callback:
                call.callback(None)

            call.ready.set()
            del self.pending_calls[str(call.id)]

    def on_rpc_error(self, id, data):
        if id in self.pending_calls.keys():
            call = self.pending_calls[id]
            call.result = None
            call.error = data
            call.ready.set()
            if call.callback is not None:
                call.callback(rpc.RpcException(obj=call.error))

            del self.pending_calls[str(call.id)]

        if self.error_callback is not None:
            self.error_callback(ClientError.RPC_CALL_ERROR)

    def on_rpc_call(self, id, data):
        if self.rpc is None:
            self.send_error(id, errno.EINVAL, 'Server functionality is not supported')
            return

        if not isinstance(self.rpc, rpc.RpcContext):
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
            except rpc.RpcException as err:
                self.trace('RPC error: id={0} code={0} message={1} extra={2}'.format(
                    id,
                    err.code,
                    err.message,
                    err.extra
                ))

                self.send_error(id, err.code, err.message)
            else:
                if isinstance(result, rpc.RpcStreamingResponse):
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

    def login_user(self, username, password, timeout=None, check_password=False, resource=None):
        call = self.PendingCall(uuid.uuid4(), 'auth')
        self.pending_calls[str(call.id)] = call
        self.call(call, call_type='auth', custom_payload={
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
        self.call(call, call_type='auth_service', custom_payload={'name': name})
        if call.error:
            raise rpc.RpcException(obj=call.error)

        self.wait_for_call(call, timeout)

    def login_token(self, token, timeout=None):
        call = self.PendingCall(uuid.uuid4(), 'auth')
        self.pending_calls[str(call.id)] = call
        self.call(call, call_type='auth_token', custom_payload={'token': token})
        self.wait_for_call(call, timeout)
        if call.error:
            raise rpc.RpcException(obj=call.error)

        self.token = call.result[0]

    def call_async(self, name, callback, *args, **kwargs):
        call = self.PendingCall(uuid.uuid4(), name, args)
        call.callback = callback
        self.pending_calls[str(call.id)] = call
        self.call(call)
        return call

    def call_sync(self, name, *args, **kwargs):
        timeout = kwargs.pop('timeout', self.default_timeout)
        call = self.PendingCall(uuid.uuid4(), name, args)
        self.pending_calls[str(call.id)] = call
        self.call(call)

        if not self.wait_for_call(call, timeout):
            if self.error_callback:
                self.error_callback(ClientError.RPC_CALL_TIMEOUT, method=call.method, args=call.args)

            raise rpc.RpcException(errno.ETIMEDOUT, 'Call timed out')

        if call.result is None and call.error is not None:
            raise rpc.RpcException(obj=call.error)

        return call.result

    def call_continue(self, id, sync=False):
        call = self.pending_calls[str(id)]
        with call.cv:
            seqno = call.seqno + 1
            self.send_continue(id, seqno)
            if sync:
                call.cv.wait_for(lambda: call.seqno == seqno)

    def enable_server(self, context=None):
        self.rpc = context or rpc.RpcContext()
        if context and isinstance(context, rpc.RpcContext):
            for name in context.services:
                self.call_sync('plugin.register_service', name)

    def on_events_event(self, id, data):
        spawn_thread(self.__process_event, data['name'], data['args'], threadpool=True)

    def on_events_event_burst(self, id, data):
        for i in data['events']:
            spawn_thread(self.__process_event, i['name'], i['args'], threadpool=True)

    def on_events_logout(self, id, data):
        self.error_callback(ClientError.LOGOUT)

    def on_event(self, callback):
        self.event_callback = callback

    def on_call(self, callback):
        self.rpc_callback = callback

    def on_error(self, callback):
        self.error_callback = callback

    def subscribe_events(self, *masks):
        self.send('events', 'subscribe', masks)

    def unsubscribe_events(self, *masks):
        self.send('events', 'unsubscribe', masks)

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
            self.send_event(name, params)
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

    def drop_pending_calls(self):
        message = "Connection closed"
        for key, call in list(self.pending_calls.items()):
            call.result = None
            call.error = {
                "code": errno.ECONNABORTED,
                "message": message
            }
            call.ready.set()
            del self.pending_calls[key]

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


class Client(Connection):
    def __init__(self):
        super(Client, self).__init__()
        self.receive_thread = None
        self.scheme = None
        self.transport = None
        self.parsed_url = None

    def wait_forever(self):
        while True:
            time.sleep(60)

    def on_close(self, reason):
        self.drop_pending_calls()
        if self.error_callback is not None:
            self.error_callback(ClientError.CONNECTION_CLOSED)

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

        self.transport = ClientTransport(self.parsed_url.scheme)
        self.transport.connect(self.parsed_url, self, **kwargs)
        debug_log('Connection opened, local address {0}', self.transport.address)

    def disconnect(self):
        debug_log('Closing connection, local address {0}', self.transport.address)
        self.drop_pending_calls()
        self.transport.close()
