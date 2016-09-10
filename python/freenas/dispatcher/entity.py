#
# Copyright 2015 iXsystems, Inc.
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

import os
import copy
from collections import OrderedDict
from threading import Condition
from freenas.utils import query as q
from freenas.dispatcher.rpc import RpcException


if os.getenv("DISPATCHERCLIENT_TYPE") == "GEVENT":
    from gevent.queue import Queue
    from gevent.event import Event
    from gevent import sleep
else:
    from threading import Event
    from time import sleep
    try:
        from queue import Queue
    except ImportError:
        from Queue import Queue


class CappedDict(OrderedDict):
    def __init__(self, maxsize):
        super(CappedDict, self).__init__()
        self.maxsize = maxsize

    def __setitem__(self, key, value):
        if len(self) == self.maxsize:
            self.popitem(last=False)
        super(CappedDict, self).__setitem__(key, value)


class EntitySubscriber(object):
    def __init__(self, client, name, maxsize=1000):
        self.client = client
        self.name = name
        self.event_handler = None
        self.items = CappedDict(maxsize)
        self.on_add = set()
        self.on_update = set()
        self.on_delete = set()
        self.on_error = set()
        self.remote = False
        self.ready = Event()
        self.cv = Condition()
        self.listeners = {}

    def __on_changed(self, args, event=True):
        if event:
            self.ready.wait()

        with self.cv:
            if args['operation'] == 'create':
                self.__add(args['entities'], event)

            if args['operation'] == 'update':
                self.__update(args['entities'], event)

            if args['operation'] == 'delete':
                self.__delete(args['ids'], event)

            if args['operation'] == 'rename':
                self.__rename(args['ids'], event)

            self.cv.notify_all()

    def __add(self, items, event=True):
        if items is None:
            return

        if isinstance(items, RpcException):
            for cbf in self.on_error:
                if callable(cbf):
                    cbf(items)
            return

        for i in items:
            self.items[i['id']] = i
            if event:
                for cbf in self.on_add:
                    cbf(i)

            if i['id'] in self.listeners:
                for q in self.listeners[i['id']]:
                    q.put(('create', i, i))

            if len(self.items) >= self.items.maxsize:
                self.remote = True

    def __update(self, items, event=True):
        for i in items:
            self.update(i, event)

    def __delete(self, ids, event=True):
        for i in ids:
            if event:
                for cbf in self.on_delete:
                    cbf(self.items[i])

            if i in self.listeners:
                for q in self.listeners[i]:
                    q.put(('delete', None, None))

            del self.items[i]

    def __rename(self, ids, event=True):
        for old, new in ids:
            oldi = self.items[old]
            newi = copy.deepcopy(oldi)
            newi['id'] = new

            self.items[new] = newi

            if event:
                for cbf in self.on_update:
                    cbf(oldi, newi)

            if oldi['id'] in self.listeners:
                for i in self.listeners[oldi['id']]:
                    i.put(('update', oldi, newi))

            del self.items[old]

    def __len__(self):
        return len(self.items)

    def start(self):
        def callback(result):
            with self.cv:
                self.__add(result, False)
                self.cv.notify_all()
                if result is None:
                    self.ready.set()

                return True

        self.client.call_async(
            '{0}.query'.format(self.name),
            callback, [],
            {'limit': self.items.maxsize},
            streaming=True
        )

        self.event_handler = self.client.register_event_handler(
            'entity-subscriber.{0}.changed'.format(self.name),
            self.__on_changed
        )

    def stop(self):
        self.client.unregister_event_handler(
            'entity-subscriber.{0}.changed'.format(self.name),
            self.event_handler
        )

    def query(self, *filter, **params):
        if self.remote:
            return self.client.call_sync('{0}.query'.format(self.name), filter, params)

        with self.cv:
            return q.query(list(self.items.values()), *filter, **params)

    def get(self, id, timeout=None):
        if self.remote:
            return self.query(('id', '=', id), single=True)

        with self.cv:
            if not self.cv.wait_for(lambda: id in self.items, timeout):
                return None

            return self.items.get(id)

    def viewport(self, *filter, **params):
        with self.cv:
            return q.query(list(self.items.values()), *filter, **params)

    def update(self, obj, event=True):
        oldobj = self.items.get(obj['id'])
        if not oldobj:
            return

        self.items[obj['id']] = obj

        if event:
            for cbf in self.on_update:
                cbf(oldobj, obj)

        if obj['id'] in self.listeners:
            for i in self.listeners[obj['id']]:
                i.put(('update', oldobj, obj))

    def listen(self, id):
        q = Queue()
        self.listeners.setdefault(id, []).append(q)
        try:
            while True:
                yield q.get()
        finally:
            self.listeners[id].remove(q)

    def wait_ready(self, timeout=None):
        self.ready.wait(timeout)
