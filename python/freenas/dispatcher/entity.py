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
from freenas.utils.query import wrap
from freenas.dispatcher.rpc import RpcException


if os.getenv("DISPATCHERCLIENT_TYPE") == "GEVENT":
    from gevent.queue import Queue
else:
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
    def __init__(self, client, name, maxsize=2000):
        self.client = client
        self.name = name
        self.event_handler = None
        self.items = CappedDict(maxsize)
        self.on_add = None
        self.on_update = None
        self.on_delete = None
        self.on_error = None
        self.remote = False
        self.listeners = {}

    def __on_changed(self, args, event=True):
        if args['operation'] == 'create':
            self.__add(args['entities'], event)
            return

        if args['operation'] == 'update':
            self.__update(args['entities'], event)
            return

        if args['operation'] == 'delete':
            self.__delete(args['ids'], event)
            return

        if args['operation'] == 'rename':
            self.__rename(args['ids'], event)
            return

    def __add(self, items, event=True):
        if items is None:
            return

        if isinstance(items, RpcException):
            if callable(self.on_error):
                self.on_error(items)
            return

        for i in items:
            self.items[i['id']] = i
            if callable(self.on_add) and event:
                self.on_add(i)

            if len(self.items) == self.items.maxsize:
                self.remote = True

    def __update(self, items, event=True):
        for i in items:
            self.update(i, event)

    def __delete(self, ids, event=True):
        for i in ids:
            if callable(self.on_delete) and event:
                self.on_delete(self.items[i])

            del self.items[i]

    def __rename(self, ids, event=True):
        for old, new in ids:
            oldi = self.items[old]
            newi = copy.deepcopy(oldi)
            newi['id'] = new

            self.items[new] = newi

            if callable(self.on_update) and event:
                self.on_update(oldi, newi)

            del self.items[old]

    def __len__(self):
        return len(self.items)

    def start(self):
        self.client.call_async(
            '{0}.query'.format(self.name),
            lambda x: self.__add(x, False), [],
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

        return wrap(list(self.items.values())).query(*filter, **params)

    def viewport(self, *filter, **params):
        return wrap(list(self.items.values())).query(*filter, **params)

    def update(self, obj, event=True):
        oldobj = self.items.get(obj['id'])
        if not oldobj:
            return

        self.items[obj['id']] = obj
        if callable(self.on_update) and event:
            self.on_update(oldobj, obj)
            if obj['id'] in self.listeners:
                for i in self.listeners[obj['id']]:
                    i.put(('update', oldobj, obj))

    def listen(self, id):
        q = Queue(1)
        self.listeners.setdefault(id, []).append(q)
        while True:
            try:
                yield q.get()
            except GeneratorExit:
                self.listeners[id].remove(q)
                raise StopIteration
