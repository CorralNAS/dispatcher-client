#+
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

from freenas.utils import first_or_default


class FileDescriptor(object):
    def __init__(self, fd=None, close=True):
        self.fd = fd
        self.close = close

    def __str__(self):
        return "<FileDescriptor fd={0}>".format(self.fd)

    def __repr__(self):
        return str(self)


class ChannelSerializer(object):
    @staticmethod
    def collect_fds(obj, start=0):
        raise NotImplementedError()

    @staticmethod
    def replace_fds(obj, fds):
        raise NotImplementedError


class UnixChannelSerializer(ChannelSerializer):
    @staticmethod
    def collect_fds(obj, start=0):
        idx = start

        if isinstance(obj, dict):
            for k, v in list(obj.items()):
                if isinstance(v, FileDescriptor):
                    obj[k] = {'$fd': idx}
                    idx += 1
                    yield v
                else:
                    for x in UnixChannelSerializer.collect_fds(v, idx):
                        yield x

        if isinstance(obj, (list, tuple)):
            for i, o in enumerate(obj):
                if isinstance(o, FileDescriptor):
                    obj[i] = {'$fd': idx}
                    idx += 1
                    yield o
                else:
                    for x in UnixChannelSerializer.collect_fds(o, idx):
                        yield x

    @staticmethod
    def replace_fds(obj, fds):
        if isinstance(obj, dict):
            for k, v in list(obj.items()):
                if isinstance(v, dict) and len(v) == 1 and '$fd' in v:
                    obj[k] = FileDescriptor(fds[v['$fd']]) if v['$fd'] < len(fds) else None
                else:
                    UnixChannelSerializer.replace_fds(v, fds)

        if isinstance(obj, list):
            for i, o in enumerate(obj):
                if isinstance(o, dict) and len(o) == 1 and '$fd' in o:
                    obj[i] = FileDescriptor(fds[o['$fd']]) if o['$fd'] < len(fds) else None
                else:
                    UnixChannelSerializer.replace_fds(o, fds)


class MSockChannelSerializer(ChannelSerializer):
    def __init__(self, msock):
        self.msock = msock

    def __fd_to_channel(self, fd):
        chan = first_or_default(lambda c: c.fileno() == fd, self.msock.channels.values())
        return chan.id if chan else -1

    def __channel_to_fd(self, id):
        chan = first_or_default(lambda c: c.id == id, self.msock.channels.values())
        if not chan:
            chan = self.msock.create_channel(id)

        return chan.fileno()

    def collect_fds(self, obj, start=0):
        if isinstance(obj, dict):
            for k, v in list(obj.items()):
                if isinstance(v, FileDescriptor):
                    obj[k] = {'$fd': self.__fd_to_channel(v.fd)}
                    yield v
                else:
                    for x in UnixChannelSerializer.collect_fds(v):
                        yield x

        if isinstance(obj, (list, tuple)):
            for i, o in enumerate(obj):
                if isinstance(o, FileDescriptor):
                    obj[i] = {'$fd': self.__fd_to_channel(o.fd)}
                    yield o
                else:
                    for x in UnixChannelSerializer.collect_fds(o):
                        yield x

    def replace_fds(self, obj, fds):
        if isinstance(obj, dict):
            for k, v in list(obj.items()):
                if isinstance(v, dict) and len(v) == 1 and '$fd' in v:
                    obj[k] = FileDescriptor(self.__channel_to_fd(v['$fd']))
                else:
                    UnixChannelSerializer.replace_fds(v, fds)

        if isinstance(obj, list):
            for i, o in enumerate(obj):
                if isinstance(o, dict) and len(o) == 1 and '$fd' in o:
                    obj[i] = FileDescriptor(self.__channel_to_fd(o['$fd']))
                else:
                    UnixChannelSerializer.replace_fds(o, fds)
