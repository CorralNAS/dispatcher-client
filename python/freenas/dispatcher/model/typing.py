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

from freenas.dispatcher.model import BaseObject
from freenas.dispatcher.rpc import convert_schema


__all__ = ['Range', 'Pattern', 'Default']


class RangeFactory(object):
    def __getitem__(self, item):
        base_type = item[0]
        min_value = item[1] if len(item) > 1 else None
        max_value = item[2] if len(item) > 2 else None

        if base_type not in (int, float):
            raise ValueError('Range can be used only with int and float types')

        def to_json_schema(cls):
            sch = {'type': convert_schema(cls.inner)}
            if cls.min_value is not None:
                sch['minimum'] = cls.min_value
            if cls.max_value is not None:
                sch['maximum'] = cls.max_value

            return sch

        return type(
            'Range[{0}, {1}, {2}]'.format(base_type.__name__, min_value, max_value),
            (BaseObject,), {
                'to_json_schema': classmethod(to_json_schema),
                'inner': base_type,
                'min_value': min_value,
                'max_value': max
            }
        )


class PatternFactory(object):
    def __getitem__(self, pat):
        def to_json_schema(cls):
            return {
                'type': 'string',
                'pattern': cls.pattern
            }

        return type(
            'Pattern["{0}"]'.format(pat),
            (BaseObject,), {
                'to_json_schema': classmethod(to_json_schema),
                'pattern': pat
            }
        )


class DefaultFactory(object):
    def __getitem__(self, item):
        if len(item) != 2:
            raise ValueError('Default requires base type and a default value')

        base_type, default_value = item

        if not isinstance(base_type, type):
            raise ValueError('Default base_type must be a type')

        def to_json_schema(cls):
            return {
                'type': cls.base_type.to_json_schema(),
                'pattern': cls.pattern
            }

        return type(
            'Default[{0}, {1!r}]'.format(base_type.__name__, default_value),
            (BaseObject,), {
                'to_json_schema': classmethod(to_json_schema),
                'base_type': base_type,
                'default_value': default_value
            }
        )


Range = RangeFactory()
Pattern = PatternFactory()
Default = DefaultFactory()
