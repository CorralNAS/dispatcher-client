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

from freenas.dispatcher.rpc import convert_schema
from freenas.utils import extend, include
import enum


class Iterator(object):
    pass


class BaseObject(object):
    @classmethod
    def json_schema_name(cls):
        return cls.__name__

    def __str__(self):
        return "<BaseObject '{0}'>".format(self.json_schema_name())


class BaseObjectRef(BaseObject):
    @classmethod
    def to_json_schema(cls):
        return {
            '$ref': cls.__name__
        }

    def __str__(self):
        return "<BaseObjectRef '{0}'>".format(self.json_schema_name())


class BaseStruct(BaseObject):
    @classmethod
    def to_json_schema(cls):
        return {
            'type': 'object',
            'additionalProperties': False,
            'properties': {k: convert_schema(v) for k, v in cls.schema_fields()},
        }

    @classmethod
    def schema_fields(cls):
        yield '%type', {'type': 'string', 'enum': [cls.__name__]}
        yield from cls.__annotations__.items()

    @classmethod
    def required_fields(cls):
        if hasattr(cls, '_required_fields'):
            return cls._required_fields

        return [k for k, v in cls.__annotations__.items() if type(v) is type(Required)]

    @property
    def fields(self):
        return list(self.__dict__['_dict'].keys())

    def __init_subclass__(cls, *args, **kwargs):
        context.type_enumerator.structures[cls.__name__] = cls

    def __init__(self, values=None, **kwargs):
        self.__dict__['_dict'] = values or {}
        self.__dict__['_dict'].update(kwargs)

    def __getattr__(self, item):
        try:
            return self._dict[item]
        except KeyError:
            raise AttributeError()

    def __setattr__(self, key, value):
        self._dict[key] = value

    def __getstate__(self):
        return extend(
            include(self._dict, *self.__annotations__.keys()),
            {'%type': self.json_schema_name()}
        )

    def __setstate__(self, state):
        self.__dict__['_dict'] = state

    def __str__(self):
        return "<Struct '{0}'>".format(self.json_schema_name())

    def __repr__(self):
        return str(self)

    def merge(self, other):
        pass


class BaseEnum(BaseObject, enum.Enum):
    @classmethod
    def to_json_schema(cls):
        return {
            'type': 'string',
            'enum': [m.value for m in cls.__members__.values()]
        }

    def __init_subclass__(cls, *args, **kwargs):
        context.type_enumerator.structures[cls.__name__] = cls


class BaseType(BaseObject):
    @classmethod
    def to_json_schema(cls):
        return cls._schema

    def __init_subclass__(cls, *args, **kwargs):
        context.type_enumerator.structures[cls.__name__] = cls


class BaseVariantType(BaseObject):
    @classmethod
    def to_json_schema(cls):
        return {
            'discriminator': '%type',
            'oneOf': [
                {'$ref': c.json_schema_name()} for c in context.type_enumerator.find_by_base(cls.__name__)
            ]
        }

    def __init_subclass__(cls, *args, **kwargs):
        context.type_enumerator.structures[cls.__name__] = cls


class BaseService(BaseObject):
    def __str__(self):
        return "<Service '{0}'>".format(self.json_schema_name())


class TypeEnumerator(object):
    def __init__(self, context):
        self.context = context
        self.structures = {}

    @property
    def schemas(self):
        return self.context.client.call_sync('discovery.get_schema')

    def construct_struct(self, name, schema):
        dct = {
            '__annotations__': schema['properties'],
            '_required_fields': schema.get('required', []),
            '_schema': schema,
        }

        return type(name, (BaseStruct,), dct)

    def construct_enum(self, name, schema):
        def escape(key):
            if key is None:
                return 'NONE'

            if isinstance(key, int):
                return '_' + str(key)

            return key

        enum = BaseEnum(name, {escape(k): k for k in schema['enum']})
        enum['_schema'] = schema
        return enum

    def construct_type(self, name, definition):
        return type(name, (BaseType,), {'_schema': definition})

    def find_by_base(self, base):
        for s in self.structures.values():
            variant = getattr(s, '__variant_of__', None)
            if variant and variant.__name__ == base:
                yield s

    def __getattr__(self, item):
        try:
            return self.structures[item]
        except KeyError:
            # Make up a JSON schema reference, maybe the structure is not registered yet
            return type(item, (BaseObjectRef,), {})


class ServiceEnumerator(object):
    def __init__(self, context):
        self.conttext = context
        self.services = []

    def construct_service(self, name, methods):
        dct = {
            '_service_name': name,
            '_client': self.context.client
        }

        for m in methods:
            def call(self, *args, **kwargs):
                return self._client.call_sync('{0}.{1}'.format(name, m['name']), *args, **kwargs)

            dct[m['name']] = call

        return type(name, (BaseService,), dct)

    def __getattr__(self, item):
        methods = self.context.client.call_sync('discovery.get_methods', item)
        cls = self.construct_service(item, methods)
        return cls()


class Context(object):
    def __init__(self):
        self.service_enumerator = ServiceEnumerator(self)
        self.type_enumerator = TypeEnumerator(self)

    def register_schema(self, name, definition):
        if 'enum' in definition:
            return self.type_enumerator.construct_enum(name, definition)

        elif 'properties' in definition:
            return self.type_enumerator.construct_struct(name, definition)

        else:
            self.type_enumerator.construct_type(name, definition)

    def unregister_schema(self, name):
        self.type_enumerator.structures.pop(name, None)

    @property
    def client(self):
        return None

    @property
    def services(self):
        return self.service_enumerator

    @property
    def types(self):
        return self.type_enumerator

    @property
    def json_schema_objects(self):
        return self.type_enumerator.structures.values()


context = Context()
types = context.types
services = context.services
