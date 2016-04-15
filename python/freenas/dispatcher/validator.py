#+
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

import errno
import datetime
from jsonschema import Draft4Validator
from jsonschema.validators import create
from jsonschema.exceptions import ValidationError
from freenas.utils import first_or_default
from freenas.dispatcher.fd import FileDescriptor

import six


def serialize_errors(errors):
    for i in errors:
        yield {
            'path': list(i.path),
            'message': i.message,
            'code': errno.EINVAL
        }


def schema_to_list(schema):
    return {
        'type': 'array',
        'items': schema,
        'minItems': sum([1 for x in schema if 'mandatory' in x and x['mandatory']]),
        'maxItems': len(schema)
    }


def schema_to_dict(schema):
    return {
        'type': 'object',
        'properties': {x['title']: x for x in schema.items()},
        'required': [x['title'] for x in schema.values() if 'mandatory' in x and x['mandatory']]
    }


def extend_with_default(validator_class):
    validate_properties = validator_class.VALIDATORS["properties"]

    def extend(validator, validators, version=None):
        all_validators = dict(validator.VALIDATORS)
        all_validators.update(validators)
        all_types = dict(validator.DEFAULT_TYPES)
        all_types.update({
            "fd": FileDescriptor,
            "datetime": datetime.datetime
        })

        return create(
            meta_schema=validator.META_SCHEMA,
            validators=all_validators,
            version=version,
            default_types=all_types,
        )

    def set_defaults(validator, properties, instance, schema):
        for error in validate_properties(
            validator, properties, instance, schema,
        ):
            yield error

        for property, subschema in six.iteritems(properties):
            if "default" in subschema:
                instance.setdefault(property, subschema["default"])

    def oneOf_discriminator(validator, oneOf, instance, schema):
        subschemas = enumerate(oneOf)
        all_errors = []

        if 'discriminator' in schema:
            discriminator = schema['discriminator']
            if discriminator in instance:
                subschema = first_or_default(lambda s: s['$ref'] == instance[discriminator], oneOf)
                if subschema:
                    for err in validator.descend(instance, subschema):
                        yield err

                return

        for index, subschema in subschemas:
            errs = list(validator.descend(instance, subschema, schema_path=index))
            if not errs:
                first_valid = subschema
                break
            all_errors.extend(errs)
        else:
            yield ValidationError(
                "%r is not valid under any of the given schemas" % (instance,),
                context=all_errors,
            )

        more_valid = [s for i, s in subschemas if validator.is_valid(instance, s)]
        if more_valid:
            more_valid.append(first_valid)
            reprs = ", ".join(repr(schema) for schema in more_valid)
            yield ValidationError(
                "%r is valid under each of %s" % (instance, reprs)
            )

    return extend(
        validator_class, {
            "properties": set_defaults,
            "oneOf": oneOf_discriminator
        },
    )

DefaultDraft4Validator = extend_with_default(Draft4Validator)
