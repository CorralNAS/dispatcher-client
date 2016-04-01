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
import numbers
from jsonschema import Draft4Validator, validators, _utils, _validators
from jsonschema.exceptions import ValidationError
from jsonschema.compat import str_types, int_types
from freenas.utils import first_or_default, exclude
from freenas.dispatcher.fd import FileDescriptor

import six

default_types = {
    u"array": list, u"boolean": bool, u"integer": int_types,
    u"null": type(None), u"number": numbers.Number, u"object": dict,
    u"string": str_types, u"fd": FileDescriptor
}


Draft4ValidatorWithFds = validators.create(
    meta_schema=_utils.load_schema("draft4"),
    validators={
        u"$ref": _validators.ref,
        u"additionalItems": _validators.additionalItems,
        u"additionalProperties": _validators.additionalProperties,
        u"allOf": _validators.allOf_draft4,
        u"anyOf": _validators.anyOf_draft4,
        u"dependencies": _validators.dependencies,
        u"enum": _validators.enum,
        u"format": _validators.format,
        u"items": _validators.items,
        u"maxItems": _validators.maxItems,
        u"maxLength": _validators.maxLength,
        u"maxProperties": _validators.maxProperties_draft4,
        u"maximum": _validators.maximum,
        u"minItems": _validators.minItems,
        u"minLength": _validators.minLength,
        u"minProperties": _validators.minProperties_draft4,
        u"minimum": _validators.minimum,
        u"multipleOf": _validators.multipleOf,
        u"not": _validators.not_draft4,
        u"oneOf": _validators.oneOf_draft4,
        u"pattern": _validators.pattern,
        u"patternProperties": _validators.patternProperties,
        u"properties": _validators.properties_draft4,
        u"required": _validators.required_draft4,
        u"type": _validators.type_draft4,
        u"uniqueItems": _validators.uniqueItems,
    },
    version="draft4",
    default_types=default_types
)


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

    return validators.extend(
        validator_class, {
            "properties": set_defaults,
            "oneOf": oneOf_discriminator
        },
    )

DefaultDraft4Validator = extend_with_default(Draft4ValidatorWithFds)
