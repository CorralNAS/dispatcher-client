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

import base64
import uuid
import re
from freenas.dispatcher import Password
from datetime import datetime
from dateutil.parser import parse


try:
    import ujson as json
except ImportError:
    import json


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if type(obj) is uuid.UUID:
            return str(obj)

        if type(obj) is datetime:
            return {'$date': str(obj)}

        if type(obj) is re._pattern_type:
            return {'$regex': obj.pattern}

        if type(obj) is bytes:
            return {'$binary': base64.b64encode(obj).decode('ascii')}
            
        if type(obj) is set:
            return list(obj)

        if type(obj) is Password:
            return {'$password': obj.secret}

        if hasattr(obj, '__getstate__'):
            return obj.__getstate__()

        return str(obj)


def decode_hook(obj):
    if len(obj) == 1:
        if '$date' in obj:
            return parse(obj['$date'])

        if '$binary' in obj:
            return base64.b64decode(obj['$binary'])

        if '$regex' in obj:
            return re.compile(obj['$regex'])

        if '$password' in obj:
            return Password(obj['$password'])

    return obj


def load(fp):
    return json.load(fp, object_hook=decode_hook)


def loads(s):
    return json.loads(s, object_hook=decode_hook)


def dump(obj, fp, **kwargs):
    return json.dump(obj, fp, cls=JsonEncoder, **kwargs)


def dumps(obj, **kwargs):
    return json.dumps(obj, cls=JsonEncoder, **kwargs)
