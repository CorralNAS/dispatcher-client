/*
 * Copyright 2015 iXsystems, Inc.
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted providing that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

var diff = require('deep-diff').diff;


var operatorsTable = {
    '=': (x, y) => x == y,
    '!=': (x, y) => x != y,
    '>': (x, y) => x > y,
    '<': (x, y) => x < y,
    '>=': (x, y) => x >= y,
    '<=': (x, y) => x <= y,
    '~': (x, y) => x.match(y),
    'in': (x, y) => y.indexOf(x) > -1,
    'nin': (x, y) => y.indexOf(x) == -1
};


var conversions_table = {
    'timestamp': v => Date.parse(v)/1000
};


function evalLogicAnd(item, lst){
    for (let i of lst){
        if (!evalTuple(item, i)){
            return false;
        }
    }

    return true;
}

function evalLogicOr(item, lst){
    for (let i of lst){
        if (evalTuple(item, i)){
            return true;
        }
    }

    return false;
}

function evalLogicNor(item, lst){
    for (let i of lst){
        if (evalTuple(item, i)){
            return false;
        }
    }

    return true;
}

function evalLogicOperator(item, t){
    let [op, lst] = t;
    switch (op) {
      case and: return evalLogicAnd(item, lst);
      case or: return evalLogicOr(item, lst);
      case nor: return evalLogicNor(item, lst);
    }
}

function evalFieldOperator(item, t){
    let [left, op, right] = t;

    if (Object.keys(t).length == 4){
        right = conversions_table[t[3]](right);
    }

    return operatorsTable[op](item[left], right);
}

function evalTuple(item, t){
    if (Object.keys(t).length == 2){
        return evalLogicOperator(item, t);
    }
    if ((Object.keys(t).length == 3)||(Object.keys(t).length == 4)){
        return evalFieldOperator(item, t);
    }
}

function matches(obj, rules){
    let fail = false;
    for (let r of rules){
        if (!evalTuple(obj, r)){
            fail = true;
            break;
        }
    }

    return !fail;
}

class CappedMap
{
    constructor(maxsize)
    {
        this.map = new Map();
        this.maxsize = maxsize;
    }

    get(key)
    {
        return this.map.get(key);
    }

    set(key, value)
    {
        this.map.set(key, value);
        if (this.map.size > this.maxsize) {
            let mapIter = this.map.entries();
            this.map.delete(mapIter.next().key);
        }
    }

    has(key)
    {
        return this.map.has(key);
    }

    getSize()
    {
        return this.map.size;
    }

    setMaxSize(maxsize)
    {
        if (maxsize < this.maxsize){
            let mapIter = this.map.entries();
            let deleteSize = this.map.size - maxsize;
            for (i = 0; i < deleteSize; i++){
                this.map.delete(mapIter.next().key);
            }
        }
        this.maxsize = maxsize;
    }

    deleteKey(key)
    {
        this.map.delete(key);
    }

    entries()
    {
        return this.map.entries();
    }

}

export class EntitySubscriber
{
    constructor(client, name, maxsize=2000)
    {
        this.name = name;
        this.client = client;
        this.maxsize = maxsize;
        this.objects = new CappedMap(maxsize);
        this.handlerCookie = null;

        /* Callbacks */
        this.onCreate = () => {};
        this.onUpdate = () => {};
        this.onDelete = () => {};
    }

    fetch()
    {
        this.client.call(`${this.name}.query`, [[], {"limit": this.maxsize}], result => {
            for (let i of result) {
                this.objects.set(i.id, i);
            }
        });
    }

    start()
    {
        this.handlerCookie = this.client.registerEventHandler(
            `entity-subscriber.${this.name}.changed`,
            this.__onChanged.bind(this)
        );
    }

    stop()
    {
        this.client.unregisterEventHandler(
            `entity-subscriber.${this.name}.changed`,
            this.handlerCookie
        );
    }

    __onChanged(event)
    {
        if (event.operation == "create") {
            for (let entity of event.entities)
                this.objects.set(entity.id, entity);
        }

        if (event.operation == "update") {
            for (let entity of event.entities)
                this.objects.set(entity.id, entity);
        }

        if (event.operation == "rename") {
            for (let [old_id, new_id] of event.ids)
                if (this.objects.has(old_id)) {
                    let entity = this.objects.get(old_id);
                    entity.id = new_id;
                    this.objects.set(entity.id, entity);
                    this.objects.deleteKey(old_id);
                }
        }

        if (event.operation == "delete") {
            for (let id of event.ids)
                if (this.objects.has(id))
                    this.objects.deleteKey(id);
        }
    }

    query(rules, params, callback){
        if (this.objects.getSize() == this.maxsize){
            DispatcherClient.call(`${this.name}.query`, [rules, params], callback);
        } else {
            let _single = params["single"];
            let _count = params["count"];
            let _offset = params["offset"];
            let _limit = params["limit"];
            let _sort = params["sort"];
            let _result = new Array();

            if (Object.keys(rules).length == 0){
                for (let [key, value] of this.objects.entries()){
                    _result.push(value);
                }
            } else {
                for (let [key, value] of this.objects.entries()){
                    if(matches(value, rules)){
                       _result.push(value);
                    }
                }
            }

            if (_sort){
                let sortKeys = [];
                let direction = [];
                for (let i of _sort){
                    if (i.charAt(0) == '-'){
                        sortKeys.push(i.slice(1));
                        direction.push('desc');
                    } else {
                        sortKeys.push(i);
                        direction.push('asc');
                    }
                }
                _.map(_.sortByOrder(_result, sortKeys, direction), _.values);
            }

            if (_offset){
                callback(_result.slice(_offset));
            }

            if (_limit) {
                callback(_result.slice(0,_limit));
            }

            if ((!_result.length) && (_single)){
                callback(null);
            }

            if (_single) {
                callback(_result[0]);
            }

            if (_count) {
                callback(_result.length);
            }

            callback(_result);
        }
    }
}
