/*+
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

#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <pthread.h>
#include <jansson.h>

#ifdef __FreeBSD__
#include <uuid.h>
#endif

#ifdef __APPLE__
#include <uuid/uuid.h>
#endif

#include "ws.h"
#include "unix.h"
#include "dispatcher.h"

#define BUFSIZE 256

struct rpc_call
{
	connection_t *      rc_conn;
	const char *        rc_type;
	const char *        rc_method;
	json_t *            rc_id;
	json_t *            rc_args;
	rpc_call_status_t   rc_status;
	json_t *            rc_result;
	json_t *            rc_error;
	pthread_cond_t      rc_completed;
	pthread_mutex_t     rc_mtx;
	rpc_callback_t *    rc_callback;
	void *              rc_callback_arg;
	TAILQ_ENTRY(rpc_call) rc_link;
};

struct connection
{
	const char *        conn_uri;
	unix_conn_t *       conn_unix;
	ws_conn_t *         conn_ws;
	error_callback_t *  conn_error_handler;
	void *              conn_error_handler_arg;
	event_callback_t *  conn_event_handler;
	void *              conn_event_handler_arg;
	int                 conn_rpc_timeout;
	TAILQ_HEAD(rpc_calls_head, rpc_call) conn_calls;
};

static rpc_call_t *rpc_call_alloc(connection_t *conn);
static json_t *dispatcher_new_id();
static int dispatcher_call_internal(connection_t *conn, const char *type,
    struct rpc_call *call);
static json_t *dispatcher_pack_msg(const char *ns, const char *name, json_t *id,
    json_t *args);
static int dispatcher_send_msg(connection_t *conn, json_t *msg);
static void dispatcher_process_msg(void *conn, void *frame, size_t len,
    void *arg);
static void dispatcher_abort(void *conn, void *arg);
static void dispatcher_process_rpc(connection_t *conn, json_t *msg);
static void dispatcher_process_events(connection_t *conn, json_t *msg);
static void dispatcher_answer_call(rpc_call_t *call,
    enum rpc_call_status status, json_t *result);

connection_t *
dispatcher_open(const char *hostname)
{
	char schema[BUFSIZE];
	char netloc[BUFSIZE];
	char *uri;

	connection_t *conn = calloc(1, sizeof(connection_t));
	TAILQ_INIT(&conn->conn_calls);

	if (sscanf(hostname, "%[^:]://%[^:]", schema, netloc) < 2) {
		free(conn);
		return (NULL);
	}

	if (strncmp(schema, "unix", sizeof("unix")) == 0) {
		conn->conn_unix = unix_connect(netloc);
		if (conn->conn_unix == NULL) {
			free(conn);
			return (NULL);
		}

		conn->conn_unix->unix_message_handler =
		    (unix_message_handler_t)dispatcher_process_msg;
		conn->conn_unix->unix_message_handler_arg = conn;
		conn->conn_unix->unix_close_handler =
		    (unix_close_handler_t)dispatcher_abort;
		conn->conn_unix->unix_close_handler_arg = conn;
	} else {
		asprintf(&uri, "http://%s:5000/socket", hostname);
		conn->conn_ws = ws_connect(uri);
		if (conn->conn_ws == NULL) {
			free(conn);
			return (NULL);
		}

		conn->conn_ws->ws_message_handler =
		    (ws_message_handler_t)dispatcher_process_msg;
		conn->conn_ws->ws_message_handler_arg = conn;
		free(uri);
	}

	if (conn->conn_ws == NULL && conn->conn_unix == NULL)
		return (NULL);

	return (conn);
}

void
dispatcher_close(connection_t *conn)
{
	rpc_call_t *call, *tmp;

	if (conn->conn_ws != NULL)
		ws_close(conn->conn_ws);

	if (conn->conn_unix != NULL)
		unix_close(conn->conn_unix);

	TAILQ_FOREACH_SAFE(call, &conn->conn_calls, rc_link, tmp) {
		rpc_call_free(call);
	}

	free(conn);
}

int
dispatcher_get_fd(connection_t *conn)
{
	if (conn->conn_ws != NULL)
		return ws_get_fd(conn->conn_ws);

	if (conn->conn_unix != NULL)
		return unix_get_fd(conn->conn_unix);

	return (-1);
}

int
dispatcher_login_user(connection_t *conn, const char *user, const char *pwd,
    const char *resource)
{
	struct rpc_call *call;
	json_t *id = dispatcher_new_id();
	json_t *msg;

	call = rpc_call_alloc(conn);
	call->rc_id = id;
	call->rc_type = "auth";
	call->rc_args = json_object();

	TAILQ_INSERT_TAIL(&conn->conn_calls, call, rc_link);

	json_object_set(call->rc_args, "username", json_string(user));
	json_object_set(call->rc_args, "password", json_string(pwd));
	json_object_set(call->rc_args, "resource", json_string(resource));

	dispatcher_call_internal(conn, "auth", call);
	rpc_call_wait(call);
	return rpc_call_success(call);
}

int
dispatcher_login_service(connection_t *conn, const char *name)
{
	struct rpc_call *call;
	json_t *id = dispatcher_new_id();
	json_t *msg;

	call = rpc_call_alloc(conn);
	call->rc_id = id;
	call->rc_type = "auth";
	call->rc_args = json_object();

	TAILQ_INSERT_TAIL(&conn->conn_calls, call, rc_link);

	json_object_set(call->rc_args, "name", json_string(name));

	dispatcher_call_internal(conn, "auth_service", call);
	rpc_call_wait(call);
	return rpc_call_success(call);
}

int dispatcher_subscribe_event(connection_t *conn, const char *name)
{
	json_t *msg;

	msg = dispatcher_pack_msg("events", "subscribe", json_null(),
	    json_pack("[s]", name));
	return (dispatcher_send_msg(conn, msg));
}

int dispatcher_unsubscribe_event(connection_t *conn, const char *name)
{
	json_t *msg;

	msg = dispatcher_pack_msg("events", "unsubscribe", json_null(),
	    json_pack("[s]", name));
	return (dispatcher_send_msg(conn, msg));
}

int
dispatcher_call_sync(connection_t *conn, const char *name, json_t *args,
    json_t **result)
{
	rpc_call_t *call = dispatcher_call_async(conn, name, args, NULL, NULL);
	rpc_call_wait(call);
	*result = rpc_call_result(call);
	return rpc_call_success(call);
}

rpc_call_t *
dispatcher_call_async(connection_t *conn, const char *name, json_t *args,
	rpc_callback_t *cb, void *cb_arg)
{
	struct rpc_call *call;
	json_t *id = dispatcher_new_id();
	json_t *msg;

	call = rpc_call_alloc(conn);
	call->rc_id = id;
	call->rc_type = "call";
	call->rc_method = name;
	call->rc_args = json_object();
	call->rc_callback = cb;
	call->rc_callback_arg = cb_arg;

	TAILQ_INSERT_TAIL(&conn->conn_calls, call, rc_link);

	json_object_set_new(call->rc_args, "method", json_string(name));
	json_object_set(call->rc_args, "args", args);
	dispatcher_call_internal(conn, "call", call);

	return (call);
}

int
dispatcher_emit_event(connection_t *conn, const char *name, json_t *args)
{
	json_t *msg, *payload, *id;
	int err;

	id = json_null();
	payload = json_pack("{ssso}", "name", name, "args", args);
	if (payload == NULL) {
		json_decref(id);
		return (-1);
	}

	msg = dispatcher_pack_msg("events", "event", id, payload);
	if (msg == NULL) {
		json_decref(id);
		json_decref(payload);
		return (-1);
	}

	err = dispatcher_send_msg(conn, msg);
	json_decref(id);
	json_decref(payload);
	json_decref(msg);
	return (err);
}

void
dispatcher_on_error(connection_t *conn, error_callback_t *cb, void *arg)
{
	conn->conn_error_handler = cb;
	conn->conn_error_handler_arg = arg;
}

void
dispatcher_on_event(connection_t *conn, event_callback_t *cb, void *arg)
{
	conn->conn_event_handler = cb;
	conn->conn_event_handler_arg = arg;
}

int
rpc_call_wait(rpc_call_t *call)
{
	return (pthread_cond_wait(&call->rc_completed, &call->rc_mtx));
}

int
rpc_call_timedwait(rpc_call_t *call, const struct timespec *abstime)
{
	return (pthread_cond_timedwait(&call->rc_completed, &call->rc_mtx,
	    abstime));
}

int
rpc_call_success(rpc_call_t *call)
{
	return (call->rc_status);
}

json_t *
rpc_call_result(rpc_call_t *call)
{
	return (call->rc_status ==
	    RPC_CALL_DONE ? call->rc_result : call->rc_error);
}

void
rpc_call_free(rpc_call_t *call)
{
	json_decref(call->rc_args);
	json_decref(call->rc_id);
	if (call->rc_error != NULL)
		json_decref(call->rc_error);

	if (call->rc_result != NULL)
		json_decref(call->rc_result);

	TAILQ_REMOVE(&call->rc_conn->conn_calls, call, rc_link);
	free(call);
}


static rpc_call_t *
rpc_call_alloc(connection_t *conn)
{
	rpc_call_t *call;

	call = malloc(sizeof(rpc_call_t));
	memset(call, 0, sizeof(rpc_call_t));
	pthread_cond_init(&call->rc_completed, NULL);
	pthread_mutex_init(&call->rc_mtx, NULL);
	call->rc_conn = conn;
	return (call);
}

static int
dispatcher_call_internal(connection_t *conn, const char *type,
    struct rpc_call *call)
{
	json_t *msg;

	msg = dispatcher_pack_msg("rpc", type, call->rc_id, call->rc_args);
	if (msg == NULL)
		return (-1);

	pthread_mutex_lock(&call->rc_mtx);

	if (dispatcher_send_msg(conn, msg) < 0) {
		json_decref(msg);
		return (-1);
	}

	return (0);
}

static json_t *
dispatcher_pack_msg(const char *ns, const char *name, json_t *id, json_t *args)
{
	json_t *obj;

	obj = json_object();
	if (obj == NULL) {
		errno = ENOMEM;
		return (NULL);
	}

	json_object_set_new(obj, "namespace", json_string(ns));
	json_object_set_new(obj, "name", json_string(name));
	json_object_set(obj, "id", id);
	json_object_set(obj, "args", args);

	return (obj);
}

static int
dispatcher_send_msg(connection_t *conn, json_t *msg)
{
	char *str = json_dumps(msg, 0);
	int ret;

	if (conn->conn_ws != NULL)
		ret = ws_send_msg(conn->conn_ws, str, strlen(str), WS_TEXT);
	else if (conn->conn_unix != NULL)
		ret = unix_send_msg(conn->conn_unix, str, strlen(str));
	else {
		errno = ENXIO;
		ret = -1;
	}

	json_decref(msg);
	free(str);
	return (ret);
}

static void
dispatcher_process_msg(void *ctx __unused, void *frame, size_t len, void *arg)
{
	connection_t *conn = (connection_t *)arg;
	json_t *msg;
	json_error_t err;
	char *framestr;
	const char *ns;

	framestr = (char *)frame;
	framestr = realloc(framestr, len + 1);
	framestr[len] = '\0';

	msg = json_loads(framestr, 0, &err);
	free(framestr);

	if (msg == NULL) {
		if (conn->conn_error_handler)
			conn->conn_error_handler(conn, INVALID_JSON_RESPONSE,
			    conn->conn_error_handler_arg);

		return;
	}

	ns = json_string_value(json_object_get(msg, "namespace"));

	if (!strcmp(ns, "rpc")) {
		dispatcher_process_rpc(conn, msg);
		return;
	}

	if (!strcmp(ns, "events")) {
		dispatcher_process_events(conn, msg);
		return;
	}
}

void
dispatcher_abort(void *ctx, void *arg)
{
	connection_t *conn = (connection_t *)arg;
	rpc_call_t *call;

	TAILQ_FOREACH(call, &conn->conn_calls, rc_link) {
		call->rc_status = RPC_CALL_ERROR;
		call->rc_error = json_pack("{siss}", "code", ECONNABORTED,
		    "message", "Connection reset");

		if (call->rc_callback != NULL) {
			call->rc_callback(conn, json_string_value(call->rc_id),
			    NULL, call->rc_error, call->rc_callback_arg);
		}

		pthread_cond_broadcast(&call->rc_completed);
	}
}

static void
dispatcher_process_rpc(connection_t *conn, json_t *msg)
{
	rpc_call_t *call, *tmp;
	const char *name;
	const char *id;
	bool error;

	name = json_string_value(json_object_get(msg, "name"));
	error = !strcmp(name, "error");
	id = json_string_value(json_object_get(msg, "id"));

	TAILQ_FOREACH_SAFE(call, &conn->conn_calls, rc_link, tmp) {
		if (!strcmp(id, json_string_value(call->rc_id))) {
			if (error) {
				call->rc_status = RPC_CALL_ERROR;
				call->rc_error = json_object_get(msg, "args");
			} else {
				call->rc_status = RPC_CALL_DONE;
				call->rc_result = json_object_get(msg, "args");
			}

			dispatcher_answer_call(call,
			    error ? RPC_CALL_ERROR : RPC_CALL_DONE,
			    json_object_get(msg, "args"));
		}
	}

	json_decref(msg);
}

static void
dispatcher_process_events(connection_t *conn, json_t *msg)
{
	const char *name;
	const char *evname;
	json_t *args;

	name = json_string_value(json_object_get(msg, "name"));
	args = json_object_get(msg, "args");
	evname = json_string_value(json_object_get(args, "name"));

	if (strcmp(name, "event") != 0)
		return;

	if (conn->conn_event_handler)
		conn->conn_event_handler(conn, evname,
		    json_object_get(args, "args"),
		    conn->conn_event_handler_arg);
}

static void
dispatcher_answer_call(rpc_call_t *call, enum rpc_call_status status,
    json_t *result)
{
	json_incref(result);

	if (status == RPC_CALL_ERROR) {
		call->rc_status = RPC_CALL_ERROR;
		call->rc_error = result;
	} else {
		call->rc_status = RPC_CALL_DONE;
		call->rc_result = result;
	}

	if (call->rc_callback != NULL) {
		call->rc_callback(call->rc_conn, json_string_value(call->rc_id),
		    call->rc_result, call->rc_error,
		    call->rc_callback_arg);
	}

	pthread_cond_broadcast(&call->rc_completed);
}

struct tm *
rpc_json_to_timestamp(json_t *json)
{
	struct tm *timestamp;
	const char *str;
	json_t *val;

	if (json == NULL) {
		errno = EINVAL;
		return (NULL);
	}

	if (!json_is_object(json)) {
		errno = EINVAL;
		return (NULL);
	}

	val = json_object_get(json, "$date");
	if (val == NULL) {
		errno = EINVAL;
		return (NULL);
	}

	str = json_string_value(val);
	if (str == NULL) {
		json_decref(val);
		errno = EINVAL;
		return (NULL);
	}

	timestamp = calloc(1, sizeof(struct tm));
	if (timestamp == NULL) {
		json_decref(val);
		errno = ENOMEM;
		return (NULL);
	}

	if (strptime(str, "%F %T", timestamp) == NULL) {
		free(timestamp);
		json_decref(val);
		errno = EINVAL;
		return (NULL);
	}

	json_decref(val);
	return (timestamp);
}

json_t *
rpc_timestamp_to_json(struct tm *timestamp)
{
	json_t *val;
	char str[64];

	if (strftime(str, sizeof(str) - 1, "%F %T", timestamp) == 0) {
		errno = EINVAL;
		return (NULL);
	}

	val = json_pack("{ss}", "$date", str);
	return (val);
}

#if 0
static void *
dispatcher_collect_task(void *arg)
{
	rpc_call_t *call;

	for (;;) {
		TAILQ_FOREACH_SAFE(call, &conn->conn_calls, rc_link, tmp) {
			json_decref(call->rc_args);
			json_decref(call->rc_id);
			json_decref(call->rc_error);
			json_decref(call->rc_result);
			TAILQ_REMOVE(&conn->conn_calls, call, rc_link);
			free(call);
		}
	}
}
#endif

#ifdef __FreeBSD__
static json_t *
dispatcher_new_id(void)
{
	json_t *id;
	uuid_t uuid;
	uint32_t status;
	char *str;

	uuid_create(&uuid, &status);
	if (status != uuid_s_ok)
		return (NULL);

	uuid_to_string(&uuid, &str, &status);
	if (status != uuid_s_ok)
		return (NULL);

	return json_string(str);
}
#endif

#ifdef __APPLE__
static json_t *
dispatcher_new_id()
{
	uuid_t uuid;
	char str[37];

	uuid_generate(uuid);
	uuid_unparse_lower(uuid, str);
	return json_string(str);
}
#endif
