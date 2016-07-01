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

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <jansson.h>
#include "dispatcher.h"

int main(int argc, char *argv[])
{
        json_t *result;
        json_t *args;
        size_t idx;
        connection_t *conn;
        rpc_call_t *call;
        int status;

        conn = dispatcher_open(argv[1]);
        if (conn == NULL) {
                perror("cannot open dispatcher connection");
                return (1);
        }

        if (dispatcher_login_user(conn, "root", "meh", "") < 0) {
                perror("cannot login to dispatcher");
                return (1);
        }

        args = json_pack("[[s]]", "streaming_responses");
        if (dispatcher_call_sync(conn, "management.enable_features", args, NULL)) {

        }

        call = dispatcher_call_sync_ex(conn, argv[2],
                   json_loads(argv[3], JSON_DECODE_ANY, NULL));
        if (rpc_call_success(call) == RPC_CALL_ERROR) {
                json_dumpf(rpc_call_result(call), stdout, JSON_ENCODE_ANY);
        }

        while (rpc_call_success(call) == RPC_CALL_MORE_AVAILABLE) {
                json_dumpf(rpc_call_result(call), stdout, JSON_ENCODE_ANY);
                printf("\n");
                rpc_call_continue(call, true);
        }

        dispatcher_close(conn);
        return (0);
}
