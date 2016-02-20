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
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/event.h>
#include <sys/un.h>
#include <pthread.h>
#include <utils.h>

#include "utils.h"
#include "unix.h"

static void *unix_event_loop(void *);

unix_conn_t *
unix_connect(const char *path)
{
    struct sockaddr_un sun;
    unix_conn_t *conn;

    conn = xmalloc(sizeof(unix_conn_t));
    conn->unix_path = strdup(path);

    sun.sun_family = AF_UNIX;
    sun.sun_len = sizeof(struct sockaddr_un);
    strncpy(sun.sun_path, path, sizeof(sun.sun_path));

    conn->unix_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (conn->unix_fd < 0)
        goto fail;

    if (connect(conn->unix_fd, (const struct sockaddr *)&sun, sizeof(sun)) < 0)
        goto fail;

    if (pthread_create(&conn->unix_thread, NULL, unix_event_loop, conn)) {
        shutdown(conn->unix_fd, SHUT_RDWR);
        goto fail;
    }

    return (conn);

fail:
    free(conn);
    return (NULL);
}

void
unix_close(unix_conn_t *conn)
{
    shutdown(conn->unix_fd, SHUT_RDWR);
    close(conn->unix_fd);

    free(conn->unix_path);
    free(conn);
}

int
unix_send_msg(unix_conn_t *conn, void *buf, size_t size)
{
    uint32_t header[2];

    header[0] = 0xdeadbeef;
    header[1] = (uint32_t)size;

    if (xwrite(conn->unix_fd, &header, sizeof(uint32_t) * 2) < 0)
        return (-1);

    if (xwrite(conn->unix_fd, buf, size) < 0)
        return (-1);

    return (0);
}

int
unix_recv_msg(unix_conn_t *conn, void **frame, size_t *size)
{
    uint32_t header[2];
    size_t length;

    if (xread(conn->unix_fd, &header, sizeof(uint32_t) * 2) < 0)
        return (-1);

    if (header[0] != 0xdeadbeef)
        return (-1);

    length = header[1];
    *frame = malloc(length);
    *size = length;

    xread(conn->unix_fd, *frame, length);

    return (0);
}

int unix_get_fd(unix_conn_t *conn)
{
    return (conn->unix_fd);
}

static void
unix_process_msg(unix_conn_t *conn, void *frame, size_t size)
{
    conn->unix_message_handler(conn, frame, size, conn->unix_message_handler_arg);
}

static void *
unix_event_loop(void *arg)
{
    unix_conn_t *conn = (unix_conn_t *)arg;
    struct kevent event;
    struct kevent change;
    int i, evs;
    int kq = kqueue();
    void *frame;
    size_t size;

    EV_SET(&change, conn->unix_fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, 0);

    for (;;) {
        evs = kevent(kq, &change, 1, &event, 1, NULL);
        if (evs < 0) {

        }

        for (i = 0; i < evs; i++) {
            if (event.ident == conn->unix_fd) {
                if (event.flags & EV_EOF)
                    return NULL;

                if (unix_recv_msg(conn, &frame, &size) < 0)
                    continue;

                unix_process_msg(conn, frame, size);
            }
        }
    }
}
