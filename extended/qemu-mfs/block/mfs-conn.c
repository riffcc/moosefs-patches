#include "qemu/osdep.h"

#include "mfs-conn.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "qemu/timer.h"

static MFSConn *mfs_conn_new(const char *host, uint16_t port)
{
    MFSConn *conn = g_new0(MFSConn, 1);

    conn->host = g_strdup(host);
    conn->port = port;
    conn->fd = -1;
    qemu_mutex_init(&conn->lock);
    return conn;
}

static void mfs_conn_destroy(gpointer data)
{
    MFSConn *conn = data;

    if (!conn) {
        return;
    }

    if (conn->fd >= 0) {
        close(conn->fd);
        conn->fd = -1;
    }
    qemu_mutex_destroy(&conn->lock);
    g_free(conn->host);
    g_free(conn);
}

static int mfs_connect_addrlist(struct addrinfo *ai, Error **errp)
{
    struct addrinfo *it;

    for (it = ai; it; it = it->ai_next) {
        int fd;
        int one = 1;

        fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0) {
            continue;
        }

        setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
#ifdef TCP_NODELAY
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#endif

        if (!connect(fd, it->ai_addr, it->ai_addrlen)) {
            return fd;
        }

        close(fd);
    }

    error_setg_errno(errp, errno, "mfs: failed to connect to endpoint");
    return -errno;
}

static int mfs_conn_connect_locked(MFSConn *conn, Error **errp)
{
    struct addrinfo hints;
    struct addrinfo *ai = NULL;
    char port_buf[8];
    int ret;

    if (conn->fd >= 0) {
        conn->online = true;
        return 0;
    }

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    snprintf(port_buf, sizeof(port_buf), "%u", conn->port);
    ret = getaddrinfo(conn->host, port_buf, &hints, &ai);
    if (ret != 0) {
        error_setg(errp, "mfs: resolve failed for %s:%u (%s)",
                   conn->host, conn->port, gai_strerror(ret));
        return -EHOSTUNREACH;
    }

    conn->fd = mfs_connect_addrlist(ai, errp);
    freeaddrinfo(ai);

    if (conn->fd < 0) {
        conn->online = false;
        return conn->fd;
    }

    conn->online = true;
    conn->last_activity_ns = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    return 0;
}

int mfs_endpoint_parse(const char *endpoint, char **host, uint16_t *port,
                       uint16_t default_port, Error **errp)
{
    char *dup;
    char *colon;
    long parsed_port;

    if (!endpoint || !*endpoint) {
        error_setg(errp, "mfs: endpoint is empty");
        return -EINVAL;
    }

    dup = g_strdup(endpoint);
    colon = strrchr(dup, ':');

    if (!colon || !colon[1]) {
        *host = dup;
        *port = default_port;
        return 0;
    }

    *colon = '\0';
    parsed_port = strtol(colon + 1, NULL, 10);
    if (parsed_port <= 0 || parsed_port > UINT16_MAX) {
        g_free(dup);
        error_setg(errp, "mfs: invalid port in endpoint '%s'", endpoint);
        return -EINVAL;
    }

    *host = dup;
    *port = parsed_port;
    return 0;
}

int mfs_conn_pool_init(MFSConnPool *pool, const char *master_endpoint,
                       uint16_t default_master_port, Error **errp)
{
    char *host = NULL;
    uint16_t port = 0;
    int ret;

    memset(pool, 0, sizeof(*pool));

    ret = mfs_endpoint_parse(master_endpoint, &host, &port,
                             default_master_port, errp);
    if (ret < 0) {
        return ret;
    }

    qemu_mutex_init(&pool->lock);
    pool->master = mfs_conn_new(host, port);
    pool->chunk_conns = g_hash_table_new_full(g_str_hash, g_str_equal,
                                              g_free, mfs_conn_destroy);
    pool->initialized = true;

    g_free(host);
    return 0;
}

void mfs_conn_pool_cleanup(MFSConnPool *pool)
{
    if (!pool) {
        return;
    }

    if (!pool->initialized) {
        memset(pool, 0, sizeof(*pool));
        return;
    }

    if (pool->chunk_conns) {
        g_hash_table_destroy(pool->chunk_conns);
        pool->chunk_conns = NULL;
    }

    if (pool->master) {
        mfs_conn_destroy(pool->master);
        pool->master = NULL;
    }

    qemu_mutex_destroy(&pool->lock);
    pool->initialized = false;
}

MFSConn *mfs_conn_pool_master(MFSConnPool *pool)
{
    return pool->master;
}

MFSConn *mfs_conn_pool_get_chunk(MFSConnPool *pool, const char *host,
                                 uint16_t port, Error **errp)
{
    g_autofree char *key = g_strdup_printf("%s:%u", host, port);
    MFSConn *conn;

    qemu_mutex_lock(&pool->lock);

    conn = g_hash_table_lookup(pool->chunk_conns, key);
    if (!conn) {
        conn = mfs_conn_new(host, port);
        g_hash_table_insert(pool->chunk_conns, g_strdup(key), conn);
    }

    qemu_mutex_unlock(&pool->lock);

    if (!conn) {
        error_setg(errp, "mfs: failed to allocate chunkserver connection");
        return NULL;
    }

    return conn;
}

void mfs_conn_lock(MFSConn *conn)
{
    qemu_mutex_lock(&conn->lock);
}

void mfs_conn_unlock(MFSConn *conn)
{
    conn->last_activity_ns = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    qemu_mutex_unlock(&conn->lock);
}

void mfs_conn_mark_dead_locked(MFSConn *conn)
{
    if (conn->fd >= 0) {
        close(conn->fd);
        conn->fd = -1;
    }
    conn->online = false;
}

int mfs_conn_get_fd_locked(MFSConn *conn, Error **errp)
{
    int ret;

    if (conn->fd >= 0) {
        return conn->fd;
    }

    ret = mfs_conn_connect_locked(conn, errp);
    if (ret < 0) {
        return ret;
    }

    return conn->fd;
}
