#ifndef QEMU_MFS_CONN_H
#define QEMU_MFS_CONN_H

#include "qemu/osdep.h"
#include "qemu/thread.h"
#include "qapi/error.h"

typedef struct MFSConn {
    char *host;
    uint16_t port;
    int fd;
    bool online;
    int64_t last_activity_ns;
    QemuMutex lock;
} MFSConn;

typedef struct MFSConnPool {
    MFSConn *master;
    GHashTable *chunk_conns; /* key=host:port string, value=MFSConn* */
    QemuMutex lock;
    bool initialized;
} MFSConnPool;

int mfs_endpoint_parse(const char *endpoint, char **host, uint16_t *port,
                       uint16_t default_port, Error **errp);

int mfs_conn_pool_init(MFSConnPool *pool, const char *master_endpoint,
                       uint16_t default_master_port, Error **errp);
void mfs_conn_pool_cleanup(MFSConnPool *pool);

MFSConn *mfs_conn_pool_master(MFSConnPool *pool);
MFSConn *mfs_conn_pool_get_chunk(MFSConnPool *pool, const char *host,
                                 uint16_t port, Error **errp);

void mfs_conn_lock(MFSConn *conn);
void mfs_conn_unlock(MFSConn *conn);
int mfs_conn_get_fd_locked(MFSConn *conn, Error **errp);
void mfs_conn_mark_dead_locked(MFSConn *conn);

#endif
