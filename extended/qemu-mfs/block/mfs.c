#include "qemu/osdep.h"

#include "block/block_int-common.h"
#include "qapi/error.h"
#include "qapi/qmp/qdict.h"
#include "qemu/coroutine.h"
#include "qemu/error-report.h"
#include "qemu/iov.h"
#include "qemu/module.h"
#include "qemu/option.h"
#include "qemu/timer.h"
#include "qemu/units.h"

#include <errno.h>

#include "mfs-conn.h"
#include "mfs-proto.h"

#define MFS_MASTER_DEFAULT_PORT 9421
#define MFS_CHUNK_TTL_MS_DEFAULT 3000
#define MFS_READAHEAD_MAX (1 * MiB)
#define MFS_WRITE_COALESCE_MAX (4 * MiB)

typedef struct MFSChunkCacheEntry {
    MFSChunkLocation loc;
    int64_t expires_ns;
    bool writable;
} MFSChunkCacheEntry;

typedef struct MFSReadAhead {
    uint8_t *buf;
    size_t cap;
    size_t len;
    int64_t start;
    int64_t last_read_end;
    bool valid;
} MFSReadAhead;

typedef struct MFSWriteBuffer {
    bool valid;
    int64_t file_offset;
    uint32_t chunk_index;
    GByteArray *buf;
} MFSWriteBuffer;

typedef struct BDRVMFSState {
    char *master_endpoint;
    char *path;
    uint64_t inode;
    int64_t size;

    MFSConnPool conns;

    GHashTable *chunk_cache; /* key=GUINT_TO_POINTER(chunk_index) */
    int64_t chunk_cache_ttl_ns;

    MFSReadAhead ra;
    MFSWriteBuffer wb;

    CoMutex io_lock;
} BDRVMFSState;

static uint64_t mfs_min_u64(uint64_t a, uint64_t b)
{
    return a < b ? a : b;
}

static int mfs_parse_size(const char *s, uint64_t *out)
{
    gchar *end = NULL;
    guint64 val;
    guint64 mult = 1;

    if (!s || !*s) {
        return -EINVAL;
    }

    val = g_ascii_strtoull(s, &end, 10);
    if (end == s) {
        return -EINVAL;
    }

    if (*end) {
        if (!g_ascii_strcasecmp(end, "k") || !g_ascii_strcasecmp(end, "kb")) {
            mult = KiB;
        } else if (!g_ascii_strcasecmp(end, "m") || !g_ascii_strcasecmp(end, "mb")) {
            mult = MiB;
        } else if (!g_ascii_strcasecmp(end, "g") || !g_ascii_strcasecmp(end, "gb")) {
            mult = GiB;
        } else if (!g_ascii_strcasecmp(end, "t") || !g_ascii_strcasecmp(end, "tb")) {
            mult = TiB;
        } else {
            return -EINVAL;
        }
    }

    if (val > UINT64_MAX / mult) {
        return -ERANGE;
    }

    *out = val * mult;
    return 0;
}

static void mfs_chunk_cache_entry_free(gpointer data)
{
    MFSChunkCacheEntry *entry = data;

    if (!entry) {
        return;
    }

    mfs_proto_chunk_location_reset(&entry->loc);
    g_free(entry);
}

static void mfs_read_ahead_reset(BDRVMFSState *s)
{
    s->ra.valid = false;
    s->ra.len = 0;
    s->ra.start = 0;
}

static void mfs_write_buffer_reset(BDRVMFSState *s)
{
    s->wb.valid = false;
    s->wb.file_offset = 0;
    s->wb.chunk_index = 0;

    if (s->wb.buf) {
        g_byte_array_set_size(s->wb.buf, 0);
    }
}

static void mfs_write_buffer_set(BDRVMFSState *s, int64_t offset,
                                 const uint8_t *buf, size_t len)
{
    s->wb.valid = true;
    s->wb.file_offset = offset;
    s->wb.chunk_index = offset / MFS_CHUNK_SIZE;
    g_byte_array_set_size(s->wb.buf, 0);
    g_byte_array_append(s->wb.buf, buf, len);
}

static int mfs_master_register(BDRVMFSState *s, Error **errp)
{
    MFSConn *master = mfs_conn_pool_master(&s->conns);
    int fd;
    int ret;

    mfs_conn_lock(master);
    fd = mfs_conn_get_fd_locked(master, errp);
    if (fd < 0) {
        mfs_conn_unlock(master);
        return fd;
    }

    ret = mfs_proto_register_tools(fd, "qemu-mfs", errp);
    if (ret < 0) {
        mfs_conn_mark_dead_locked(master);
    }
    mfs_conn_unlock(master);
    return ret;
}

static int mfs_master_lookup(BDRVMFSState *s, Error **errp)
{
    MFSConn *master = mfs_conn_pool_master(&s->conns);
    int fd;
    int ret;
    uint64_t inode;
    uint64_t size;

    mfs_conn_lock(master);
    fd = mfs_conn_get_fd_locked(master, errp);
    if (fd < 0) {
        mfs_conn_unlock(master);
        return fd;
    }

    ret = mfs_proto_master_lookup_path(fd, s->path, &inode, &size, errp);
    if (ret < 0) {
        mfs_conn_mark_dead_locked(master);
        mfs_conn_unlock(master);
        return ret;
    }

    mfs_conn_unlock(master);

    s->inode = inode;
    if (size > 0) {
        s->size = size;
    }
    return 0;
}

static int mfs_master_get_chunk(BDRVMFSState *s, uint32_t chunk_index,
                                bool write, MFSChunkCacheEntry **out_entry,
                                uint64_t *write_id,
                                Error **errp)
{
    gpointer key = GUINT_TO_POINTER(chunk_index);
    MFSChunkCacheEntry *entry = g_hash_table_lookup(s->chunk_cache, key);
    int64_t now = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);

    if (write_id) {
        *write_id = 0;
    }

    if (entry && now < entry->expires_ns && (!write || entry->writable)) {
        *out_entry = entry;
        return 0;
    }

    if (!entry) {
        entry = g_new0(MFSChunkCacheEntry, 1);
        mfs_proto_chunk_location_init(&entry->loc);
        g_hash_table_insert(s->chunk_cache, key, entry);
    }

    {
        MFSConn *master = mfs_conn_pool_master(&s->conns);
        int fd;
        int ret;

        mfs_conn_lock(master);
        fd = mfs_conn_get_fd_locked(master, errp);
        if (fd < 0) {
            mfs_conn_unlock(master);
            return fd;
        }

        if (write) {
            ret = mfs_proto_master_write_chunk(fd, s->inode, chunk_index,
                                               &entry->loc, write_id, errp);
        } else {
            ret = mfs_proto_master_read_chunk(fd, s->inode, chunk_index,
                                              &entry->loc, errp);
        }

        if (ret < 0) {
            mfs_conn_mark_dead_locked(master);
            mfs_conn_unlock(master);
            return ret;
        }

        mfs_conn_unlock(master);
    }

    entry->expires_ns = now + s->chunk_cache_ttl_ns;
    entry->writable = write;
    *out_entry = entry;
    return 0;
}

static int mfs_chunkserver_read(BDRVMFSState *s, MFSChunkCacheEntry *entry,
                                uint32_t chunk_offset, uint32_t length,
                                uint8_t *out, Error **errp)
{
    uint32_t i;
    Error *last_err = NULL;

    for (i = 0; i < entry->loc.replicas->len; i++) {
        MFSChunkReplica *rep = g_ptr_array_index(entry->loc.replicas, i);
        MFSConn *conn = mfs_conn_pool_get_chunk(&s->conns, rep->host, rep->port,
                                                errp);
        int fd;
        int ret;

        if (!conn) {
            return -EIO;
        }

        mfs_conn_lock(conn);
        fd = mfs_conn_get_fd_locked(conn, &last_err);
        if (fd < 0) {
            mfs_conn_unlock(conn);
            continue;
        }

        ret = mfs_proto_cs_read(fd, &entry->loc, chunk_offset, length,
                                out, &last_err);
        if (ret < 0) {
            mfs_conn_mark_dead_locked(conn);
            mfs_conn_unlock(conn);
            continue;
        }

        mfs_conn_unlock(conn);
        return 0;
    }

    if (last_err) {
        error_propagate(errp, last_err);
    } else {
        error_setg(errp, "mfs: no chunkserver replicas available for read");
    }

    return -EIO;
}

static int mfs_chunkserver_write(BDRVMFSState *s, MFSChunkCacheEntry *entry,
                                 uint32_t chunk_offset, const uint8_t *buf,
                                 uint32_t length, Error **errp)
{
    uint32_t i;
    Error *last_err = NULL;

    for (i = 0; i < entry->loc.replicas->len; i++) {
        MFSChunkReplica *rep = g_ptr_array_index(entry->loc.replicas, i);
        MFSConn *conn = mfs_conn_pool_get_chunk(&s->conns, rep->host, rep->port,
                                                errp);
        int fd;
        int ret;

        if (!conn) {
            return -EIO;
        }

        mfs_conn_lock(conn);
        fd = mfs_conn_get_fd_locked(conn, &last_err);
        if (fd < 0) {
            mfs_conn_unlock(conn);
            continue;
        }

        ret = mfs_proto_cs_write(fd, &entry->loc, chunk_offset,
                                 buf, length, &last_err);
        if (ret < 0) {
            mfs_conn_mark_dead_locked(conn);
            mfs_conn_unlock(conn);
            continue;
        }

        mfs_conn_unlock(conn);
        return 0;
    }

    if (last_err) {
        error_propagate(errp, last_err);
    } else {
        error_setg(errp, "mfs: no chunkserver replicas available for write");
    }

    return -EIO;
}

static int mfs_backend_read_locked(BDRVMFSState *s, int64_t offset,
                                   uint8_t *buf, uint64_t bytes,
                                   Error **errp)
{
    uint64_t pos = 0;

    while (pos < bytes) {
        uint64_t file_off = offset + pos;
        uint32_t chunk_index = file_off / MFS_CHUNK_SIZE;
        uint32_t chunk_off = file_off % MFS_CHUNK_SIZE;
        uint32_t this_len = mfs_min_u64(bytes - pos,
                                        MFS_CHUNK_SIZE - chunk_off);
        MFSChunkCacheEntry *entry;
        int ret;

        ret = mfs_master_get_chunk(s, chunk_index, false, &entry, NULL, errp);
        if (ret < 0) {
            return ret;
        }

        ret = mfs_chunkserver_read(s, entry, chunk_off, this_len,
                                   buf + pos, errp);
        if (ret < 0) {
            return ret;
        }

        pos += this_len;
    }

    return 0;
}

static int mfs_backend_write_locked(BDRVMFSState *s, int64_t offset,
                                    const uint8_t *buf, uint64_t bytes,
                                    Error **errp)
{
    uint64_t pos = 0;

    while (pos < bytes) {
        uint64_t file_off = offset + pos;
        uint32_t chunk_index = file_off / MFS_CHUNK_SIZE;
        uint32_t chunk_off = file_off % MFS_CHUNK_SIZE;
        uint32_t this_len = mfs_min_u64(bytes - pos,
                                        MFS_CHUNK_SIZE - chunk_off);
        MFSChunkCacheEntry *entry;
        uint64_t write_id = 0;
        int ret;

        ret = mfs_master_get_chunk(s, chunk_index, true, &entry,
                                   &write_id, errp);
        if (ret < 0) {
            return ret;
        }

        ret = mfs_chunkserver_write(s, entry, chunk_off, buf + pos,
                                    this_len, errp);

        {
            MFSConn *master = mfs_conn_pool_master(&s->conns);
            int fd;
            int end_ret;
            uint8_t status = ret < 0 ? 1 : 0;

            mfs_conn_lock(master);
            fd = mfs_conn_get_fd_locked(master, errp);
            if (fd < 0) {
                mfs_conn_unlock(master);
                return fd;
            }

            end_ret = mfs_proto_master_write_chunk_end(fd, s->inode,
                                                       chunk_index,
                                                       entry->loc.chunk_id,
                                                       entry->loc.version,
                                                       write_id,
                                                       status,
                                                       errp);
            if (end_ret < 0) {
                mfs_conn_mark_dead_locked(master);
                mfs_conn_unlock(master);
                return end_ret;
            }
            mfs_conn_unlock(master);
        }

        if (ret < 0) {
            return ret;
        }

        pos += this_len;
    }

    return 0;
}

static int mfs_flush_write_buffer_locked(BDRVMFSState *s, Error **errp)
{
    int ret;

    if (!s->wb.valid || !s->wb.buf->len) {
        return 0;
    }

    ret = mfs_backend_write_locked(s, s->wb.file_offset,
                                   s->wb.buf->data,
                                   s->wb.buf->len,
                                   errp);
    if (ret < 0) {
        return ret;
    }

    if ((uint64_t)(s->wb.file_offset + s->wb.buf->len) > s->size) {
        s->size = s->wb.file_offset + s->wb.buf->len;
    }

    mfs_write_buffer_reset(s);
    return 0;
}

static bool mfs_write_can_coalesce(BDRVMFSState *s, int64_t offset,
                                   uint64_t bytes)
{
    uint64_t new_end;

    if (!s->wb.valid) {
        return false;
    }

    if (offset != s->wb.file_offset + s->wb.buf->len) {
        return false;
    }

    if (s->wb.buf->len + bytes > MFS_WRITE_COALESCE_MAX) {
        return false;
    }

    new_end = offset + bytes;

    if (offset / MFS_CHUNK_SIZE != s->wb.chunk_index) {
        return false;
    }

    if ((new_end - 1) / MFS_CHUNK_SIZE != s->wb.chunk_index) {
        return false;
    }

    return true;
}

static int bdrv_mfs_open(BlockDriverState *bs, QDict *options, int flags,
                         Error **errp)
{
    BDRVMFSState *s = bs->opaque;
    const char *master;
    const char *path;
    const char *size_opt;
    const char *ttl_opt;
    uint64_t parsed = 0;
    int ret;

    (void)flags;

    master = qdict_get_try_str(options, "master");
    path = qdict_get_try_str(options, "path");
    size_opt = qdict_get_try_str(options, "size");
    ttl_opt = qdict_get_try_str(options, "chunk-cache-ttl-ms");

    if (!master || !path) {
        error_setg(errp,
                   "mfs: missing required options (master=HOST:PORT,path=/file)");
        return -EINVAL;
    }

    s->master_endpoint = g_strdup(master);
    s->path = g_strdup(path);
    s->size = 0;

    s->chunk_cache_ttl_ns = MFS_CHUNK_TTL_MS_DEFAULT * 1000LL * 1000LL;
    if (ttl_opt && *ttl_opt) {
        uint64_t ttl_ms;

        ret = mfs_parse_size(ttl_opt, &ttl_ms);
        if (!ret) {
            s->chunk_cache_ttl_ns = ttl_ms * 1000LL * 1000LL;
        }
    }

    if (size_opt && *size_opt) {
        ret = mfs_parse_size(size_opt, &parsed);
        if (ret < 0) {
            error_setg(errp, "mfs: invalid size option '%s'", size_opt);
            return ret;
        }
        s->size = parsed;
    }

    qdict_del(options, "master");
    qdict_del(options, "path");
    qdict_del(options, "size");
    qdict_del(options, "chunk-cache-ttl-ms");

    ret = mfs_conn_pool_init(&s->conns, s->master_endpoint,
                             MFS_MASTER_DEFAULT_PORT, errp);
    if (ret < 0) {
        goto fail;
    }

    s->chunk_cache = g_hash_table_new_full(g_direct_hash, g_direct_equal,
                                           NULL, mfs_chunk_cache_entry_free);

    s->ra.buf = NULL;
    s->ra.cap = 0;
    s->ra.len = 0;
    s->ra.valid = false;
    s->ra.last_read_end = -1;

    s->wb.buf = g_byte_array_new();
    s->wb.valid = false;

    qemu_co_mutex_init(&s->io_lock);

    ret = mfs_master_register(s, errp);
    if (ret < 0) {
        goto fail;
    }

    ret = mfs_master_lookup(s, errp);
    if (ret < 0) {
        goto fail;
    }

    return 0;

fail:
    if (s->wb.buf) {
        g_byte_array_unref(s->wb.buf);
        s->wb.buf = NULL;
    }
    if (s->ra.buf) {
        g_free(s->ra.buf);
        s->ra.buf = NULL;
    }
    if (s->chunk_cache) {
        g_hash_table_destroy(s->chunk_cache);
        s->chunk_cache = NULL;
    }
    mfs_conn_pool_cleanup(&s->conns);
    g_free(s->master_endpoint);
    s->master_endpoint = NULL;
    g_free(s->path);
    s->path = NULL;
    return ret;
}

static void bdrv_mfs_close(BlockDriverState *bs)
{
    BDRVMFSState *s = bs->opaque;

    if (s->wb.valid && s->wb.buf && s->wb.buf->len) {
        Error *local_err = NULL;

        if (mfs_backend_write_locked(s, s->wb.file_offset,
                                     s->wb.buf->data, s->wb.buf->len,
                                     &local_err) < 0) {
            error_reportf_err(local_err, "mfs close flush failed: ");
        }
    }
    mfs_write_buffer_reset(s);

    if (s->wb.buf) {
        g_byte_array_unref(s->wb.buf);
        s->wb.buf = NULL;
    }

    if (s->ra.buf) {
        g_free(s->ra.buf);
        s->ra.buf = NULL;
    }

    if (s->chunk_cache) {
        g_hash_table_destroy(s->chunk_cache);
        s->chunk_cache = NULL;
    }

    mfs_conn_pool_cleanup(&s->conns);

    g_free(s->master_endpoint);
    s->master_endpoint = NULL;

    g_free(s->path);
    s->path = NULL;
}

static int coroutine_fn bdrv_mfs_co_preadv(BlockDriverState *bs,
                                           int64_t offset,
                                           int64_t bytes,
                                           QEMUIOVector *qiov,
                                           BdrvRequestFlags flags)
{
    BDRVMFSState *s = bs->opaque;
    uint64_t read_len;
    uint64_t tail_zeros = 0;
    int ret = 0;
    Error *local_err = NULL;

    (void)flags;

    if (bytes == 0) {
        return 0;
    }

    qemu_co_mutex_lock(&s->io_lock);

    ret = mfs_flush_write_buffer_locked(s, &local_err);
    if (ret < 0) {
        goto out;
    }

    if (offset >= s->size) {
        qemu_iovec_memset(qiov, 0, 0, bytes);
        ret = 0;
        goto out;
    }

    read_len = mfs_min_u64(bytes, s->size - offset);
    tail_zeros = bytes - read_len;

    if (read_len && s->ra.valid &&
        offset >= s->ra.start &&
        offset + read_len <= s->ra.start + s->ra.len) {
        size_t off = offset - s->ra.start;

        qemu_iovec_from_buf(qiov, 0, s->ra.buf + off, read_len);
    } else if (read_len) {
        g_autofree uint8_t *tmp = g_malloc(read_len);

        ret = mfs_backend_read_locked(s, offset, tmp, read_len, &local_err);
        if (ret < 0) {
            goto out;
        }

        qemu_iovec_from_buf(qiov, 0, tmp, read_len);

        if (s->ra.last_read_end == offset) {
            uint64_t prefetch_len = mfs_min_u64(MFS_READAHEAD_MAX,
                                                s->size - (offset + read_len));

            if (prefetch_len) {
                if (s->ra.cap < prefetch_len) {
                    s->ra.buf = g_realloc(s->ra.buf, prefetch_len);
                    s->ra.cap = prefetch_len;
                }

                ret = mfs_backend_read_locked(s, offset + read_len,
                                              s->ra.buf,
                                              prefetch_len,
                                              &local_err);
                if (!ret) {
                    s->ra.start = offset + read_len;
                    s->ra.len = prefetch_len;
                    s->ra.valid = true;
                } else {
                    mfs_read_ahead_reset(s);
                    ret = 0;
                }
            } else {
                mfs_read_ahead_reset(s);
            }
        } else {
            mfs_read_ahead_reset(s);
        }
    }

    s->ra.last_read_end = offset + read_len;

    if (tail_zeros) {
        qemu_iovec_memset(qiov, read_len, 0, tail_zeros);
    }

out:
    qemu_co_mutex_unlock(&s->io_lock);

    if (ret < 0) {
        error_reportf_err(local_err, "mfs read failed: ");
    }

    return ret;
}

static int coroutine_fn bdrv_mfs_co_pwritev(BlockDriverState *bs,
                                            int64_t offset,
                                            int64_t bytes,
                                            QEMUIOVector *qiov,
                                            BdrvRequestFlags flags)
{
    BDRVMFSState *s = bs->opaque;
    g_autofree uint8_t *tmp = NULL;
    int ret = 0;
    Error *local_err = NULL;

    if (bytes == 0) {
        return 0;
    }

    tmp = g_malloc(bytes);
    qemu_iovec_to_buf(qiov, 0, tmp, bytes);

    qemu_co_mutex_lock(&s->io_lock);

    mfs_read_ahead_reset(s);

    if ((offset / MFS_CHUNK_SIZE) != ((offset + bytes - 1) / MFS_CHUNK_SIZE)) {
        ret = mfs_flush_write_buffer_locked(s, &local_err);
        if (ret < 0) {
            goto out;
        }

        ret = mfs_backend_write_locked(s, offset, tmp, bytes, &local_err);
        if (ret < 0) {
            goto out;
        }
    } else {
        if (mfs_write_can_coalesce(s, offset, bytes)) {
            g_byte_array_append(s->wb.buf, tmp, bytes);
        } else {
            ret = mfs_flush_write_buffer_locked(s, &local_err);
            if (ret < 0) {
                goto out;
            }
            mfs_write_buffer_set(s, offset, tmp, bytes);
        }

        if (s->wb.buf->len >= MFS_WRITE_COALESCE_MAX || (flags & BDRV_REQ_FUA)) {
            ret = mfs_flush_write_buffer_locked(s, &local_err);
            if (ret < 0) {
                goto out;
            }
        }
    }

    if ((uint64_t)(offset + bytes) > s->size) {
        s->size = offset + bytes;
    }

out:
    qemu_co_mutex_unlock(&s->io_lock);

    if (ret < 0) {
        error_reportf_err(local_err, "mfs write failed: ");
    }

    return ret;
}

static int coroutine_fn bdrv_mfs_co_flush(BlockDriverState *bs)
{
    BDRVMFSState *s = bs->opaque;
    int ret;
    Error *local_err = NULL;

    qemu_co_mutex_lock(&s->io_lock);
    ret = mfs_flush_write_buffer_locked(s, &local_err);
    qemu_co_mutex_unlock(&s->io_lock);

    if (ret < 0) {
        error_reportf_err(local_err, "mfs flush failed: ");
    }

    return ret;
}

static int coroutine_fn bdrv_mfs_co_pdiscard(BlockDriverState *bs,
                                             int64_t offset,
                                             int64_t bytes)
{
    BDRVMFSState *s = bs->opaque;
    uint8_t zeros[MFS_BLOCK_SIZE] = { 0 };
    int ret = 0;
    Error *local_err = NULL;

    qemu_co_mutex_lock(&s->io_lock);

    ret = mfs_flush_write_buffer_locked(s, &local_err);
    if (ret < 0) {
        goto out;
    }

    while (bytes > 0) {
        uint64_t chunk_remaining = MFS_CHUNK_SIZE - (offset % MFS_CHUNK_SIZE);
        uint64_t step = mfs_min_u64(bytes, chunk_remaining);
        uint64_t pos = 0;

        while (pos < step) {
            uint64_t block = mfs_min_u64(step - pos, sizeof(zeros));

            ret = mfs_backend_write_locked(s, offset + pos, zeros,
                                           block, &local_err);
            if (ret < 0) {
                goto out;
            }
            pos += block;
        }

        offset += step;
        bytes -= step;
    }

out:
    qemu_co_mutex_unlock(&s->io_lock);

    if (ret < 0) {
        error_reportf_err(local_err, "mfs discard failed: ");
    }

    return ret;
}

static int64_t coroutine_fn bdrv_mfs_getlength(BlockDriverState *bs)
{
    BDRVMFSState *s = bs->opaque;

    return s->size;
}

static int coroutine_fn bdrv_mfs_co_create(BlockdevCreateOptions *options,
                                           Error **errp)
{
    (void)options;
    error_setg(errp,
               "mfs: generic co_create requires QAPI mfs options; use co_create_opts/master/path/size");
    return -ENOTSUP;
}

static int coroutine_fn bdrv_mfs_co_create_opts(BlockDriver *drv,
                                                const char *filename,
                                                QemuOpts *opts,
                                                Error **errp)
{
    const char *master = qemu_opt_get(opts, "master");
    const char *path = qemu_opt_get(opts, "path");
    uint64_t size;
    MFSConnPool pool;
    uint64_t inode;
    int ret;

    (void)drv;

    if (!master) {
        error_setg(errp, "mfs create: missing master option");
        return -EINVAL;
    }

    if (!path || !*path) {
        path = filename;
    }

    if (!path || !*path) {
        error_setg(errp, "mfs create: missing path option");
        return -EINVAL;
    }

    size = qemu_opt_get_size_del(opts, BLOCK_OPT_SIZE, 0);
    size = ROUND_UP(size, BDRV_SECTOR_SIZE);
    if (!size) {
        error_setg(errp, "mfs create: size must be > 0");
        return -EINVAL;
    }

    ret = mfs_conn_pool_init(&pool, master, MFS_MASTER_DEFAULT_PORT, errp);
    if (ret < 0) {
        return ret;
    }

    {
        MFSConn *master_conn = mfs_conn_pool_master(&pool);
        int fd;

        mfs_conn_lock(master_conn);
        fd = mfs_conn_get_fd_locked(master_conn, errp);
        if (fd < 0) {
            mfs_conn_unlock(master_conn);
            mfs_conn_pool_cleanup(&pool);
            return fd;
        }

        ret = mfs_proto_register_tools(fd, "qemu-mfs-create", errp);
        if (ret < 0) {
            mfs_conn_mark_dead_locked(master_conn);
            mfs_conn_unlock(master_conn);
            mfs_conn_pool_cleanup(&pool);
            return ret;
        }

        ret = mfs_proto_master_create_path(fd, path, size, &inode, errp);
        if (ret < 0) {
            mfs_conn_mark_dead_locked(master_conn);
            mfs_conn_unlock(master_conn);
            mfs_conn_pool_cleanup(&pool);
            return ret;
        }
        (void)inode;

        mfs_conn_unlock(master_conn);
    }

    mfs_conn_pool_cleanup(&pool);
    return 0;
}

static QemuOptsList mfs_create_opts = {
    .name = "mfs-create-opts",
    .head = QTAILQ_HEAD_INITIALIZER(mfs_create_opts.head),
    .desc = {
        {
            .name = "master",
            .type = QEMU_OPT_STRING,
            .help = "MooseFS master endpoint host:port",
        },
        {
            .name = "path",
            .type = QEMU_OPT_STRING,
            .help = "MooseFS path for the virtual disk file",
        },
        {
            .name = BLOCK_OPT_SIZE,
            .type = QEMU_OPT_SIZE,
            .help = "Virtual disk size",
        },
        {
            .name = "chunk-cache-ttl-ms",
            .type = QEMU_OPT_NUMBER,
            .help = "Chunk location cache TTL in milliseconds",
        },
        { /* end */ }
    },
};

static const char *const mfs_strong_runtime_opts[] = {
    "master",
    "path",
    NULL,
};

static BlockDriver bdrv_mfs = {
    .format_name           = "mfs",
    .protocol_name         = "mfs",
    .instance_size         = sizeof(BDRVMFSState),

    .bdrv_open             = bdrv_mfs_open,
    .bdrv_close            = bdrv_mfs_close,

    .bdrv_co_create        = bdrv_mfs_co_create,
    .bdrv_co_create_opts   = bdrv_mfs_co_create_opts,
    .create_opts           = &mfs_create_opts,

    .bdrv_co_preadv        = bdrv_mfs_co_preadv,
    .bdrv_co_pwritev       = bdrv_mfs_co_pwritev,
    .bdrv_co_flush_to_disk = bdrv_mfs_co_flush,
    .bdrv_co_pdiscard      = bdrv_mfs_co_pdiscard,
    .bdrv_co_getlength     = bdrv_mfs_getlength,

    .bdrv_has_zero_init    = bdrv_has_zero_init_1,

    .strong_runtime_opts   = mfs_strong_runtime_opts,
};

static void bdrv_mfs_init(void)
{
    bdrv_register(&bdrv_mfs);
}

block_init(bdrv_mfs_init);
