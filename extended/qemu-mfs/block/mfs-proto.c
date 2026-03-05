#include "qemu/osdep.h"

#include "mfs-proto.h"

#include <arpa/inet.h>
#include <endian.h>
#include <ctype.h>
#include <errno.h>
#include <sys/socket.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

static uint32_t mfs_crc32_table[256];
static bool mfs_crc32_table_ready;

static void mfs_crc32_init(void)
{
    uint32_t i;

    if (mfs_crc32_table_ready) {
        return;
    }

    for (i = 0; i < 256; i++) {
        uint32_t c = i;
        int j;

        for (j = 0; j < 8; j++) {
            c = (c & 1) ? (0xedb88320U ^ (c >> 1)) : (c >> 1);
        }
        mfs_crc32_table[i] = c;
    }
    mfs_crc32_table_ready = true;
}

static uint32_t mfs_crc32_update(uint32_t seed, const uint8_t *buf, size_t len)
{
    size_t i;
    uint32_t c = ~seed;

    for (i = 0; i < len; i++) {
        c = mfs_crc32_table[(c ^ buf[i]) & 0xff] ^ (c >> 8);
    }
    return ~c;
}

static inline void mfs_put_be32(uint8_t *p, uint32_t v)
{
    uint32_t be = htonl(v);
    memcpy(p, &be, sizeof(be));
}

static inline void mfs_put_be64(uint8_t *p, uint64_t v)
{
    uint64_t be = htobe64(v);
    memcpy(p, &be, sizeof(be));
}

static inline uint32_t mfs_get_be32(const uint8_t *p)
{
    uint32_t be;

    memcpy(&be, p, sizeof(be));
    return ntohl(be);
}

static inline uint64_t mfs_get_be64(const uint8_t *p)
{
    uint64_t be;

    memcpy(&be, p, sizeof(be));
    return be64toh(be);
}

static int mfs_send_all(int fd, const uint8_t *buf, size_t len, Error **errp)
{
    size_t off = 0;

    while (off < len) {
        ssize_t ret = send(fd, buf + off, len - off, MSG_NOSIGNAL);

        if (ret < 0) {
            if (errno == EINTR) {
                continue;
            }
            error_setg_errno(errp, errno, "mfs: send failed");
            return -errno;
        }
        if (ret == 0) {
            error_setg(errp, "mfs: short send (peer closed)");
            return -EPIPE;
        }
        off += ret;
    }

    return 0;
}

static int mfs_recv_all(int fd, uint8_t *buf, size_t len, Error **errp)
{
    size_t off = 0;

    while (off < len) {
        ssize_t ret = recv(fd, buf + off, len - off, 0);

        if (ret < 0) {
            if (errno == EINTR) {
                continue;
            }
            error_setg_errno(errp, errno, "mfs: recv failed");
            return -errno;
        }
        if (ret == 0) {
            error_setg(errp, "mfs: peer disconnected");
            return -EPIPE;
        }
        off += ret;
    }

    return 0;
}

static void mfs_chunk_replica_free(gpointer data)
{
    MFSChunkReplica *rep = data;

    if (!rep) {
        return;
    }

    g_free(rep->host);
    g_free(rep);
}

void mfs_proto_chunk_location_init(MFSChunkLocation *loc)
{
    memset(loc, 0, sizeof(*loc));
    loc->replicas = g_ptr_array_new_with_free_func(mfs_chunk_replica_free);
}

void mfs_proto_chunk_location_reset(MFSChunkLocation *loc)
{
    if (!loc) {
        return;
    }

    if (loc->replicas) {
        g_ptr_array_free(loc->replicas, true);
    }
    memset(loc, 0, sizeof(*loc));
}

static int mfs_proto_expect_status_ok(const GByteArray *payload,
                                      const char *what,
                                      Error **errp)
{
    if (!payload || payload->len == 0) {
        return 0;
    }

    if (payload->data[0] != MFS_STATUS_OK) {
        error_setg(errp, "mfs: %s failed with status=%u", what,
                   payload->data[0]);
        return -EIO;
    }
    return 0;
}

int mfs_proto_send_packet(int fd, uint32_t type, const uint8_t *data,
                          uint32_t len, Error **errp)
{
    uint8_t hdr[MFS_PACKET_HEADER_LEN];
    int ret;

    mfs_put_be32(&hdr[0], type);
    mfs_put_be32(&hdr[4], len);

    ret = mfs_send_all(fd, hdr, sizeof(hdr), errp);
    if (ret < 0) {
        return ret;
    }
    if (len == 0) {
        return 0;
    }
    return mfs_send_all(fd, data, len, errp);
}

int mfs_proto_recv_packet(int fd, uint32_t *type, GByteArray **payload,
                          Error **errp)
{
    uint8_t hdr[MFS_PACKET_HEADER_LEN];
    uint32_t msg_type;
    uint32_t len;
    int ret;
    GByteArray *out;

    ret = mfs_recv_all(fd, hdr, sizeof(hdr), errp);
    if (ret < 0) {
        return ret;
    }

    msg_type = mfs_get_be32(&hdr[0]);
    len = mfs_get_be32(&hdr[4]);

    out = g_byte_array_sized_new(len);
    g_byte_array_set_size(out, len);

    if (len) {
        ret = mfs_recv_all(fd, out->data, len, errp);
        if (ret < 0) {
            g_byte_array_unref(out);
            return ret;
        }
    }

    *type = msg_type;
    *payload = out;
    return 0;
}

static void mfs_payload_append_u32(GByteArray *payload, uint32_t v)
{
    uint8_t tmp[4];

    mfs_put_be32(tmp, v);
    g_byte_array_append(payload, tmp, sizeof(tmp));
}

static void mfs_payload_append_u64(GByteArray *payload, uint64_t v)
{
    uint8_t tmp[8];

    mfs_put_be64(tmp, v);
    g_byte_array_append(payload, tmp, sizeof(tmp));
}

static void mfs_payload_append_u16(GByteArray *payload, uint16_t v)
{
    uint16_t be = htons(v);

    g_byte_array_append(payload, (const uint8_t *)&be, sizeof(be));
}

static int mfs_proto_roundtrip(int fd, uint32_t req_type,
                               const GByteArray *req,
                               uint32_t expected_reply,
                               GByteArray **resp,
                               Error **errp)
{
    GByteArray *payload = NULL;
    uint32_t reply_type;
    int ret;

    ret = mfs_proto_send_packet(fd, req_type,
                                req ? req->data : NULL,
                                req ? req->len : 0,
                                errp);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_recv_packet(fd, &reply_type, &payload, errp);
    if (ret < 0) {
        return ret;
    }

    if (expected_reply && reply_type != expected_reply) {
        g_byte_array_unref(payload);
        error_setg(errp,
                   "mfs: unexpected reply type %u for request %u (wanted %u)",
                   reply_type, req_type, expected_reply);
        return -EPROTO;
    }

    *resp = payload;
    return 0;
}

int mfs_proto_register_tools(int fd, const char *client_id, Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    const char *id = client_id ? client_id : "qemu-mfs";
    uint32_t expected = MFS_CLTOMA_FUSE_REGISTER + 1;
    uint16_t id_len = strlen(id);
    int ret;

    mfs_payload_append_u32(req, MFS_REGISTER_TOOLS);
    mfs_payload_append_u32(req, (uint32_t)getpid());
    mfs_payload_append_u64(req, (uint64_t)time(NULL));
    mfs_payload_append_u16(req, id_len);
    g_byte_array_append(req, (const uint8_t *)id, id_len);

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_REGISTER, req,
                              expected, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_expect_status_ok(resp, "session register", errp);
    g_byte_array_unref(resp);
    return ret;
}

uint64_t mfs_proto_path_fallback_inode(const char *path)
{
    const uint8_t *p = (const uint8_t *)path;
    uint64_t h = 1469598103934665603ULL;

    while (*p) {
        h ^= *p++;
        h *= 1099511628211ULL;
    }

    return h ? h : 1;
}

int mfs_proto_master_lookup_path(int fd, const char *path, uint64_t *inode,
                                 uint64_t *size, Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t expected = MFS_CLTOMA_FUSE_LOOKUP_PATH + 1;
    uint32_t path_len = strlen(path);
    int ret;

    mfs_payload_append_u32(req, path_len);
    g_byte_array_append(req, (const uint8_t *)path, path_len);

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_LOOKUP_PATH, req,
                              expected, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        if (ret == -EPROTO || ret == -ENOTSUP) {
            /*
             * Keep the session alive when the lookup extension is unavailable.
             * Data I/O still resolves chunks by inode/chunk index RPCs.
             */
            *inode = mfs_proto_path_fallback_inode(path);
            *size = 0;
            return 0;
        }
        return ret;
    }

    if (resp->len >= 1 && resp->data[0] != MFS_STATUS_OK) {
        g_byte_array_unref(resp);
        *inode = mfs_proto_path_fallback_inode(path);
        *size = 0;
        return 0;
    }

    if (resp->len >= 1 + 8) {
        *inode = mfs_get_be64(&resp->data[1]);
    } else {
        *inode = mfs_proto_path_fallback_inode(path);
    }

    if (resp->len >= 1 + 8 + 8) {
        *size = mfs_get_be64(&resp->data[9]);
    } else {
        *size = 0;
    }

    g_byte_array_unref(resp);
    return 0;
}

int mfs_proto_master_create_path(int fd, const char *path, uint64_t size,
                                 uint64_t *inode, Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t expected = MFS_CLTOMA_FUSE_CREATE_PATH + 1;
    uint32_t path_len = strlen(path);
    int ret;

    mfs_payload_append_u32(req, path_len);
    g_byte_array_append(req, (const uint8_t *)path, path_len);
    mfs_payload_append_u64(req, size);

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_CREATE_PATH, req,
                              expected, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_expect_status_ok(resp, "create path", errp);
    if (ret < 0) {
        g_byte_array_unref(resp);
        return ret;
    }

    if (resp->len >= 1 + 8) {
        *inode = mfs_get_be64(&resp->data[1]);
    } else {
        *inode = mfs_proto_path_fallback_inode(path);
    }

    g_byte_array_unref(resp);
    return 0;
}

static bool mfs_host_bytes_printable(const uint8_t *p, size_t len)
{
    size_t i;

    for (i = 0; i < len; i++) {
        unsigned char c = p[i];

        if (!(isalnum(c) || c == '.' || c == '-' || c == '_' || c == ':')) {
            return false;
        }
    }
    return true;
}

static int mfs_parse_replicas(const uint8_t *buf, size_t len,
                              uint32_t replica_count,
                              GPtrArray *replicas,
                              Error **errp)
{
    size_t pos = 0;
    uint32_t i;

    for (i = 0; i < replica_count; i++) {
        MFSChunkReplica *rep;

        if (pos >= len) {
            error_setg(errp, "mfs: replica list truncated");
            return -EPROTO;
        }

        rep = g_new0(MFSChunkReplica, 1);

        if (pos + 1 <= len) {
            uint8_t host_len = buf[pos];

            if (host_len > 0 && pos + 1 + host_len + 2 <= len &&
                mfs_host_bytes_printable(&buf[pos + 1], host_len)) {
                rep->host = g_strndup((const char *)&buf[pos + 1], host_len);
                pos += 1 + host_len;
                rep->port = ((uint16_t)buf[pos] << 8) | buf[pos + 1];
                pos += 2;
                g_ptr_array_add(replicas, rep);
                continue;
            }
        }

        if (pos + 6 <= len) {
            rep->host = g_strdup_printf("%u.%u.%u.%u",
                                        buf[pos], buf[pos + 1],
                                        buf[pos + 2], buf[pos + 3]);
            rep->port = ((uint16_t)buf[pos + 4] << 8) | buf[pos + 5];
            pos += 6;
            g_ptr_array_add(replicas, rep);
            continue;
        }

        g_free(rep);
        error_setg(errp, "mfs: malformed replica descriptor");
        return -EPROTO;
    }

    return 0;
}

static int mfs_decode_chunk_location(const GByteArray *resp,
                                     bool has_write_id,
                                     MFSChunkLocation *loc,
                                     uint64_t *write_id,
                                     Error **errp)
{
    const uint8_t *p = resp->data;
    size_t len = resp->len;
    size_t pos = 0;
    uint8_t status = MFS_STATUS_OK;
    uint32_t replica_count;
    int ret;

    if (len >= 1 && p[0] <= 32) {
        status = p[0];
        pos++;
    }

    if (status != MFS_STATUS_OK) {
        error_setg(errp, "mfs: master returned status=%u", status);
        return -EIO;
    }

    if (pos + 8 + 4 > len) {
        error_setg(errp, "mfs: short chunk location payload");
        return -EPROTO;
    }

    mfs_proto_chunk_location_reset(loc);
    mfs_proto_chunk_location_init(loc);

    loc->chunk_id = mfs_get_be64(&p[pos]);
    pos += 8;
    loc->version = mfs_get_be32(&p[pos]);
    pos += 4;

    if (has_write_id) {
        if (pos + 8 > len) {
            error_setg(errp, "mfs: short write-chunk lease payload");
            return -EPROTO;
        }
        if (write_id) {
            *write_id = mfs_get_be64(&p[pos]);
        }
        pos += 8;
    }

    if (pos + 2 <= len) {
        replica_count = ((uint32_t)p[pos] << 8) | p[pos + 1];
        pos += 2;
    } else if (pos + 1 <= len) {
        replica_count = p[pos++];
    } else {
        replica_count = 0;
    }

    ret = mfs_parse_replicas(&p[pos], len - pos, replica_count,
                             loc->replicas, errp);
    if (ret < 0) {
        return ret;
    }

    if (loc->replicas->len == 0) {
        error_setg(errp, "mfs: chunk location has no chunkserver replicas");
        return -ENOENT;
    }

    return 0;
}

int mfs_proto_master_read_chunk(int fd, uint64_t inode, uint32_t chunk_index,
                                MFSChunkLocation *loc, Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t expected = MFS_CLTOMA_FUSE_READ_CHUNK + 1;
    int ret;

    mfs_payload_append_u64(req, inode);
    mfs_payload_append_u32(req, chunk_index);

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_READ_CHUNK, req,
                              expected, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_decode_chunk_location(resp, false, loc, NULL, errp);
    loc->chunk_index = chunk_index;

    g_byte_array_unref(resp);
    return ret;
}

int mfs_proto_master_write_chunk(int fd, uint64_t inode, uint32_t chunk_index,
                                 MFSChunkLocation *loc, uint64_t *write_id,
                                 Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t expected = MFS_CLTOMA_FUSE_WRITE_CHUNK + 1;
    int ret;

    mfs_payload_append_u64(req, inode);
    mfs_payload_append_u32(req, chunk_index);

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_WRITE_CHUNK, req,
                              expected, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_decode_chunk_location(resp, true, loc, write_id, errp);
    loc->chunk_index = chunk_index;

    g_byte_array_unref(resp);
    return ret;
}

int mfs_proto_master_write_chunk_end(int fd, uint64_t inode,
                                     uint32_t chunk_index,
                                     uint64_t chunk_id,
                                     uint32_t version,
                                     uint64_t write_id,
                                     uint8_t status,
                                     Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t expected = MFS_CLTOMA_FUSE_WRITE_CHUNK_END + 1;
    int ret;

    mfs_payload_append_u64(req, inode);
    mfs_payload_append_u32(req, chunk_index);
    mfs_payload_append_u64(req, chunk_id);
    mfs_payload_append_u32(req, version);
    mfs_payload_append_u64(req, write_id);
    g_byte_array_append(req, &status, 1);

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_WRITE_CHUNK_END, req,
                              expected, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_expect_status_ok(resp, "write_chunk_end", errp);
    g_byte_array_unref(resp);
    return ret;
}

static int mfs_validate_read_crc(const uint8_t *crc_ptr, uint32_t blocks,
                                 const uint8_t *data, uint32_t length,
                                 Error **errp)
{
    uint32_t i;

    mfs_crc32_init();

    for (i = 0; i < blocks; i++) {
        uint32_t off = i * MFS_BLOCK_SIZE;
        uint32_t sz = MIN((uint32_t)MFS_BLOCK_SIZE, length - off);
        uint32_t expected = mfs_get_be32(&crc_ptr[i * 4]);
        uint32_t got = mfs_crc32_update(0, data + off, sz);

        if (expected != got) {
            error_setg(errp,
                       "mfs: read CRC mismatch block=%u expected=0x%08x got=0x%08x",
                       i, expected, got);
            return -EIO;
        }
    }

    return 0;
}

int mfs_proto_cs_read(int fd, const MFSChunkLocation *loc,
                      uint32_t chunk_offset, uint32_t length,
                      uint8_t *out, Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t reply_type;
    int ret;
    const uint8_t *p;
    size_t pos = 0;

    mfs_payload_append_u64(req, loc->chunk_id);
    mfs_payload_append_u32(req, loc->version);
    mfs_payload_append_u32(req, chunk_offset);
    mfs_payload_append_u32(req, length);

    ret = mfs_proto_send_packet(fd, MFS_CLTOCS_READ, req->data, req->len, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_recv_packet(fd, &reply_type, &resp, errp);
    if (ret < 0) {
        return ret;
    }

    if (reply_type != MFS_CLTOCS_READ && reply_type != MFS_CLTOCS_READ + 1) {
        g_byte_array_unref(resp);
        error_setg(errp,
                   "mfs: unexpected chunkserver read reply type=%u",
                   reply_type);
        return -EPROTO;
    }

    p = resp->data;
    if (resp->len >= 1 + length) {
        if (p[0] != MFS_STATUS_OK) {
            g_byte_array_unref(resp);
            error_setg(errp, "mfs: chunkserver read failed status=%u", p[0]);
            return -EIO;
        }
        pos = 1;
    }

    if (resp->len >= pos + 4 + length) {
        uint32_t blocks = mfs_get_be32(&p[pos]);
        size_t crc_bytes = (size_t)blocks * 4;

        if (resp->len >= pos + 4 + crc_bytes + length) {
            ret = mfs_validate_read_crc(&p[pos + 4], blocks,
                                        &p[pos + 4 + crc_bytes],
                                        length, errp);
            if (ret < 0) {
                g_byte_array_unref(resp);
                return ret;
            }
            memcpy(out, &p[pos + 4 + crc_bytes], length);
            g_byte_array_unref(resp);
            return 0;
        }
    }

    if (resp->len < pos + length) {
        g_byte_array_unref(resp);
        error_setg(errp, "mfs: short chunkserver read payload");
        return -EIO;
    }

    memcpy(out, &p[pos], length);
    g_byte_array_unref(resp);
    return 0;
}

int mfs_proto_cs_write(int fd, const MFSChunkLocation *loc,
                       uint32_t chunk_offset, const uint8_t *buf,
                       uint32_t length, Error **errp)
{
    GByteArray *req;
    GByteArray *resp = NULL;
    uint32_t blocks = (length + MFS_BLOCK_SIZE - 1) / MFS_BLOCK_SIZE;
    uint32_t i;
    uint32_t reply_type;
    int ret;

    mfs_crc32_init();

    req = g_byte_array_new();
    mfs_payload_append_u64(req, loc->chunk_id);
    mfs_payload_append_u32(req, loc->version);
    mfs_payload_append_u32(req, chunk_offset);
    mfs_payload_append_u32(req, length);
    mfs_payload_append_u32(req, blocks);

    for (i = 0; i < blocks; i++) {
        uint32_t off = i * MFS_BLOCK_SIZE;
        uint32_t sz = MIN((uint32_t)MFS_BLOCK_SIZE, length - off);
        uint8_t crc_be[4];
        uint32_t crc = mfs_crc32_update(0, buf + off, sz);

        mfs_put_be32(crc_be, crc);
        g_byte_array_append(req, crc_be, sizeof(crc_be));
    }

    g_byte_array_append(req, buf, length);

    ret = mfs_proto_send_packet(fd, MFS_CLTOCS_WRITE, req->data, req->len, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_recv_packet(fd, &reply_type, &resp, errp);
    if (ret < 0) {
        return ret;
    }

    if (reply_type != MFS_CLTOCS_WRITE && reply_type != MFS_CLTOCS_WRITE + 1) {
        g_byte_array_unref(resp);
        error_setg(errp,
                   "mfs: unexpected chunkserver write reply type=%u",
                   reply_type);
        return -EPROTO;
    }

    ret = mfs_proto_expect_status_ok(resp, "chunkserver write", errp);
    g_byte_array_unref(resp);
    return ret;
}
