#include "qemu/osdep.h"

#include "mfs-proto.h"

#include <arpa/inet.h>
#include <endian.h>
#include <errno.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

#define MFS_VERSMAJ 4
#define MFS_VERSMID 58
#define MFS_VERSMIN 3

static const char mfs_register_blob_acl[] =
    "DjI1GAQDULI5d2YjA26ypc3ovkhjvhciTQVx3CS4nYgtBoUcsljiVpsErJENHaw0";

static uint32_t mfs_crc32_table[256];
static bool mfs_crc32_table_ready;
static uint32_t mfs_next_msgid = 1;

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

static inline void mfs_put_be16(uint8_t *p, uint16_t v)
{
    uint16_t be = htons(v);
    memcpy(p, &be, sizeof(be));
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

static inline uint16_t mfs_get_be16(const uint8_t *p)
{
    uint16_t be;

    memcpy(&be, p, sizeof(be));
    return ntohs(be);
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

static uint32_t mfs_proto_next_msgid(void)
{
    if (++mfs_next_msgid == 0) {
        mfs_next_msgid = 1;
    }
    return mfs_next_msgid;
}

static int mfs_status_to_errno(uint8_t status)
{
    switch (status) {
    case MFS_STATUS_OK:
        return 0;
    case MFS_ERROR_ENOENT:
        return -ENOENT;
    case MFS_ERROR_ENOTSUP:
        return -ENOTSUP;
    case MFS_ERROR_NOCHUNKSERVERS:
        return -EHOSTUNREACH;
    default:
        return -EIO;
    }
}

static int mfs_set_status_error(Error **errp, const char *what, uint8_t status)
{
    error_setg(errp, "mfs: %s failed with status=%u", what, status);
    return mfs_status_to_errno(status);
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

static int mfs_cs_send_nop(int fd, Error **errp)
{
    return mfs_proto_send_packet(fd, MFS_ANTOAN_NOP, NULL, 0, errp);
}

static int mfs_cs_recv_all(int fd, uint8_t *buf, size_t len, Error **errp)
{
    size_t off = 0;

    while (off < len) {
        struct pollfd pfd = {
            .fd = fd,
            .events = POLLIN,
        };
        int pret;
        ssize_t ret;

        pret = poll(&pfd, 1, 1000);
        if (pret < 0) {
            if (errno == EINTR) {
                continue;
            }
            error_setg_errno(errp, errno, "mfs: chunkserver poll failed");
            return -errno;
        }

        if (pret == 0) {
            int nop_ret = mfs_cs_send_nop(fd, errp);

            if (nop_ret < 0) {
                return nop_ret;
            }
            continue;
        }

        ret = recv(fd, buf + off, len - off, 0);
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

static int mfs_proto_cs_recv_packet(int fd, uint32_t *type, GByteArray **payload,
                                    Error **errp)
{
    uint8_t hdr[MFS_PACKET_HEADER_LEN];
    uint32_t msg_type;
    uint32_t len;
    int ret;
    GByteArray *out;

    ret = mfs_cs_recv_all(fd, hdr, sizeof(hdr), errp);
    if (ret < 0) {
        return ret;
    }

    msg_type = mfs_get_be32(&hdr[0]);
    len = mfs_get_be32(&hdr[4]);

    out = g_byte_array_sized_new(len);
    g_byte_array_set_size(out, len);

    if (len) {
        ret = mfs_cs_recv_all(fd, out->data, len, errp);
        if (ret < 0) {
            g_byte_array_unref(out);
            return ret;
        }
    }

    *type = msg_type;
    *payload = out;
    return 0;
}

static int mfs_proto_roundtrip(int fd, uint32_t req_type,
                               const uint8_t *req, uint32_t req_len,
                               uint32_t expected_reply,
                               GByteArray **resp,
                               Error **errp)
{
    int ret;

    ret = mfs_proto_send_packet(fd, req_type, req, req_len, errp);
    if (ret < 0) {
        return ret;
    }

    for (;;) {
        GByteArray *payload = NULL;
        uint32_t reply_type;

        ret = mfs_proto_recv_packet(fd, &reply_type, &payload, errp);
        if (ret < 0) {
            return ret;
        }

        if (reply_type == MFS_ANTOAN_NOP) {
            g_byte_array_unref(payload);
            continue;
        }

        if (expected_reply && reply_type != expected_reply) {
            g_byte_array_unref(payload);
            error_setg(errp,
                       "mfs: unexpected reply type %u for request %u "
                       "(wanted %u)",
                       reply_type, req_type, expected_reply);
            return -EPROTO;
        }

        *resp = payload;
        return 0;
    }
}

static int mfs_proto_expect_msgid(const GByteArray *resp, uint32_t msgid,
                                  Error **errp)
{
    if (!resp || resp->len < 4) {
        error_setg(errp, "mfs: short master reply");
        return -EPROTO;
    }
    if (mfs_get_be32(resp->data) != msgid) {
        error_setg(errp, "mfs: stale or mismatched master reply");
        return -EPROTO;
    }
    return 0;
}

static int mfs_proto_expect_simple_status(const GByteArray *resp, uint32_t msgid,
                                          const char *what, Error **errp)
{
    int ret;

    ret = mfs_proto_expect_msgid(resp, msgid, errp);
    if (ret < 0) {
        return ret;
    }
    if (resp->len < 5) {
        error_setg(errp, "mfs: short status reply for %s", what);
        return -EPROTO;
    }
    if (resp->data[4] != MFS_STATUS_OK) {
        return mfs_set_status_error(errp, what, resp->data[4]);
    }
    return 0;
}

static void mfs_payload_append_u8(GByteArray *payload, uint8_t v)
{
    g_byte_array_append(payload, &v, 1);
}

static void mfs_payload_append_u16(GByteArray *payload, uint16_t v)
{
    uint8_t tmp[2];

    mfs_put_be16(tmp, v);
    g_byte_array_append(payload, tmp, sizeof(tmp));
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

static uint8_t mfs_attr_type(const uint8_t *attr, size_t len)
{
    if (!attr || len < 3) {
        return 0;
    }

    if (attr[0] < 64) {
        return attr[1] >> 4;
    }
    return attr[0] & 0x7f;
}

static uint64_t mfs_attr_size(const uint8_t *attr, size_t len)
{
    if (!attr || len < 35) {
        return 0;
    }
    return mfs_get_be64(attr + 27);
}

static int mfs_proto_master_simple_lookup(int fd, const char *name,
                                          uint32_t parent_inode,
                                          uint64_t *inode_out,
                                          uint8_t *attr_buf,
                                          size_t *attr_len,
                                          Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t msgid = mfs_proto_next_msgid();
    uint32_t name_len = strlen(name);
    int ret;

    if (name_len == 0 || name_len > UINT8_MAX) {
        g_byte_array_unref(req);
        error_setg(errp, "mfs: invalid path component");
        return -EINVAL;
    }

    mfs_payload_append_u32(req, msgid);
    mfs_payload_append_u32(req, parent_inode);
    mfs_payload_append_u8(req, name_len);
    g_byte_array_append(req, (const uint8_t *)name, name_len);
    mfs_payload_append_u32(req, 0);
    mfs_payload_append_u32(req, 1);
    mfs_payload_append_u32(req, 0);

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_LOOKUP,
                              req->data, req->len,
                              MFS_MATOCL_FUSE_LOOKUP, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_expect_msgid(resp, msgid, errp);
    if (ret < 0) {
        g_byte_array_unref(resp);
        return ret;
    }

    if (resp->len == 5) {
        ret = mfs_set_status_error(errp, "lookup", resp->data[4]);
        g_byte_array_unref(resp);
        return ret;
    }

    if (resp->len < 8 + 35) {
        g_byte_array_unref(resp);
        error_setg(errp, "mfs: short lookup reply");
        return -EPROTO;
    }

    if (inode_out) {
        *inode_out = mfs_get_be32(resp->data + 4);
    }
    if (attr_buf && attr_len) {
        size_t copy_len = MIN((size_t)36, (size_t)resp->len - 8);

        memcpy(attr_buf, resp->data + 8, copy_len);
        *attr_len = copy_len;
    }

    g_byte_array_unref(resp);
    return 0;
}

static int mfs_proto_master_opencheck(int fd, uint32_t inode, uint8_t flags,
                                      Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t msgid = mfs_proto_next_msgid();
    int ret;

    mfs_payload_append_u32(req, msgid);
    mfs_payload_append_u32(req, inode);
    mfs_payload_append_u32(req, 0);
    mfs_payload_append_u32(req, 1);
    mfs_payload_append_u32(req, 0);
    mfs_payload_append_u8(req, flags);

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_OPEN, req->data, req->len,
                              MFS_MATOCL_FUSE_OPEN, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_expect_msgid(resp, msgid, errp);
    if (ret < 0) {
        g_byte_array_unref(resp);
        return ret;
    }

    if (resp->len == 5) {
        ret = mfs_set_status_error(errp, "open", resp->data[4]);
        g_byte_array_unref(resp);
        return ret;
    }

    if (resp->len < 4 + 35) {
        g_byte_array_unref(resp);
        error_setg(errp, "mfs: short open reply");
        return -EPROTO;
    }

    g_byte_array_unref(resp);
    return 0;
}

int mfs_proto_master_open(int fd, uint64_t inode, uint8_t flags,
                          Error **errp)
{
    return mfs_proto_master_opencheck(fd, inode, flags, errp);
}

static int mfs_proto_master_create(int fd, const char *name, uint32_t parent_inode,
                                   uint64_t *inode_out, Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t msgid = mfs_proto_next_msgid();
    uint32_t name_len = strlen(name);
    int ret;

    if (name_len == 0 || name_len > UINT8_MAX) {
        g_byte_array_unref(req);
        error_setg(errp, "mfs: invalid path component");
        return -EINVAL;
    }

    mfs_payload_append_u32(req, msgid);
    mfs_payload_append_u32(req, parent_inode);
    mfs_payload_append_u8(req, name_len);
    g_byte_array_append(req, (const uint8_t *)name, name_len);
    mfs_payload_append_u16(req, 0644);
    mfs_payload_append_u32(req, 0);
    mfs_payload_append_u32(req, 0xffffffffU);

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_CREATE, req->data, req->len,
                              MFS_MATOCL_FUSE_CREATE, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_expect_msgid(resp, msgid, errp);
    if (ret < 0) {
        g_byte_array_unref(resp);
        return ret;
    }

    if (resp->len == 5) {
        ret = mfs_set_status_error(errp, "create", resp->data[4]);
        g_byte_array_unref(resp);
        return ret;
    }

    if (resp->len == 4 + 35 || resp->len == 4 + 36) {
        *inode_out = mfs_get_be32(resp->data + 4);
    } else if (resp->len >= 5 + 35) {
        *inode_out = mfs_get_be32(resp->data + 5);
    } else {
        g_byte_array_unref(resp);
        error_setg(errp, "mfs: short create reply");
        return -EPROTO;
    }

    g_byte_array_unref(resp);
    return 0;
}

static int mfs_proto_master_truncate(int fd, uint32_t master_version,
                                     uint32_t inode, uint64_t size,
                                     Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t msgid = mfs_proto_next_msgid();
    int ret;

    mfs_payload_append_u32(req, msgid);
    mfs_payload_append_u32(req, inode);
    if (master_version >= MFS_VERSION_INT(2, 0, 0)) {
        mfs_payload_append_u8(req, 0);
    }
    mfs_payload_append_u32(req, 0);
    mfs_payload_append_u32(req, 0xffffffffU);
    mfs_payload_append_u64(req, size);

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_TRUNCATE,
                              req->data, req->len,
                              MFS_MATOCL_FUSE_TRUNCATE, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    if (resp->len == 5) {
        ret = mfs_proto_expect_simple_status(resp, msgid, "truncate", errp);
    } else {
        ret = mfs_proto_expect_msgid(resp, msgid, errp);
    }

    g_byte_array_unref(resp);
    return ret;
}

int mfs_proto_register_session(int fd, const char *client_id,
                               const char *subdir,
                               const uint8_t password_md5[16],
                               uint32_t *master_version, Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    const char *id = client_id ? client_id : "qemu-mfs";
    const char *reg_subdir = (subdir && *subdir) ? subdir : "/";
    uint32_t info_len = strlen(id) + 1;
    uint32_t subdir_len = strlen(reg_subdir) + 1;
    uint8_t digest[16];
    int ret;

    memset(digest, 0, sizeof(digest));

    if (password_md5) {
        GByteArray *rand_req = g_byte_array_new();
        GByteArray *rand_resp = NULL;
        GChecksum *sum = g_checksum_new(G_CHECKSUM_MD5);
        gsize digest_len = sizeof(digest);

        g_byte_array_append(rand_req, (const uint8_t *)mfs_register_blob_acl, 64);
        mfs_payload_append_u8(rand_req, 1);

        ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_REGISTER,
                                  rand_req->data, rand_req->len,
                                  MFS_MATOCL_FUSE_REGISTER, &rand_resp, errp);
        g_byte_array_unref(rand_req);
        if (ret < 0) {
            g_checksum_free(sum);
            g_byte_array_unref(req);
            return ret;
        }

        if (rand_resp->len != 32) {
            g_checksum_free(sum);
            g_byte_array_unref(rand_resp);
            g_byte_array_unref(req);
            error_setg(errp, "mfs: short password challenge");
            return -EPROTO;
        }

        g_checksum_update(sum, rand_resp->data, 16);
        g_checksum_update(sum, password_md5, 16);
        g_checksum_update(sum, rand_resp->data + 16, 16);
        g_checksum_get_digest(sum, digest, &digest_len);
        g_checksum_free(sum);
        g_byte_array_unref(rand_resp);
    }

    g_byte_array_append(req, (const uint8_t *)mfs_register_blob_acl, 64);
    mfs_payload_append_u8(req, MFS_REGISTER_NEWSESSION);
    mfs_payload_append_u16(req, MFS_VERSMAJ);
    mfs_payload_append_u8(req, MFS_VERSMID);
    mfs_payload_append_u8(req, MFS_VERSMIN);
    mfs_payload_append_u32(req, info_len);
    g_byte_array_append(req, (const uint8_t *)id, info_len);
    mfs_payload_append_u32(req, subdir_len);
    g_byte_array_append(req, (const uint8_t *)reg_subdir, subdir_len);
    if (password_md5) {
        g_byte_array_append(req, digest, sizeof(digest));
    }

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_REGISTER,
                              req->data, req->len,
                              MFS_MATOCL_FUSE_REGISTER, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    if (resp->len == 1) {
        ret = mfs_set_status_error(errp, "register", resp->data[0]);
        g_byte_array_unref(resp);
        return ret;
    }

    if (resp->len < 8) {
        g_byte_array_unref(resp);
        error_setg(errp, "mfs: short register reply");
        return -EPROTO;
    }

    if (master_version) {
        uint32_t v1 = mfs_get_be32(resp->data);
        uint32_t v2 = mfs_get_be32(resp->data + 4);

        *master_version = (v1 >= 0x00010000 && v1 <= 0x0fffffff) ? v1 : v2;
    }

    g_byte_array_unref(resp);
    return 0;
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

int mfs_proto_master_lookup_path(int fd, uint32_t master_version,
                                 const char *path, uint64_t *inode,
                                 uint64_t *size, Error **errp)
{
    g_auto(GStrv) parts = NULL;
    uint32_t cur = MFS_ROOT_ID;
    uint64_t current_inode = MFS_ROOT_ID;
    int i;
    int ret;

    (void)master_version;

    if (!path || path[0] != '/') {
        error_setg(errp, "mfs: path must be absolute");
        return -EINVAL;
    }

    if (strcmp(path, "/") == 0) {
        if (inode) {
            *inode = MFS_ROOT_ID;
        }
        if (size) {
            *size = 0;
        }
        return 0;
    }

    parts = g_strsplit(path, "/", -1);
    for (i = 0; parts[i]; i++) {
        uint8_t attr[36];
        size_t attr_len = 0;
        uint8_t type = 0;

        if (parts[i][0] == '\0') {
            continue;
        }

        ret = mfs_proto_master_simple_lookup(fd, parts[i], cur, &current_inode,
                                             attr, &attr_len, errp);
        if (ret < 0) {
            return ret;
        }

        cur = current_inode;
        type = mfs_attr_type(attr, attr_len);
        if (parts[i + 1] && parts[i + 1][0] != '\0' &&
            type != MFS_TYPE_DIRECTORY) {
            error_setg(errp, "mfs: '%s' is not a directory", parts[i]);
            return -ENOTDIR;
        }

        if (size) {
            *size = mfs_attr_size(attr, attr_len);
        }
    }

    if (inode) {
        *inode = current_inode;
    }
    return 0;
}

int mfs_proto_master_create_path(int fd, uint32_t master_version,
                                 const char *path, uint64_t size,
                                 uint64_t *inode, Error **errp)
{
    g_autofree char *dir = NULL;
    g_autofree char *base = NULL;
    uint64_t parent_inode = 0;
    uint64_t file_inode = 0;
    uint64_t ignored_size = 0;
    int ret;

    if (!path || path[0] != '/' || strcmp(path, "/") == 0) {
        error_setg(errp, "mfs: create path must be an absolute file path");
        return -EINVAL;
    }

    dir = g_path_get_dirname(path);
    base = g_path_get_basename(path);

    ret = mfs_proto_master_lookup_path(fd, master_version, dir,
                                       &parent_inode, &ignored_size, errp);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_master_create(fd, base, parent_inode, &file_inode, errp);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_master_opencheck(fd, file_inode, MFS_OPEN_RDWR, errp);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_master_truncate(fd, master_version, file_inode, size, errp);
    if (ret < 0) {
        return ret;
    }

    if (inode) {
        *inode = file_inode;
    }
    return 0;
}

static int mfs_decode_chunk_location(const GByteArray *resp, uint32_t msgid,
                                     MFSChunkLocation *loc, Error **errp)
{
    const uint8_t *p = resp->data;
    const uint8_t *end = resp->data + resp->len;
    size_t entry_sz;
    int ret;

    ret = mfs_proto_expect_msgid(resp, msgid, errp);
    if (ret < 0) {
        return ret;
    }

    if (resp->len == 5) {
        return mfs_set_status_error(errp, "chunk lookup", resp->data[4]);
    }

    p += 4;
    if ((size_t)(end - p) < 20) {
        error_setg(errp, "mfs: short chunk metadata reply");
        return -EPROTO;
    }

    mfs_proto_chunk_location_reset(loc);
    mfs_proto_chunk_location_init(loc);

    if ((size_t)(end - p) >= 21 && p[0] >= 1 && p[0] <= 3) {
        loc->protocol = p[0];
        p += 1;
    } else {
        loc->protocol = 0;
    }

    loc->length = mfs_get_be64(p);
    p += 8;
    loc->chunk_id = mfs_get_be64(p);
    p += 8;
    loc->version = mfs_get_be32(p);
    p += 4;

    switch (loc->protocol) {
    case 0:
        entry_sz = 6;
        break;
    case 1:
        entry_sz = 10;
        break;
    default:
        entry_sz = 14;
        break;
    }

    while ((size_t)(end - p) >= entry_sz) {
        MFSChunkReplica *rep = g_new0(MFSChunkReplica, 1);
        uint32_t ip = mfs_get_be32(p);

        rep->host = g_strdup_printf("%u.%u.%u.%u",
                                    (ip >> 24) & 0xff,
                                    (ip >> 16) & 0xff,
                                    (ip >> 8) & 0xff,
                                    ip & 0xff);
        rep->port = mfs_get_be16(p + 4);
        if (entry_sz >= 10) {
            rep->cs_ver = mfs_get_be32(p + 6);
        }
        if (entry_sz >= 14) {
            rep->labelmask = mfs_get_be32(p + 10);
        }
        g_ptr_array_add(loc->replicas, rep);
        p += entry_sz;
    }

    if (loc->replicas->len == 0 &&
        (loc->length == 0 || (loc->chunk_id == 0 && loc->version == 0))) {
        return 0;
    }

    if (loc->replicas->len == 0) {
        error_setg(errp, "mfs: chunk metadata has no replicas");
        return -EHOSTUNREACH;
    }

    return 0;
}

int mfs_proto_master_read_chunk(int fd, uint32_t master_version,
                                uint64_t inode, uint32_t chunk_index,
                                MFSChunkLocation *loc, Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t msgid = mfs_proto_next_msgid();
    int ret;

    mfs_payload_append_u32(req, msgid);
    mfs_payload_append_u32(req, inode);
    mfs_payload_append_u32(req, chunk_index);
    if (master_version >= MFS_VERSION_INT(3, 0, 4)) {
        mfs_payload_append_u8(req, 0);
    }

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_READ_CHUNK,
                              req->data, req->len,
                              MFS_MATOCL_FUSE_READ_CHUNK, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_decode_chunk_location(resp, msgid, loc, errp);
    loc->chunk_index = chunk_index;
    g_byte_array_unref(resp);
    return ret;
}

int mfs_proto_master_write_chunk(int fd, uint32_t master_version,
                                 uint64_t inode, uint32_t chunk_index,
                                 MFSChunkLocation *loc, uint64_t *write_id,
                                 Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t msgid = mfs_proto_next_msgid();
    int ret;

    if (write_id) {
        *write_id = 0;
    }

    mfs_payload_append_u32(req, msgid);
    mfs_payload_append_u32(req, inode);
    mfs_payload_append_u32(req, chunk_index);
    if (master_version >= MFS_VERSION_INT(3, 0, 4)) {
        mfs_payload_append_u8(req, MFS_CHUNKOPFLAG_CANMODTIME);
    }

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_WRITE_CHUNK,
                              req->data, req->len,
                              MFS_MATOCL_FUSE_WRITE_CHUNK, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_decode_chunk_location(resp, msgid, loc, errp);
    loc->chunk_index = chunk_index;
    g_byte_array_unref(resp);
    return ret;
}

int mfs_proto_master_write_chunk_end(int fd, uint32_t master_version,
                                     uint64_t inode,
                                     uint32_t chunk_index,
                                     uint64_t chunk_id,
                                     uint64_t file_size,
                                     uint32_t chunk_offset,
                                     uint32_t write_size,
                                     Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t msgid = mfs_proto_next_msgid();
    int ret;

    mfs_payload_append_u32(req, msgid);
    mfs_payload_append_u64(req, chunk_id);
    mfs_payload_append_u32(req, inode);
    if (master_version >= MFS_VERSION_INT(3, 0, 74)) {
        mfs_payload_append_u32(req, chunk_index);
    }
    mfs_payload_append_u64(req, file_size);
    if (master_version >= MFS_VERSION_INT(3, 0, 4)) {
        mfs_payload_append_u8(req, 0);
    }
    if (master_version >= MFS_VERSION_INT(4, 40, 0)) {
        mfs_payload_append_u32(req, chunk_offset);
        mfs_payload_append_u32(req, write_size);
    }

    ret = mfs_proto_roundtrip(fd, MFS_CLTOMA_FUSE_WRITE_CHUNK_END,
                              req->data, req->len,
                              MFS_MATOCL_FUSE_WRITE_CHUNK_END, &resp, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_proto_expect_simple_status(resp, msgid, "write_chunk_end", errp);
    g_byte_array_unref(resp);
    return ret;
}

static int mfs_cs_recv_write_status(int fd, uint64_t chunk_id, Error **errp)
{
    GByteArray *resp = NULL;
    uint32_t reply_type;
    int ret;

    for (;;) {
        ret = mfs_proto_cs_recv_packet(fd, &reply_type, &resp, errp);
        if (ret < 0) {
            return ret;
        }

        if (reply_type != MFS_CSTOCL_WRITE_STATUS) {
            g_byte_array_unref(resp);
            continue;
        }

        if (resp->len < 13) {
            g_byte_array_unref(resp);
            error_setg(errp, "mfs: short write status");
            return -EPROTO;
        }

        if (mfs_get_be64(resp->data) != chunk_id) {
            g_byte_array_unref(resp);
            error_setg(errp, "mfs: write status chunk mismatch");
            return -EPROTO;
        }

        ret = (resp->data[12] == MFS_STATUS_OK) ? 0 :
            mfs_set_status_error(errp, "chunkserver write", resp->data[12]);
        g_byte_array_unref(resp);
        return ret;
    }
}

int mfs_proto_cs_read(int fd, const MFSChunkLocation *loc,
                      uint32_t chunk_offset, uint32_t length,
                      uint8_t *out, Error **errp)
{
    GByteArray *req = g_byte_array_new();
    GByteArray *resp = NULL;
    uint32_t reply_type;
    uint32_t total = 0;
    bool proto_v1 = false;
    int ret;

    if (loc->replicas->len > 0) {
        MFSChunkReplica *rep = g_ptr_array_index(loc->replicas, 0);

        proto_v1 = rep->cs_ver >= MFS_VERSION_INT(1, 7, 32);
    }

    if (proto_v1) {
        mfs_payload_append_u8(req, 1);
    }
    mfs_payload_append_u64(req, loc->chunk_id);
    mfs_payload_append_u32(req, loc->version);
    mfs_payload_append_u32(req, chunk_offset);
    mfs_payload_append_u32(req, length);

    ret = mfs_proto_send_packet(fd, MFS_CLTOCS_READ, req->data, req->len, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    for (;;) {
        ret = mfs_proto_cs_recv_packet(fd, &reply_type, &resp, errp);
        if (ret < 0) {
            error_prepend(errp,
                          "mfs: chunkserver read chunk=%" PRIu64
                          " off=%u len=%u received=%u: ",
                          loc->chunk_id, chunk_offset, length, total);
            return ret;
        }

        if (reply_type == MFS_ANTOAN_NOP) {
            g_byte_array_unref(resp);
            continue;
        }

        if (reply_type == MFS_CSTOCL_READ_STATUS) {
            if (resp->len < 9) {
                g_byte_array_unref(resp);
                error_setg(errp, "mfs: short read status");
                return -EPROTO;
            }
            ret = (resp->data[8] == MFS_STATUS_OK) ? 0 :
                mfs_set_status_error(errp, "chunkserver read", resp->data[8]);
            g_byte_array_unref(resp);
            return ret;
        }

        if (reply_type != MFS_CSTOCL_READ_DATA) {
            g_byte_array_unref(resp);
            error_setg(errp, "mfs: unexpected chunkserver read reply type=%u",
                       reply_type);
            return -EPROTO;
        }

        if (resp->len < 20) {
            g_byte_array_unref(resp);
            error_setg(errp, "mfs: short read data packet");
            return -EPROTO;
        }

        {
            uint16_t block = mfs_get_be16(resp->data + 8);
            uint16_t block_off = mfs_get_be16(resp->data + 10);
            uint32_t data_len = mfs_get_be32(resp->data + 12);
            uint32_t remote_off = ((uint32_t)block * MFS_BLOCK_SIZE) + block_off;
            uint32_t copy_off = remote_off < chunk_offset ? chunk_offset - remote_off : 0;
            uint32_t copy_len;

            if (resp->len < 20 + data_len) {
                g_byte_array_unref(resp);
                error_setg(errp, "mfs: truncated read data packet");
                return -EPROTO;
            }

            if (copy_off >= data_len || total >= length) {
                g_byte_array_unref(resp);
                continue;
            }

            copy_len = data_len - copy_off;
            copy_len = MIN(copy_len, length - total);
            memcpy(out + total, resp->data + 20 + copy_off, copy_len);
            total += copy_len;
            g_byte_array_unref(resp);
        }
    }
}

int mfs_proto_cs_write(int fd, const MFSChunkLocation *loc,
                       uint32_t chunk_offset, const uint8_t *buf,
                       uint32_t length, Error **errp)
{
    GByteArray *req = g_byte_array_new();
    bool proto_v1 = false;
    uint32_t sent = 0;
    uint32_t write_id = 1;
    int ret;

    if (loc->replicas->len > 0) {
        MFSChunkReplica *rep = g_ptr_array_index(loc->replicas, 0);

        proto_v1 = rep->cs_ver >= MFS_VERSION_INT(1, 7, 32);
    }

    mfs_crc32_init();

    if (proto_v1) {
        mfs_payload_append_u8(req, 1);
    }
    mfs_payload_append_u64(req, loc->chunk_id);
    mfs_payload_append_u32(req, loc->version);

    ret = mfs_proto_send_packet(fd, MFS_CLTOCS_WRITE, req->data, req->len, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    ret = mfs_cs_recv_write_status(fd, loc->chunk_id, errp);
    if (ret < 0) {
        return ret;
    }

    while (sent < length) {
        GByteArray *data_req = g_byte_array_new();
        uint32_t off = chunk_offset + sent;
        uint16_t block = off / MFS_BLOCK_SIZE;
        uint16_t block_off = off % MFS_BLOCK_SIZE;
        uint32_t frag = MIN((uint32_t)(length - sent),
                            (uint32_t)(MFS_BLOCK_SIZE - block_off));
        uint32_t crc = mfs_crc32_update(0, buf + sent, frag);

        mfs_payload_append_u64(data_req, loc->chunk_id);
        mfs_payload_append_u32(data_req, write_id);
        mfs_payload_append_u16(data_req, block);
        mfs_payload_append_u16(data_req, block_off);
        mfs_payload_append_u32(data_req, frag);
        mfs_payload_append_u32(data_req, crc);
        g_byte_array_append(data_req, buf + sent, frag);

        ret = mfs_proto_send_packet(fd, MFS_CLTOCS_WRITE_DATA,
                                    data_req->data, data_req->len, errp);
        g_byte_array_unref(data_req);
        if (ret < 0) {
            return ret;
        }

        ret = mfs_cs_recv_write_status(fd, loc->chunk_id, errp);
        if (ret < 0) {
            return ret;
        }

        sent += frag;
        write_id++;
    }

    req = g_byte_array_new();
    mfs_payload_append_u64(req, loc->chunk_id);
    mfs_payload_append_u32(req, loc->version);
    ret = mfs_proto_send_packet(fd, MFS_CLTOCS_WRITE_FINISH,
                                req->data, req->len, errp);
    g_byte_array_unref(req);
    if (ret < 0) {
        return ret;
    }

    /*
     * MooseFS last-in-chain writes do not emit a trailing WRITE_STATUS on
     * successful WRITE_FINISH. The reference client sends WRITE_FINISH and
     * then reuses/closes the socket without reading another status packet.
     */
    return 0;
}
