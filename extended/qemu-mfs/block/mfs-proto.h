#ifndef QEMU_MFS_PROTO_H
#define QEMU_MFS_PROTO_H

#include "qemu/osdep.h"
#include "qemu/units.h"
#include "qapi/error.h"

#define MFS_PACKET_HEADER_LEN 8
#define MFS_CHUNK_SIZE (64ULL * MiB)
#define MFS_BLOCK_SIZE (64ULL * KiB)
#define MFS_ROOT_ID 1

#define MFS_VERSION_INT(maj, mid, min) \
    ((((uint32_t)(maj)) << 16) | (((uint32_t)(mid)) << 8) | ((uint32_t)(min)))

#define MFS_CLTOMA_FUSE_REGISTER        400
#define MFS_MATOCL_FUSE_REGISTER        401
#define MFS_CLTOMA_FUSE_LOOKUP          406
#define MFS_MATOCL_FUSE_LOOKUP          407
#define MFS_CLTOMA_FUSE_GETATTR         408
#define MFS_MATOCL_FUSE_GETATTR         409
#define MFS_CLTOMA_FUSE_OPEN            430
#define MFS_MATOCL_FUSE_OPEN            431
#define MFS_CLTOMA_FUSE_READ_CHUNK      432
#define MFS_MATOCL_FUSE_READ_CHUNK      433
#define MFS_CLTOMA_FUSE_WRITE_CHUNK     434
#define MFS_MATOCL_FUSE_WRITE_CHUNK     435
#define MFS_CLTOMA_FUSE_WRITE_CHUNK_END 436
#define MFS_MATOCL_FUSE_WRITE_CHUNK_END 437
#define MFS_CLTOMA_FUSE_TRUNCATE        464
#define MFS_MATOCL_FUSE_TRUNCATE        465
#define MFS_CLTOMA_FUSE_CREATE          482
#define MFS_MATOCL_FUSE_CREATE          483

#define MFS_CLTOCS_READ   200
#define MFS_CSTOCL_READ_STATUS 201
#define MFS_CSTOCL_READ_DATA   202
#define MFS_CLTOCS_WRITE  210
#define MFS_CSTOCL_WRITE_STATUS 211
#define MFS_CLTOCS_WRITE_DATA 212
#define MFS_CLTOCS_WRITE_FINISH 213

#define MFS_STATUS_OK 0
#define MFS_ERROR_ENOENT 3
#define MFS_ERROR_ENOTSUP 39
#define MFS_ERROR_NOCHUNKSERVERS 12

#define MFS_REGISTER_NEWSESSION 2

#define MFS_TYPE_FILE 1
#define MFS_TYPE_DIRECTORY 2

#define MFS_CHUNKOPFLAG_CANMODTIME 0x01
#define MFS_OPEN_READONLY 0
#define MFS_OPEN_WRITEONLY 1
#define MFS_OPEN_RDWR 2

typedef struct MFSChunkReplica {
    char *host;
    uint16_t port;
    uint32_t cs_ver;
    uint32_t labelmask;
} MFSChunkReplica;

typedef struct MFSChunkLocation {
    uint8_t protocol;
    uint64_t length;
    uint64_t chunk_id;
    uint32_t version;
    uint32_t chunk_index;
    GPtrArray *replicas; /* MFSChunkReplica* */
} MFSChunkLocation;

void mfs_proto_chunk_location_init(MFSChunkLocation *loc);
void mfs_proto_chunk_location_reset(MFSChunkLocation *loc);

int mfs_proto_send_packet(int fd, uint32_t type, const uint8_t *data,
                          uint32_t len, Error **errp);
int mfs_proto_recv_packet(int fd, uint32_t *type, GByteArray **payload,
                          Error **errp);

int mfs_proto_register_session(int fd, const char *client_id,
                               const uint8_t password_md5[16],
                               uint32_t *master_version, Error **errp);
int mfs_proto_master_lookup_path(int fd, uint32_t master_version,
                                 const char *path, uint64_t *inode,
                                 uint64_t *size, Error **errp);
int mfs_proto_master_create_path(int fd, uint32_t master_version,
                                 const char *path, uint64_t size,
                                 uint64_t *inode, Error **errp);
int mfs_proto_master_open(int fd, uint64_t inode, uint8_t flags,
                          Error **errp);

int mfs_proto_master_read_chunk(int fd, uint32_t master_version,
                                uint64_t inode, uint32_t chunk_index,
                                MFSChunkLocation *loc, Error **errp);
int mfs_proto_master_write_chunk(int fd, uint32_t master_version,
                                 uint64_t inode, uint32_t chunk_index,
                                 MFSChunkLocation *loc, uint64_t *write_id,
                                 Error **errp);
int mfs_proto_master_write_chunk_end(int fd, uint32_t master_version,
                                     uint64_t inode,
                                     uint32_t chunk_index,
                                     uint64_t chunk_id,
                                     uint64_t file_size,
                                     uint32_t chunk_offset,
                                     uint32_t write_size,
                                     Error **errp);

int mfs_proto_cs_read(int fd, const MFSChunkLocation *loc,
                      uint32_t chunk_offset, uint32_t length,
                      uint8_t *out, Error **errp);
int mfs_proto_cs_write(int fd, const MFSChunkLocation *loc,
                       uint32_t chunk_offset, const uint8_t *buf,
                       uint32_t length, Error **errp);

uint64_t mfs_proto_path_fallback_inode(const char *path);

#endif
