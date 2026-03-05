#ifndef QEMU_MFS_PROTO_H
#define QEMU_MFS_PROTO_H

#include "qemu/osdep.h"
#include "qemu/units.h"
#include "qapi/error.h"

#define MFS_PACKET_HEADER_LEN 8
#define MFS_CHUNK_SIZE (64ULL * MiB)
#define MFS_BLOCK_SIZE (64ULL * KiB)
#define MFS_REGISTER_TOOLS 1

#define MFS_CLTOMA_FUSE_REGISTER        400
#define MFS_CLTOMA_FUSE_LOOKUP_PATH     408
#define MFS_CLTOMA_FUSE_CREATE_PATH     410
#define MFS_CLTOMA_FUSE_READ_CHUNK      432
#define MFS_CLTOMA_FUSE_WRITE_CHUNK     434
#define MFS_CLTOMA_FUSE_WRITE_CHUNK_END 436

#define MFS_CLTOCS_READ   200
#define MFS_CLTOCS_WRITE  210

#define MFS_STATUS_OK 0

typedef struct MFSChunkReplica {
    char *host;
    uint16_t port;
} MFSChunkReplica;

typedef struct MFSChunkLocation {
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

int mfs_proto_register_tools(int fd, const char *client_id, Error **errp);
int mfs_proto_master_lookup_path(int fd, const char *path, uint64_t *inode,
                                 uint64_t *size, Error **errp);
int mfs_proto_master_create_path(int fd, const char *path, uint64_t size,
                                 uint64_t *inode, Error **errp);

int mfs_proto_master_read_chunk(int fd, uint64_t inode, uint32_t chunk_index,
                                MFSChunkLocation *loc, Error **errp);
int mfs_proto_master_write_chunk(int fd, uint64_t inode, uint32_t chunk_index,
                                 MFSChunkLocation *loc, uint64_t *write_id,
                                 Error **errp);
int mfs_proto_master_write_chunk_end(int fd, uint64_t inode,
                                     uint32_t chunk_index,
                                     uint64_t chunk_id,
                                     uint32_t version,
                                     uint64_t write_id,
                                     uint8_t status,
                                     Error **errp);

int mfs_proto_cs_read(int fd, const MFSChunkLocation *loc,
                      uint32_t chunk_offset, uint32_t length,
                      uint8_t *out, Error **errp);
int mfs_proto_cs_write(int fd, const MFSChunkLocation *loc,
                       uint32_t chunk_offset, const uint8_t *buf,
                       uint32_t length, Error **errp);

uint64_t mfs_proto_path_fallback_inode(const char *path);

#endif
