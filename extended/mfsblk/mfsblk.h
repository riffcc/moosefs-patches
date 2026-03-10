#ifndef _MFSBLK_H_
#define _MFSBLK_H_

#include <linux/atomic.h>
#include <linux/blk-mq.h>
#include <linux/blkdev.h>
#include <linux/completion.h>
#include <linux/device.h>
#include <linux/hashtable.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mutex.h>
#include <linux/net.h>
#include <linux/spinlock.h>
#include <linux/sysfs.h>   /* sysfs bus interface for device control */
#include <linux/types.h>
#include <linux/workqueue.h>

#define MFSBLK_DRV_NAME "mfsblk"
#define MFSBLK_DISK_PREFIX "mfsblk"
#define MFSBLK_DEFAULT_MASTER_PORT 9421

#define MFSBLK_PATH_MAX 512
#define MFSBLK_HOST_MAX 64
#define MFSBLK_PASSWORD_MAX 128
#define MFSBLK_MAX_SERVERS 100
#define MFSBLK_MAX_PACKET (8 + 1024 * 1024)

#define MFSBLK_CHUNK_SIZE 0x04000000U
#define MFSBLK_BLOCK_SIZE 0x00010000U
#define MFSBLK_BLOCKS_PER_CHUNK 1024U

#define MFSBLK_CACHE_BITS 8
#define MFSBLK_CACHE_TTL (5 * HZ)

#define MFSBLK_SECTOR_SHIFT 9
#define MFSBLK_SECTOR_SIZE (1U << MFSBLK_SECTOR_SHIFT)

#define MFSBLK_PROTO_BASE 0
#define MFSBLK_REGISTER_NEWSESSION 2
#define MFSBLK_CLTOMA_FUSE_REGISTER (MFSBLK_PROTO_BASE + 400)
#define MFSBLK_MATOCL_FUSE_REGISTER (MFSBLK_PROTO_BASE + 401)
#define MFSBLK_CLTOMA_FUSE_LOOKUP (MFSBLK_PROTO_BASE + 406)
#define MFSBLK_MATOCL_FUSE_LOOKUP (MFSBLK_PROTO_BASE + 407)
#define MFSBLK_CLTOMA_PATH_LOOKUP (MFSBLK_PROTO_BASE + 390)
#define MFSBLK_MATOCL_PATH_LOOKUP (MFSBLK_PROTO_BASE + 391)
#define MFSBLK_CLTOCS_READ (MFSBLK_PROTO_BASE + 200)
#define MFSBLK_CSTOCL_READ_STATUS (MFSBLK_PROTO_BASE + 201)
#define MFSBLK_CSTOCL_READ_DATA (MFSBLK_PROTO_BASE + 202)
#define MFSBLK_CLTOCS_WRITE (MFSBLK_PROTO_BASE + 210)
#define MFSBLK_CSTOCL_WRITE_STATUS (MFSBLK_PROTO_BASE + 211)
#define MFSBLK_CLTOCS_WRITE_DATA (MFSBLK_PROTO_BASE + 212)
#define MFSBLK_CLTOCS_WRITE_FINISH (MFSBLK_PROTO_BASE + 213)

#define MFSBLK_CLTOMA_FUSE_READ_CHUNK (MFSBLK_PROTO_BASE + 432)
#define MFSBLK_MATOCL_FUSE_READ_CHUNK (MFSBLK_PROTO_BASE + 433)
#define MFSBLK_CLTOMA_FUSE_WRITE_CHUNK (MFSBLK_PROTO_BASE + 434)
#define MFSBLK_MATOCL_FUSE_WRITE_CHUNK (MFSBLK_PROTO_BASE + 435)
#define MFSBLK_CLTOMA_FUSE_WRITE_CHUNK_END (MFSBLK_PROTO_BASE + 436)
#define MFSBLK_MATOCL_FUSE_WRITE_CHUNK_END (MFSBLK_PROTO_BASE + 437)
#define MFSBLK_CLTOMA_FUSE_TRUNCATE (MFSBLK_PROTO_BASE + 464)
#define MFSBLK_MATOCL_FUSE_TRUNCATE (MFSBLK_PROTO_BASE + 465)
#define MFSBLK_CLTOMA_FUSE_CREATE (MFSBLK_PROTO_BASE + 482)
#define MFSBLK_MATOCL_FUSE_CREATE (MFSBLK_PROTO_BASE + 483)

#define MFSBLK_CHUNKOPFLAG_CANMODTIME 1

struct mfsblk_map_spec {
	int id;
	char master_host[MFSBLK_HOST_MAX];
	u32 master_ip;
	u16 master_port;
	char password[MFSBLK_PASSWORD_MAX];
	char image_path[MFSBLK_PATH_MAX];
	u64 size_bytes;
	u32 inode;
	bool inode_explicit;
	bool size_explicit;
};

struct mfsblk_server {
	u32 ip;
	u16 port;
	u32 cs_ver;
	u32 label_mask;
};

struct mfsblk_chunk_desc {
	u64 chunk_index;
	u64 file_length;
	u64 chunk_id;
	u64 write_id;
	u32 version;
	u8 csdata_ver;
	u8 split_parts;
	u8 server_count;
	struct mfsblk_server servers[MFSBLK_MAX_SERVERS];
};

struct mfsblk_chunk_cache_entry {
	struct hlist_node node;
	u64 chunk_index;
	unsigned long expires;
	struct mfsblk_chunk_desc desc;
};

struct mfsblk_cs_conn {
	struct list_head link;
	u32 ip;
	u16 port;
	struct socket *sock;
	struct mutex io_lock;
	unsigned long last_used;
};

struct mfsblk_stats {
	atomic64_t read_reqs;
	atomic64_t write_reqs;
	atomic64_t trim_reqs;
	atomic64_t read_bytes;
	atomic64_t write_bytes;
	atomic64_t errors;
	atomic_t inflight;
};

struct mfsblk_dev {
	struct list_head list;
	int id;
	char name[DISK_NAME_LEN];

	char master_host[MFSBLK_HOST_MAX];
	u32 master_ip;
	u16 master_port;
	char password[MFSBLK_PASSWORD_MAX];
	char image_path[MFSBLK_PATH_MAX];
	u64 size_bytes;
	u32 inode;

	struct mutex master_lock;
	struct socket *master_sock;
	atomic_t next_msgid;
	bool master_registered;
	u32 master_version;
	u32 master_session_id;

	struct mutex cache_lock;
	struct hlist_head chunk_cache[1 << MFSBLK_CACHE_BITS];

	struct mutex conn_lock;
	struct list_head conn_pool;
	struct mutex write_lock;
	bool write_active;
	struct mfsblk_chunk_desc write_chunk;
	u64 write_file_size;
	u32 write_min_chunk_off;
	u32 write_max_chunk_end;

	spinlock_t state_lock;
	bool removing;

	struct blk_mq_tag_set tag_set;
	struct request_queue *queue;
	struct gendisk *disk;
	struct workqueue_struct *io_wq;

	struct device ctrl_dev;
	bool ctrl_dev_added;
	struct completion ctrl_dev_released;

	struct mfsblk_stats stats;
};

struct mfsblk_cs_read_data {
	u64 chunk_id;
	u16 block_num;
	u16 block_offset;
	u32 size;
	u32 crc;
	const u8 *data;
};

struct mfsblk_cs_write_status {
	u64 chunk_id;
	u32 write_id;
	u8 status;
};

extern struct bus_type mfsblk_bus_type;
extern const struct blk_mq_ops mfsblk_mq_ops;

/* mfsblk_dev.c */
int mfsblk_dev_module_init(void);
void mfsblk_dev_module_exit(void);
int mfsblk_dev_create(const struct mfsblk_map_spec *spec, struct mfsblk_dev **out);
void mfsblk_dev_destroy(struct mfsblk_dev *dev);
int mfsblk_dev_resize(struct mfsblk_dev *dev);
int mfsblk_dev_set_size(struct mfsblk_dev *dev, u64 size_bytes);

/* mfsblk_io.c */
int mfsblk_io_rw(struct mfsblk_dev *dev, bool write, u64 offset, void *buffer,
		u32 len);
int mfsblk_io_discard(struct mfsblk_dev *dev, u64 offset, u64 len);

/* mfsblk_conn.c */
int mfsblk_conn_master_fetch_chunk(struct mfsblk_dev *dev, u64 chunk_index,
				   bool write, struct mfsblk_chunk_desc *out);
int mfsblk_conn_resolve_image(struct mfsblk_dev *dev, bool inode_explicit,
			      bool size_explicit);
int mfsblk_conn_set_file_size(struct mfsblk_dev *dev, u64 size_bytes,
			      u64 *actual_size);
int mfsblk_conn_chunk_read(struct mfsblk_dev *dev,
			   const struct mfsblk_chunk_desc *chunk, u32 chunk_offset,
			   void *dst, u32 len);
int mfsblk_conn_get_write_chunk(struct mfsblk_dev *dev, u64 chunk_index,
				struct mfsblk_chunk_desc *out);
int mfsblk_conn_chunk_write(struct mfsblk_dev *dev,
			    const struct mfsblk_chunk_desc *chunk, u32 chunk_offset,
			    const void *src, u32 len);
void mfsblk_conn_note_written(struct mfsblk_dev *dev, u64 chunk_index,
			      u32 chunk_offset, u32 len);
int mfsblk_conn_flush_writes(struct mfsblk_dev *dev);
int mfsblk_conn_trim(struct mfsblk_dev *dev, u64 offset, u64 len);
void mfsblk_conn_close_master(struct mfsblk_dev *dev);

/* mfsblk_cache.c */
void mfsblk_cache_init(struct mfsblk_dev *dev);
void mfsblk_cache_cleanup(struct mfsblk_dev *dev);
int mfsblk_cache_get_chunk(struct mfsblk_dev *dev, u64 chunk_index, bool write,
			 struct mfsblk_chunk_desc *out);
void mfsblk_cache_invalidate_chunk(struct mfsblk_dev *dev, u64 chunk_index);
void mfsblk_cache_invalidate_all(struct mfsblk_dev *dev);
int mfsblk_cache_get_conn(struct mfsblk_dev *dev, u32 ip, u16 port,
			 struct mfsblk_cs_conn **out);
void mfsblk_cache_put_conn(struct mfsblk_cs_conn *conn);

/* mfsblk_proto.c */
void mfsblk_proto_build_header(u8 *hdr, u32 type, u32 len);
void mfsblk_proto_parse_header(const u8 *hdr, u32 *type, u32 *len);
int mfsblk_proto_status_to_errno(u8 status);
u32 mfsblk_proto_path_fallback_inode(const char *path);
int mfsblk_proto_build_register_req(u8 *packet, size_t packet_sz,
				    const char *subdir, const u8 *passdigest);
int mfsblk_proto_build_register_getrandom_req(u8 *packet, size_t packet_sz);
u8 mfsblk_proto_attr_type(const u8 *attr, size_t attr_len);
u64 mfsblk_proto_attr_size(const u8 *attr, size_t attr_len);
int mfsblk_proto_parse_register_rsp(const u8 *payload, size_t payload_sz,
				    u32 *master_version, u32 *session_id);
int mfsblk_proto_build_lookup_path_req(u8 *packet, size_t packet_sz, u32 msgid,
				       const char *path);
int mfsblk_proto_build_simple_lookup_req(u8 *packet, size_t packet_sz,
					 u32 msgid, u32 parent_inode,
					 const char *name);
int mfsblk_proto_parse_lookup_path_rsp(const u8 *payload, size_t payload_sz,
				       u32 expected_msgid, u32 *parent_inode,
				       u32 *inode, char *name, size_t name_sz,
				       size_t *name_len, u8 *attr,
				       size_t attr_sz);
int mfsblk_proto_parse_simple_lookup_rsp(const u8 *payload, size_t payload_sz,
					 u32 expected_msgid, u32 *inode,
					 u8 *attr, size_t attr_sz);
int mfsblk_proto_build_create_path_req(u8 *packet, size_t packet_sz, u32 msgid,
				       u32 parent_inode, const char *name);
int mfsblk_proto_parse_create_path_rsp(const u8 *payload, size_t payload_sz,
				       u32 expected_msgid, u32 *inode, u8 *attr,
				       size_t attr_sz);
int mfsblk_proto_build_truncate_req(u8 *packet, size_t packet_sz, u32 msgid,
				    u32 inode, u64 length);
int mfsblk_proto_parse_truncate_rsp(const u8 *payload, size_t payload_sz,
				    u32 expected_msgid, u64 *length);
int mfsblk_proto_build_master_chunk_req(u8 *payload, size_t payload_sz, u32 msgid,
				       u32 inode, u32 chunk_index, bool write);
int mfsblk_proto_parse_master_chunk_rsp(const u8 *payload, size_t payload_sz,
				       u32 expected_msgid, bool write,
				       struct mfsblk_chunk_desc *out);
int mfsblk_proto_build_master_write_end_req(u8 *packet, size_t packet_sz,
					    u32 master_version, u32 msgid,
					    u64 chunk_id, u32 inode,
					    u32 chunk_index,
					    u64 length, u32 offset, u32 size);
int mfsblk_proto_parse_master_write_end_rsp(const u8 *payload, size_t payload_sz,
					    u32 expected_msgid);
int mfsblk_proto_build_cs_read_req(u8 *packet, size_t packet_sz, u64 chunk_id,
				  u32 version, u32 offset, u32 len);
int mfsblk_proto_parse_cs_read_data(const u8 *payload, size_t payload_sz,
				   struct mfsblk_cs_read_data *out);
int mfsblk_proto_parse_cs_write_status(const u8 *payload, size_t payload_sz,
				      struct mfsblk_cs_write_status *out);
int mfsblk_proto_build_cs_write_init(u8 *packet, size_t packet_sz, u64 chunk_id,
				    u32 version,
				    const struct mfsblk_server *servers,
				    u8 server_count);
int mfsblk_proto_build_cs_write_data(u8 *packet, size_t packet_sz, u64 chunk_id,
				    u32 write_id, u16 block_num,
				    u16 block_offset, const void *src,
				    u32 len, u32 crc);
int mfsblk_proto_build_cs_write_finish(u8 *packet, size_t packet_sz,
				      u64 chunk_id, u32 version);

#endif
