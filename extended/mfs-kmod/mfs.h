#ifndef _MFS_KMOD_H_
#define _MFS_KMOD_H_

#include <linux/module.h>
#include <linux/fs.h>
#include <linux/statfs.h>
#include <linux/pagemap.h>
#include <linux/mm.h>
#include <linux/slab.h>
#include <linux/uio.h>
#include <linux/time64.h>
#include <linux/kref.h>
#include <linux/xattr.h>
#include <linux/parser.h>
#include <linux/mutex.h>
#include <linux/wait.h>
#include <linux/kfifo.h>
#include <linux/list.h>
#include <linux/hashtable.h>
#include <linux/jiffies.h>
#include <linux/namei.h>
#include <linux/mount.h>
#include <linux/version.h>
#include <linux/iversion.h>

#include "mfs_ctrl_proto.h"

/*
 * Kernel 6.7+ replaced direct inode->i_mtime / i_ctime / i_atime access
 * with accessor functions.  Provide compat macros.
 */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 7, 0)
#define mfs_update_dir_times(inode) do {			\
	inode_set_mtime_to_ts(inode, inode_set_ctime_current(inode)); \
} while (0)
#define mfs_set_inode_atime(inode, sec) do {			\
	inode_set_atime(inode, sec, 0);				\
} while (0)
#define mfs_set_inode_mtime(inode, sec) do {			\
	inode_set_mtime(inode, sec, 0);				\
} while (0)
#define mfs_set_inode_ctime(inode, sec) do {			\
	inode_set_ctime(inode, sec, 0);				\
} while (0)
#else
#define mfs_update_dir_times(inode) do {			\
	(inode)->i_mtime = (inode)->i_ctime = current_time(inode); \
} while (0)
#define mfs_set_inode_atime(inode, sec) do {			\
	(inode)->i_atime.tv_sec = (sec);			\
	(inode)->i_atime.tv_nsec = 0;				\
} while (0)
#define mfs_set_inode_mtime(inode, sec) do {			\
	(inode)->i_mtime.tv_sec = (sec);			\
	(inode)->i_mtime.tv_nsec = 0;				\
} while (0)
#define mfs_set_inode_ctime(inode, sec) do {			\
	(inode)->i_ctime.tv_sec = (sec);			\
	(inode)->i_ctime.tv_nsec = 0;				\
} while (0)
#endif

#define MFS_SUPER_MAGIC 0x4d46534dU

#define MFS_CHUNK_SIZE 0x04000000ULL
#define MFS_BLOCK_SIZE 0x00010000U
#define MFS_NAME_MAX 255U
#define MFS_SYMLINK_MAX 4096U
#define MFS_PATH_MAX 1024U

#define MFS_DEFAULT_MASTER_PORT 9421
#define MFS_DEFAULT_MASTER_HOST "127.0.0.1"
#define MFS_DEFAULT_SUBDIR "/"

#define MFS_HELPER_TIMEOUT (30 * HZ)
#define MFS_CTRL_DEV_NAME "mfs_ctrl"

#define MFS_CHUNK_CACHE_BITS 6
#define MFS_CHUNK_CACHE_BUCKETS (1U << MFS_CHUNK_CACHE_BITS)
#define MFS_CHUNK_CACHE_TTL (5 * HZ)

/* MooseFS status values from MFSCommunication.h */
#define MFS_STATUS_OK 0
#define MFS_ERROR_EPERM 1
#define MFS_ERROR_ENOTDIR 2
#define MFS_ERROR_ENOENT 3
#define MFS_ERROR_EACCES 4
#define MFS_ERROR_EEXIST 5
#define MFS_ERROR_EINVAL 6
#define MFS_ERROR_ENOTEMPTY 7
#define MFS_ERROR_CHUNKLOST 8
#define MFS_ERROR_OUTOFMEMORY 9
#define MFS_ERROR_INDEXTOOBIG 10
#define MFS_ERROR_NOCHUNKSERVERS 12
#define MFS_ERROR_IO 22
#define MFS_ERROR_EROFS 33
#define MFS_ERROR_QUOTA 34
#define MFS_ERROR_ENOATTR 38
#define MFS_ERROR_ENOTSUP 39
#define MFS_ERROR_ERANGE 40
#define MFS_ERROR_CSNOTPRESENT 43
#define MFS_ERROR_EAGAIN 45
#define MFS_ERROR_EINTR 46
#define MFS_ERROR_ECANCELED 47
#define MFS_ERROR_ENAMETOOLONG 58
#define MFS_ERROR_EMLINK 59
#define MFS_ERROR_ETIMEDOUT 60
#define MFS_ERROR_EBADF 61
#define MFS_ERROR_EFBIG 62
#define MFS_ERROR_EISDIR 63

struct mfs_chunk_location {
	u32 ip;
	u16 port;
};

struct mfs_chunk_cache_entry {
	u32 inode;
	u32 chunk_index;
	u64 chunk_id;
	u32 chunk_version;
	u32 loc_count;
	struct mfs_chunk_location locs[8];
	unsigned long expires;
	struct hlist_node hnode;
};

struct mfs_chunk_cache {
	spinlock_t lock;
	struct hlist_head buckets[MFS_CHUNK_CACHE_BUCKETS];
};

struct mfs_sb_info {
	char master_host[256];
	char subdir[MFS_PATH_MAX + 1];
	char password[128];
	u16 master_port;
	u32 session_id;
	kuid_t mount_uid;
	kgid_t mount_gid;
	struct mfs_chunk_cache chunk_cache;
};

struct mfs_inode_info {
	struct inode vfs_inode;
	spinlock_t lock;
	u64 known_size;
	u8 type;
	u8 attr_valid;
};

static inline struct mfs_sb_info *MFS_SB(struct super_block *sb)
{
	return sb->s_fs_info;
}

static inline struct mfs_inode_info *MFS_I(struct inode *inode)
{
	return container_of(inode, struct mfs_inode_info, vfs_inode);
}

/* mfs_proto.c */
int mfs_moosefs_error_to_errno(s32 status);
int mfs_wire_type_to_dtype(u8 wire_type);
umode_t mfs_wire_type_to_mode(u8 wire_type);
int mfs_wire_attr_to_kstat(u32 ino, const struct mfs_wire_attr *wa, struct kstat *st);
void mfs_wire_attr_to_inode(struct inode *inode, const struct mfs_wire_attr *wa);
void mfs_inode_to_wire_attr(const struct inode *inode, struct mfs_wire_attr *wa);

/* mfs_helper_comm.c */
int mfs_helper_comm_init(void);
void mfs_helper_comm_exit(void);
int mfs_helper_call(u16 op, const void *req, u32 req_len,
		    void **rsp, u32 *rsp_len, s32 *status,
		    unsigned long timeout);
bool mfs_helper_is_online(void);

/* mfs_cache.c */
void mfs_cache_init(struct mfs_chunk_cache *cache);
void mfs_cache_destroy(struct mfs_chunk_cache *cache);
void mfs_cache_purge_inode(struct mfs_chunk_cache *cache, u32 inode);
void mfs_cache_store(struct mfs_chunk_cache *cache, u32 inode, u32 chunk_index,
		     u64 chunk_id, u32 chunk_version,
		     const struct mfs_chunk_location *locs, u32 loc_count);
bool mfs_cache_lookup(struct mfs_chunk_cache *cache, u32 inode, u32 chunk_index,
		      u64 *chunk_id, u32 *chunk_version,
		      struct mfs_chunk_location *locs, u32 *loc_count);

/* mfs_inode.c */
int mfs_inode_cache_init(void);
void mfs_inode_cache_destroy(void);
struct inode *mfs_iget(struct super_block *sb, u32 ino);
struct inode *mfs_iget_with_attr(struct super_block *sb, u32 ino,
				 const struct mfs_wire_attr *attr);
int mfs_refresh_inode(struct inode *inode, kuid_t uid, kgid_t gid);
int mfs_do_getattr(struct inode *inode, struct mfs_wire_attr *attr,
		   kuid_t uid, kgid_t gid);
struct inode *mfs_alloc_inode(struct super_block *sb);
void mfs_free_inode(struct inode *inode);
void mfs_evict_inode(struct inode *inode);
int mfs_inode_getattr(struct mnt_idmap *idmap, const struct path *path,
		      struct kstat *stat, u32 request_mask,
		      unsigned int query_flags);
int mfs_inode_setattr(struct mnt_idmap *idmap, struct dentry *dentry,
		      struct iattr *iattr);

extern const struct inode_operations mfs_dir_inode_ops;
extern const struct inode_operations mfs_file_inode_ops;
extern const struct inode_operations mfs_symlink_inode_ops;
extern const struct inode_operations mfs_special_inode_ops;

/* mfs_dir.c */
extern const struct file_operations mfs_dir_ops;
int mfs_mkdir_op(struct mnt_idmap *idmap, struct inode *dir,
		 struct dentry *dentry, umode_t mode);
int mfs_rmdir_op(struct inode *dir, struct dentry *dentry);

/* mfs_file.c */
extern const struct file_operations mfs_file_ops;
extern const struct address_space_operations mfs_aops;

/* mfs_symlink.c */
int mfs_symlink_op(struct mnt_idmap *idmap, struct inode *dir,
		   struct dentry *dentry, const char *symname);

/* mfs_xattr.c */
ssize_t mfs_listxattr(struct dentry *dentry, char *buffer, size_t size);
extern const struct xattr_handler * const mfs_xattr_handlers[];

/* mfs_super.c */
extern struct file_system_type mfs_fs_type;

#endif
