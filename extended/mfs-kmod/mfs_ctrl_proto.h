#ifndef _MFS_CTRL_PROTO_H_
#define _MFS_CTRL_PROTO_H_

#ifdef __KERNEL__
#include <linux/types.h>
#else
#include <stdint.h>
/*
 * On some architectures (aarch64) the kernel's asm-generic/int-ll64.h
 * leaks through glibc headers and already defines __u64 as
 * 'unsigned long long', while uint64_t is 'unsigned long'.
 * Guard with __EXPORTED_HEADERS__ / type checks to avoid redefinition.
 */
#include <linux/types.h>  /* picks up __u8/__u16/__u32/__u64 portably */
#endif

#define MFS_CTRL_MAGIC 0x4d465343U /* MFSC */
#define MFS_CTRL_VERSION 1U

#define MFS_CTRL_FLAG_REQUEST 0x0001U
#define MFS_CTRL_FLAG_RESPONSE 0x0002U

#define MFS_CTRL_MAX_PAYLOAD (2U * 1024U * 1024U)

#define MFS_CTRL_OP_HELLO 1U
#define MFS_CTRL_OP_REGISTER 2U
#define MFS_CTRL_OP_LOOKUP 3U
#define MFS_CTRL_OP_GETATTR 4U
#define MFS_CTRL_OP_SETATTR 5U
#define MFS_CTRL_OP_TRUNCATE 6U
#define MFS_CTRL_OP_READDIR 7U
#define MFS_CTRL_OP_CREATE 8U
#define MFS_CTRL_OP_UNLINK 9U
#define MFS_CTRL_OP_LINK 10U
#define MFS_CTRL_OP_RENAME 11U
#define MFS_CTRL_OP_MKDIR 12U
#define MFS_CTRL_OP_RMDIR 13U
#define MFS_CTRL_OP_READLINK 14U
#define MFS_CTRL_OP_SYMLINK 15U
#define MFS_CTRL_OP_GETXATTR 16U
#define MFS_CTRL_OP_SETXATTR 17U
#define MFS_CTRL_OP_LISTXATTR 18U
#define MFS_CTRL_OP_REMOVEXATTR 19U
#define MFS_CTRL_OP_READ 20U
#define MFS_CTRL_OP_WRITE 21U
#define MFS_CTRL_OP_FSYNC 22U
#define MFS_CTRL_OP_STATFS 23U

#define MFS_SETATTR_MODE (1U << 0)
#define MFS_SETATTR_UID (1U << 1)
#define MFS_SETATTR_GID (1U << 2)
#define MFS_SETATTR_ATIME (1U << 3)
#define MFS_SETATTR_MTIME (1U << 4)
#define MFS_SETATTR_CTIME (1U << 5)
#define MFS_SETATTR_ATIME_NOW (1U << 6)
#define MFS_SETATTR_MTIME_NOW (1U << 7)

#define MFS_WIRE_TYPE_UNKNOWN 0U
#define MFS_WIRE_TYPE_FILE 1U
#define MFS_WIRE_TYPE_DIR 2U
#define MFS_WIRE_TYPE_SYMLINK 3U
#define MFS_WIRE_TYPE_FIFO 4U
#define MFS_WIRE_TYPE_BLOCK 5U
#define MFS_WIRE_TYPE_CHAR 6U
#define MFS_WIRE_TYPE_SOCK 7U

struct mfs_ctrl_hdr {
	__u32 magic;
	__u32 version;
	__u32 req_id;
	__u16 op;
	__u16 flags;
	__s32 status;
	__u32 payload_len;
} __attribute__((packed));

struct mfs_wire_attr {
	__u8 type;
	__u8 mattr;
	__u16 mode;
	__u32 uid;
	__u32 gid;
	__u64 size;
	__u32 atime;
	__u32 mtime;
	__u32 ctime;
	__u32 nlink;
	__u32 rdev;
	__u8 winattr;
	__u8 reserved[3];
} __attribute__((packed));

struct mfs_ctrl_register_req {
	__u16 master_len;
	__u16 subdir_len;
	__u16 password_len;
	__u16 reserved;
	__u16 master_port;
	__u16 flags;
	__u32 mount_uid;
	__u32 mount_gid;
	/* master, subdir, password */
} __attribute__((packed));

struct mfs_ctrl_register_rsp {
	__u32 session_id;
	__u32 root_inode;
	struct mfs_wire_attr root_attr;
} __attribute__((packed));

struct mfs_ctrl_inode_req {
	__u32 session_id;
	__u32 inode;
	__u32 uid;
	__u32 gid;
} __attribute__((packed));

struct mfs_ctrl_lookup_req {
	__u32 session_id;
	__u32 parent_inode;
	__u32 uid;
	__u32 gid;
	__u16 name_len;
	__u16 reserved;
	/* name bytes */
} __attribute__((packed));

struct mfs_ctrl_lookup_rsp {
	__u32 inode;
	struct mfs_wire_attr attr;
} __attribute__((packed));

struct mfs_ctrl_setattr_req {
	__u32 session_id;
	__u32 inode;
	__u32 uid;
	__u32 gid;
	__u32 valid;
	__u32 mode;
	__u32 attr_uid;
	__u32 attr_gid;
	__u64 atime_ns;
	__u64 mtime_ns;
	__u64 ctime_ns;
} __attribute__((packed));

struct mfs_ctrl_truncate_req {
	__u32 session_id;
	__u32 inode;
	__u32 uid;
	__u32 gid;
	__u64 size;
} __attribute__((packed));

struct mfs_ctrl_readdir_req {
	__u32 session_id;
	__u32 inode;
	__u32 uid;
	__u32 gid;
	__u64 offset;
	__u32 max_entries;
	__u32 flags;
} __attribute__((packed));

struct mfs_ctrl_readdir_rsp {
	__u64 next_offset;
	__u32 count;
	__u8 eof;
	__u8 reserved[3];
	/* entries */
} __attribute__((packed));

struct mfs_ctrl_dirent_wire {
	__u64 next_offset;
	__u32 inode;
	__u8 type;
	__u8 reserved;
	__u16 name_len;
	/* name bytes */
} __attribute__((packed));

struct mfs_ctrl_create_req {
	__u32 session_id;
	__u32 parent_inode;
	__u32 uid;
	__u32 gid;
	__u32 mode;
	__u16 name_len;
	__u16 reserved;
	/* name bytes */
} __attribute__((packed));

struct mfs_ctrl_create_rsp {
	__u32 inode;
	struct mfs_wire_attr attr;
} __attribute__((packed));

struct mfs_ctrl_unlink_req {
	__u32 session_id;
	__u32 parent_inode;
	__u32 uid;
	__u32 gid;
	__u16 name_len;
	__u16 reserved;
	/* name bytes */
} __attribute__((packed));

struct mfs_ctrl_link_req {
	__u32 session_id;
	__u32 inode;
	__u32 new_parent_inode;
	__u32 uid;
	__u32 gid;
	__u16 name_len;
	__u16 reserved;
	/* name bytes */
} __attribute__((packed));

struct mfs_ctrl_rename_req {
	__u32 session_id;
	__u32 old_parent_inode;
	__u32 new_parent_inode;
	__u32 uid;
	__u32 gid;
	__u32 flags;
	__u16 old_name_len;
	__u16 new_name_len;
	/* old name + new name */
} __attribute__((packed));

struct mfs_ctrl_mkdir_req {
	__u32 session_id;
	__u32 parent_inode;
	__u32 uid;
	__u32 gid;
	__u32 mode;
	__u16 name_len;
	__u16 reserved;
	/* name bytes */
} __attribute__((packed));

struct mfs_ctrl_rmdir_req {
	__u32 session_id;
	__u32 parent_inode;
	__u32 uid;
	__u32 gid;
	__u16 name_len;
	__u16 reserved;
	/* name bytes */
} __attribute__((packed));

struct mfs_ctrl_symlink_req {
	__u32 session_id;
	__u32 parent_inode;
	__u32 uid;
	__u32 gid;
	__u16 name_len;
	__u16 target_len;
	/* name + target */
} __attribute__((packed));

struct mfs_ctrl_readlink_rsp {
	__u32 size;
	/* link data */
} __attribute__((packed));

struct mfs_ctrl_xattr_req {
	__u32 session_id;
	__u32 inode;
	__u32 uid;
	__u32 gid;
	__u32 flags;
	__u16 name_len;
	__u16 reserved;
	__u32 value_len;
	/* name + value */
} __attribute__((packed));

struct mfs_ctrl_xattr_rsp {
	__u32 size;
	/* value bytes */
} __attribute__((packed));

struct mfs_ctrl_read_req {
	__u32 session_id;
	__u32 inode;
	__u64 offset;
	__u32 size;
	__u32 flags;
} __attribute__((packed));

struct mfs_ctrl_read_rsp {
	__u32 size;
	/* payload */
} __attribute__((packed));

struct mfs_ctrl_write_req {
	__u32 session_id;
	__u32 inode;
	__u64 offset;
	__u32 size;
	__u32 flags;
	/* payload */
} __attribute__((packed));

struct mfs_ctrl_write_rsp {
	__u32 written;
	__u8 attr_valid;
	__u8 reserved[3];
	struct mfs_wire_attr attr;
} __attribute__((packed));

struct mfs_ctrl_fsync_req {
	__u32 session_id;
	__u32 inode;
	__u32 datasync;
	__u32 reserved;
} __attribute__((packed));

struct mfs_ctrl_statfs_req {
	__u32 session_id;
	__u32 reserved;
} __attribute__((packed));

struct mfs_ctrl_statfs_rsp {
	__u64 total_space;
	__u64 avail_space;
	__u64 free_space;
	__u64 trash_space;
	__u64 sustained_space;
	__u32 inodes;
	__u32 reserved;
} __attribute__((packed));

#endif
