#ifndef _MFS_PROTO_H_
#define _MFS_PROTO_H_

#include <linux/byteorder/generic.h>
#include <linux/errno.h>
#include <linux/kernel.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/types.h>

/*
 * MooseFS wire protocol (network byte order):
 * type:32 length:32 data:length
 */
struct mfs_packet_header {
	__be32 type;
	__be32 length;
} __packed;

#ifndef PROTO_BASE
#define PROTO_BASE 0
#endif

#define MFSBLOCKSINCHUNK 0x400U
#define MFSCHUNKSIZE 0x04000000U
#define MFSBLOCKSIZE 0x10000U
#define MFSCRCEMPTY 0xD7978EEBU

#define MFS_NAME_MAX 255U
#define MFS_SYMLINK_MAX 4096U
#define MFS_PATH_MAX 1024U
#define MFS_ROOT_ID 1U

#define MFS_MAX_PACKETSIZE 50000000U

/* CLTOCS / CSTOCL chunkserver protocol */
#define CLTOCS_READ (PROTO_BASE + 200)
#define CSTOCL_READ_STATUS (PROTO_BASE + 201)
#define CSTOCL_READ_DATA (PROTO_BASE + 202)
#define CLTOCS_WRITE (PROTO_BASE + 210)
#define CSTOCL_WRITE_STATUS (PROTO_BASE + 211)
#define CLTOCS_WRITE_DATA (PROTO_BASE + 212)
#define CLTOCS_WRITE_FINISH (PROTO_BASE + 213)

/* CLTOMA_FUSE_* metadata opcodes */
#define CLTOMA_FUSE_REGISTER (PROTO_BASE + 400)
#define CLTOMA_FUSE_STATFS (PROTO_BASE + 402)
#define CLTOMA_FUSE_ACCESS (PROTO_BASE + 404)
#define CLTOMA_FUSE_LOOKUP (PROTO_BASE + 406)
#define CLTOMA_FUSE_GETATTR (PROTO_BASE + 408)
#define CLTOMA_FUSE_SETATTR (PROTO_BASE + 410)
#define CLTOMA_FUSE_READLINK (PROTO_BASE + 412)
#define CLTOMA_FUSE_SYMLINK (PROTO_BASE + 414)
#define CLTOMA_FUSE_MKNOD (PROTO_BASE + 416)
#define CLTOMA_FUSE_MKDIR (PROTO_BASE + 418)
#define CLTOMA_FUSE_UNLINK (PROTO_BASE + 420)
#define CLTOMA_FUSE_RMDIR (PROTO_BASE + 422)
#define CLTOMA_FUSE_RENAME (PROTO_BASE + 424)
#define CLTOMA_FUSE_LINK (PROTO_BASE + 426)
#define CLTOMA_FUSE_READDIR (PROTO_BASE + 428)
#define CLTOMA_FUSE_OPEN (PROTO_BASE + 430)
#define CLTOMA_FUSE_READ_CHUNK (PROTO_BASE + 432)
#define CLTOMA_FUSE_READCHUNK CLTOMA_FUSE_READ_CHUNK /* alias for tests */
#define CLTOMA_FUSE_WRITE_CHUNK (PROTO_BASE + 434)
#define CLTOMA_FUSE_WRITE_CHUNK_END (PROTO_BASE + 436)
#define CLTOMA_FUSE_APPEND_SLICE (PROTO_BASE + 438)
#define CLTOMA_FUSE_CHECK (PROTO_BASE + 440)
#define CLTOMA_FUSE_GETTRASHRETENTION (PROTO_BASE + 442)
#define CLTOMA_FUSE_SETTRASHRETENTION (PROTO_BASE + 444)
#define CLTOMA_FUSE_GETSCLASS (PROTO_BASE + 446)
#define CLTOMA_FUSE_SETSCLASS (PROTO_BASE + 448)
#define CLTOMA_FUSE_GETTRASH (PROTO_BASE + 450)
#define CLTOMA_FUSE_GETDETACHEDATTR (PROTO_BASE + 452)
#define CLTOMA_FUSE_GETTRASHPATH (PROTO_BASE + 454)
#define CLTOMA_FUSE_SETTRASHPATH (PROTO_BASE + 456)
#define CLTOMA_FUSE_UNDEL (PROTO_BASE + 458)
#define CLTOMA_FUSE_PURGE (PROTO_BASE + 460)
#define CLTOMA_FUSE_GETDIRSTATS (PROTO_BASE + 462)
#define CLTOMA_FUSE_TRUNCATE (PROTO_BASE + 464)
#define CLTOMA_FUSE_REPAIR (PROTO_BASE + 466)
#define CLTOMA_FUSE_SNAPSHOT (PROTO_BASE + 468)
#define CLTOMA_FUSE_GETSUSTAINED (PROTO_BASE + 470)
#define CLTOMA_FUSE_GETEATTR (PROTO_BASE + 472)
#define CLTOMA_FUSE_SETEATTR (PROTO_BASE + 474)
#define CLTOMA_FUSE_QUOTACONTROL (PROTO_BASE + 476)
#define CLTOMA_FUSE_GETXATTR (PROTO_BASE + 478)
#define CLTOMA_FUSE_SETXATTR (PROTO_BASE + 480)
#define CLTOMA_FUSE_CREATE (PROTO_BASE + 482)
#define CLTOMA_FUSE_PARENTS (PROTO_BASE + 484)
#define CLTOMA_FUSE_PATHS (PROTO_BASE + 486)
#define CLTOMA_FUSE_GETFACL (PROTO_BASE + 488)
#define CLTOMA_FUSE_SETFACL (PROTO_BASE + 490)
#define CLTOMA_FUSE_FLOCK (PROTO_BASE + 492)
#define CLTOMA_FUSE_POSIX_LOCK (PROTO_BASE + 494)
#define CLTOMA_FUSE_ARCHCTL (PROTO_BASE + 496)
#define CLTOMA_FUSE_FSYNC (PROTO_BASE + 498)
#define CLTOMA_FUSE_SUSTAINED_INODES_DEPRECATED (PROTO_BASE + 499)
#define CLTOMA_FUSE_SUSTAINED_INODES (PROTO_BASE + 700)
#define CLTOMA_FUSE_AMTIME_INODES (PROTO_BASE + 701)
#define CLTOMA_FUSE_TIME_SYNC (PROTO_BASE + 704)
#define CLTOMA_FUSE_OPDATA (PROTO_BASE + 710)
#define CLTOMA_FUSE_WFLAGS (PROTO_BASE + 711)

/* MATOCL_FUSE_* responses/notifications */
#define MATOCL_FUSE_REGISTER (PROTO_BASE + 401)
#define MATOCL_FUSE_STATFS (PROTO_BASE + 403)
#define MATOCL_FUSE_ACCESS (PROTO_BASE + 405)
#define MATOCL_FUSE_LOOKUP (PROTO_BASE + 407)
#define MATOCL_FUSE_GETATTR (PROTO_BASE + 409)
#define MATOCL_FUSE_SETATTR (PROTO_BASE + 411)
#define MATOCL_FUSE_READLINK (PROTO_BASE + 413)
#define MATOCL_FUSE_SYMLINK (PROTO_BASE + 415)
#define MATOCL_FUSE_MKNOD (PROTO_BASE + 417)
#define MATOCL_FUSE_MKDIR (PROTO_BASE + 419)
#define MATOCL_FUSE_UNLINK (PROTO_BASE + 421)
#define MATOCL_FUSE_RMDIR (PROTO_BASE + 423)
#define MATOCL_FUSE_RENAME (PROTO_BASE + 425)
#define MATOCL_FUSE_LINK (PROTO_BASE + 427)
#define MATOCL_FUSE_READDIR (PROTO_BASE + 429)
#define MATOCL_FUSE_OPEN (PROTO_BASE + 431)
#define MATOCL_FUSE_READ_CHUNK (PROTO_BASE + 433)
#define MATOCL_FUSE_WRITE_CHUNK (PROTO_BASE + 435)
#define MATOCL_FUSE_WRITE_CHUNK_END (PROTO_BASE + 437)
#define MATOCL_FUSE_APPEND_SLICE (PROTO_BASE + 439)
#define MATOCL_FUSE_CHECK (PROTO_BASE + 441)
#define MATOCL_FUSE_GETTRASHRETENTION (PROTO_BASE + 443)
#define MATOCL_FUSE_SETTRASHRETENTION (PROTO_BASE + 445)
#define MATOCL_FUSE_GETSCLASS (PROTO_BASE + 447)
#define MATOCL_FUSE_SETSCLASS (PROTO_BASE + 449)
#define MATOCL_FUSE_GETTRASH (PROTO_BASE + 451)
#define MATOCL_FUSE_GETDETACHEDATTR (PROTO_BASE + 453)
#define MATOCL_FUSE_GETTRASHPATH (PROTO_BASE + 455)
#define MATOCL_FUSE_SETTRASHPATH (PROTO_BASE + 457)
#define MATOCL_FUSE_UNDEL (PROTO_BASE + 459)
#define MATOCL_FUSE_PURGE (PROTO_BASE + 461)
#define MATOCL_FUSE_GETDIRSTATS (PROTO_BASE + 463)
#define MATOCL_FUSE_TRUNCATE (PROTO_BASE + 465)
#define MATOCL_FUSE_REPAIR (PROTO_BASE + 467)
#define MATOCL_FUSE_SNAPSHOT (PROTO_BASE + 469)
#define MATOCL_FUSE_GETSUSTAINED (PROTO_BASE + 471)
#define MATOCL_FUSE_GETEATTR (PROTO_BASE + 473)
#define MATOCL_FUSE_SETEATTR (PROTO_BASE + 475)
#define MATOCL_FUSE_QUOTACONTROL (PROTO_BASE + 477)
#define MATOCL_FUSE_GETXATTR (PROTO_BASE + 479)
#define MATOCL_FUSE_SETXATTR (PROTO_BASE + 481)
#define MATOCL_FUSE_CREATE (PROTO_BASE + 483)
#define MATOCL_FUSE_PARENTS (PROTO_BASE + 485)
#define MATOCL_FUSE_PATHS (PROTO_BASE + 487)
#define MATOCL_FUSE_GETFACL (PROTO_BASE + 489)
#define MATOCL_FUSE_SETFACL (PROTO_BASE + 491)
#define MATOCL_FUSE_FLOCK (PROTO_BASE + 493)
#define MATOCL_FUSE_POSIX_LOCK (PROTO_BASE + 495)
#define MATOCL_FUSE_ARCHCTL (PROTO_BASE + 497)
#define MATOCL_FUSE_FSYNC (PROTO_BASE + 499)
#define MATOCL_FUSE_CHUNK_HAS_CHANGED (PROTO_BASE + 702)
#define MATOCL_FUSE_FLENG_HAS_CHANGED (PROTO_BASE + 703)
#define MATOCL_FUSE_TIME_SYNC (PROTO_BASE + 705)
#define MATOCL_FUSE_INVALIDATE_CHUNK_CACHE (PROTO_BASE + 706)

/* Operation constants */
#define REGISTER_GETRANDOM 1
#define REGISTER_NEWSESSION 2
#define REGISTER_RECONNECT 3
#define REGISTER_TOOLS 4
#define REGISTER_NEWMETASESSION 5
#define REGISTER_CLOSESESSION 6

#define GETDIR_FLAG_WITHATTR 0x01
#define GETDIR_FLAG_ADDTOCACHE 0x02

#define MFS_XATTR_CREATE_OR_REPLACE 0
#define MFS_XATTR_CREATE_ONLY 1
#define MFS_XATTR_REPLACE_ONLY 2
#define MFS_XATTR_REMOVE 3
#define MFS_XATTR_GETA_DATA 0
#define MFS_XATTR_LENGTH_ONLY 1

#define MFS_CHUNKOPFLAG_CANMODTIME 0x01

#define TYPE_FILE 1
#define TYPE_DIRECTORY 2
#define TYPE_SYMLINK 3
#define TYPE_FIFO 4
#define TYPE_BLOCKDEV 5
#define TYPE_CHARDEV 6
#define TYPE_SOCKET 7
#define TYPE_TRASH 8

/* MooseFS status / error codes */
#define MFS_STATUS_OK 0              /* OK */
#define MFS_ERROR_EPERM 1            /* Operation not permitted */
#define MFS_ERROR_ENOTDIR 2          /* Not a directory */
#define MFS_ERROR_ENOENT 3           /* No such file or directory */
#define MFS_ERROR_EACCES 4           /* Permission denied */
#define MFS_ERROR_EEXIST 5           /* File exists */
#define MFS_ERROR_EINVAL 6           /* Invalid argument */
#define MFS_ERROR_ENOTEMPTY 7        /* Directory not empty */
#define MFS_ERROR_CHUNKLOST 8        /* Chunk lost */
#define MFS_ERROR_OUTOFMEMORY 9      /* Out of memory */
#define MFS_ERROR_INDEXTOOBIG 10     /* Index too big */
#define MFS_ERROR_LOCKED 11          /* Chunk locked */
#define MFS_ERROR_NOCHUNKSERVERS 12  /* No chunk servers */
#define MFS_ERROR_NOCHUNK 13         /* No such chunk */
#define MFS_ERROR_CHUNKBUSY 14       /* Chunk is busy */
#define MFS_ERROR_REGISTER 15        /* Incorrect register BLOB */
#define MFS_ERROR_NOTDONE 16         /* Operation not completed */
#define MFS_ERROR_NOTOPENED 17       /* File not opened */
#define MFS_ERROR_NOTSTARTED 18      /* Write not started */
#define MFS_ERROR_WRONGVERSION 19    /* Wrong chunk version */
#define MFS_ERROR_CHUNKEXIST 20      /* Chunk already exists */
#define MFS_ERROR_NOSPACE 21         /* No space left */
#define MFS_ERROR_IO 22              /* I/O error */
#define MFS_ERROR_BNUMTOOBIG 23      /* Incorrect block number */
#define MFS_ERROR_WRONGSIZE 24       /* Incorrect size */
#define MFS_ERROR_WRONGOFFSET 25     /* Incorrect offset */
#define MFS_ERROR_CANTCONNECT 26     /* Can't connect */
#define MFS_ERROR_WRONGCHUNKID 27    /* Incorrect chunk id */
#define MFS_ERROR_DISCONNECTED 28    /* Disconnected */
#define MFS_ERROR_CRC 29             /* CRC error */
#define MFS_ERROR_DELAYED 30         /* Operation delayed */
#define MFS_ERROR_CANTCREATEPATH 31  /* Can't create path */
#define MFS_ERROR_MISMATCH 32        /* Data mismatch */
#define MFS_ERROR_EROFS 33           /* Read-only file system */
#define MFS_ERROR_QUOTA 34           /* Quota exceeded */
#define MFS_ERROR_BADSESSIONID 35    /* Bad session id */
#define MFS_ERROR_NOPASSWORD 36      /* Password is needed */
#define MFS_ERROR_BADPASSWORD 37     /* Incorrect password */
#define MFS_ERROR_ENOATTR 38         /* Attribute not found */
#define MFS_ERROR_ENOTSUP 39         /* Operation not supported */
#define MFS_ERROR_ERANGE 40          /* Result too large */
#define MFS_ERROR_NOTFOUND 41        /* Entity not found */
#define MFS_ERROR_ACTIVE 42          /* Entity is active */
#define MFS_ERROR_CSNOTPRESENT 43    /* Chunkserver not present */
#define MFS_ERROR_WAITING 44         /* Waiting on lock */
#define MFS_ERROR_EAGAIN 45          /* Resource temporarily unavailable */
#define MFS_ERROR_EINTR 46           /* Interrupted system call */
#define MFS_ERROR_ECANCELED 47       /* Operation canceled */
#define MFS_ERROR_ENOENT_NOCACHE 48  /* No such file (not cacheable) */
#define MFS_ERROR_EPERM_NOTADMIN 49  /* Operation not permitted (admin only) */
#define MFS_ERROR_CLASSEXISTS 50     /* Class name already in use */
#define MFS_ERROR_CLASSLIMITREACH 51 /* Maximum number of classes reached */
#define MFS_ERROR_NOSUCHCLASS 52     /* No such class */
#define MFS_ERROR_CLASSINUSE 53      /* Class in use */
#define MFS_ERROR_INCOMPATVERSION 54 /* Incompatible version */
#define MFS_ERROR_PATTERNEXISTS 55   /* Pattern already defined */
#define MFS_ERROR_PATLIMITREACHED 56 /* Maximum number of patterns reached */
#define MFS_ERROR_NOSUCHPATTERN 57   /* No such pattern */
#define MFS_ERROR_ENAMETOOLONG 58    /* File name too long */
#define MFS_ERROR_EMLINK 59          /* Too many links */
#define MFS_ERROR_ETIMEDOUT 60       /* Operation timed out */
#define MFS_ERROR_EBADF 61           /* Bad file descriptor */
#define MFS_ERROR_EFBIG 62           /* File too large */
#define MFS_ERROR_EISDIR 63          /* Is a directory */
#define MFS_ERROR_MAX 64

#define MFS_REGISTER_FLAG_INCLUDE_SESSION BIT(0)
#define MFS_REGISTER_FLAG_INCLUDE_METAID BIT(1)
#define MFS_REGISTER_FLAG_INCLUDE_PASSCODE BIT(2)

struct mfs_register_info {
	const u8 *blob;      /* 64-byte register blob */
	u8 rcode;
	u32 version;
	const u8 *info;
	u32 info_len;
	const u8 *path;
	u32 path_len;
	u32 session_id;
	u64 meta_id;
	const u8 *passcode16;
};

struct mfs_setattr {
	u8 opened;
	u32 uid;
	u32 gid;
	u16 mode;
	u32 attr_uid;
	u32 attr_gid;
	u32 atime;
	u32 mtime;
	u8 winattr;
	u8 sugidclearmode;
};

struct mfs_attr {
	u8 flags;
	u8 type;
	u16 mode;
	u32 uid;
	u32 gid;
	u32 atime;
	u32 mtime;
	u32 ctime;
	u32 nlink;
	u64 size;
	u32 rdev;
	u8 winattr;
};

struct mfs_readdir_entry {
	char *name;
	u8 name_len;
	u32 inode;
	u8 type;
	struct mfs_attr attr;
};

struct mfs_statfs {
	u64 total_space;
	u64 avail_space;
	u64 free_space;
	u64 trash_space;
	u64 sustained_space;
	u32 inodes;
};

struct mfs_chunk_location {
	u32 ip;
	u16 port;
	u32 cs_ver;
	u32 labelmask;
};

struct mfs_cs_write_status {
	u64 chunk_id;
	u32 write_id;
	u8 status;
};

struct mfs_cs_read_packet {
	u64 chunk_id;
	u16 block_num;
	u16 block_offset;
	u32 size;
	u32 crc;
};

static inline void mfs_put64bit(u8 **ptr, u64 val)
{
	__be64 be = cpu_to_be64(val);

	memcpy(*ptr, &be, sizeof(be));
	*ptr += sizeof(be);
}

static inline void mfs_put32bit(u8 **ptr, u32 val)
{
	__be32 be = cpu_to_be32(val);

	memcpy(*ptr, &be, sizeof(be));
	*ptr += sizeof(be);
}

static inline void mfs_put16bit(u8 **ptr, u16 val)
{
	__be16 be = cpu_to_be16(val);

	memcpy(*ptr, &be, sizeof(be));
	*ptr += sizeof(be);
}

static inline void mfs_put8bit(u8 **ptr, u8 val)
{
	(*ptr)[0] = val;
	(*ptr)++;
}

static inline u64 mfs_get64bit(const u8 **ptr)
{
	__be64 be;

	memcpy(&be, *ptr, sizeof(be));
	*ptr += sizeof(be);
	return be64_to_cpu(be);
}

static inline u32 mfs_get32bit(const u8 **ptr)
{
	__be32 be;

	memcpy(&be, *ptr, sizeof(be));
	*ptr += sizeof(be);
	return be32_to_cpu(be);
}

static inline u16 mfs_get16bit(const u8 **ptr)
{
	__be16 be;

	memcpy(&be, *ptr, sizeof(be));
	*ptr += sizeof(be);
	return be16_to_cpu(be);
}

static inline u8 mfs_get8bit(const u8 **ptr)
{
	u8 v = (*ptr)[0];

	(*ptr)++;
	return v;
}

/* encode */
size_t mfs_pack_header(u8 *buf, u32 type, u32 length);
size_t mfs_encode_register(u8 *buf, size_t buf_len,
			   const struct mfs_register_info *info, u32 flags);
size_t mfs_encode_lookup(u8 *buf, size_t buf_len, u32 parent_inode,
			 const char *name, u8 name_len);
size_t mfs_encode_getattr(u8 *buf, size_t buf_len, u32 inode);
size_t mfs_encode_setattr(u8 *buf, size_t buf_len, u32 inode,
			  const struct mfs_setattr *attr, u8 setmask);
size_t mfs_encode_readdir(u8 *buf, size_t buf_len, u32 inode,
			  u64 offset, u32 max_entries);
size_t mfs_encode_create(u8 *buf, size_t buf_len, u32 parent,
			 const char *name, u16 mode, u16 umask,
			 u32 uid, u32 gid);
size_t mfs_encode_unlink(u8 *buf, size_t buf_len, u32 parent,
			 const char *name);
size_t mfs_encode_mkdir(u8 *buf, size_t buf_len, u32 parent,
			const char *name, u16 mode, u16 umask,
			u32 uid, u32 gid);
size_t mfs_encode_rmdir(u8 *buf, size_t buf_len, u32 parent,
			const char *name);
size_t mfs_encode_rename(u8 *buf, size_t buf_len, u32 src_parent,
			 const char *src_name, u32 dst_parent,
			 const char *dst_name);
size_t mfs_encode_link(u8 *buf, size_t buf_len, u32 inode,
		       u32 dst_parent, const char *dst_name);
size_t mfs_encode_symlink(u8 *buf, size_t buf_len, u32 parent,
			  const char *name, const char *target);
size_t mfs_encode_readlink(u8 *buf, size_t buf_len, u32 inode);
size_t mfs_encode_statfs(u8 *buf, size_t buf_len);
size_t mfs_encode_open(u8 *buf, size_t buf_len, u32 inode, u8 flags);
size_t mfs_encode_read_chunk(u8 *buf, size_t buf_len, u32 inode,
			     u32 chunk_index);
size_t mfs_encode_write_chunk(u8 *buf, size_t buf_len, u32 inode,
			      u32 chunk_index);
size_t mfs_encode_write_chunk_end(u8 *buf, size_t buf_len,
				  u64 chunk_id, u32 inode, u64 length);
size_t mfs_encode_flock(u8 *buf, size_t buf_len, u32 inode,
			u32 reqid, u64 owner, u8 cmd);
size_t mfs_encode_setxattr(u8 *buf, size_t buf_len, u32 inode,
			   const char *name, const void *value,
			   u32 value_len, u8 mode, u32 uid, u32 gid);
size_t mfs_encode_getxattr(u8 *buf, size_t buf_len, u32 inode,
			   const char *name, u8 mode, u32 uid, u32 gid);
size_t mfs_encode_listxattr(u8 *buf, size_t buf_len, u32 inode,
			    u8 mode, u32 uid, u32 gid);
size_t mfs_encode_removexattr(u8 *buf, size_t buf_len, u32 inode,
			      const char *name, u32 uid, u32 gid);
size_t mfs_encode_access(u8 *buf, size_t buf_len, u32 inode,
			 u32 uid, u32 gid, u16 mask);

/* decode */
int mfs_decode_header(const u8 *buf, size_t buf_len, u32 *type, u32 *length);
int mfs_decode_status(const u8 *buf, size_t buf_len, u8 *status);
int mfs_decode_attr(const u8 *buf, size_t buf_len, struct mfs_attr *attr);
int mfs_decode_lookup(const u8 *buf, size_t buf_len, u32 *inode,
		      struct mfs_attr *attr);
int mfs_decode_readdir(const u8 *buf, size_t buf_len,
		       struct mfs_readdir_entry *entries, u32 *count);
void mfs_readdir_entries_free(struct mfs_readdir_entry *entries, u32 count);
int mfs_decode_statfs(const u8 *buf, size_t buf_len, struct mfs_statfs *stats);
int mfs_decode_chunk_info(const u8 *buf, size_t buf_len, u64 *chunk_id,
			  u32 *version, struct mfs_chunk_location *locations,
			  u32 *loc_count);
int mfs_decode_write_chunk(const u8 *buf, size_t buf_len, u64 *chunk_id,
			   u32 *version, u64 *length,
			   struct mfs_chunk_location *locations,
			   u32 *loc_count);
int mfs_decode_readlink(const u8 *buf, size_t buf_len,
		       char *target, u32 *target_len);
int mfs_decode_xattr(const u8 *buf, size_t buf_len,
		     void *value, u32 *value_len);

/* chunkserver */
size_t mfs_cs_encode_read(u8 *buf, size_t buf_len, u64 chunk_id,
			  u32 version, u32 offset, u32 size);
size_t mfs_cs_encode_write(u8 *buf, size_t buf_len, u64 chunk_id,
			   u32 version, u32 chain_length,
			   const struct mfs_chunk_location *chain_servers);
size_t mfs_cs_encode_write_data(u8 *buf, size_t buf_len, u64 chunk_id,
				u32 write_id, u16 block_num,
				u16 offset, u32 size,
				u32 crc, const void *data);
size_t mfs_cs_encode_write_end(u8 *buf, size_t buf_len);
size_t mfs_cs_encode_write_finish(u8 *buf, size_t buf_len,
				  u64 chunk_id, u32 version);
int mfs_cs_decode_read_data(const u8 *buf, size_t buf_len,
			    struct mfs_cs_read_packet *packet,
			    const u8 **data, u32 *size);
int mfs_cs_decode_write_status(const u8 *buf, size_t buf_len,
			       struct mfs_cs_write_status *status);

u32 mfs_cs_crc32_block(const u8 *data, u32 size);
u64 mfs_chunk_index_from_offset(u64 offset);
u32 mfs_chunk_offset_in_chunk(u64 offset);
u16 mfs_block_index_from_chunk_offset(u32 chunk_offset);
u16 mfs_block_offset_in_block(u32 chunk_offset);

/* errno mapping */
int mfs_status_to_errno(s32 status);
const char *mfs_status_to_string(u8 status);
const char *mfs_status_description(u8 status);

#endif /* _MFS_PROTO_H_ */
