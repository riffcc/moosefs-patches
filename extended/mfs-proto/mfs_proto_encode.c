#include <linux/export.h>
#include <linux/string.h>

#include "mfs_proto.h"

#define MFS_DEFAULT_MSGID 0U
#define MFS_DEFAULT_UID 0U
#define MFS_DEFAULT_GID 0U

#define FUSE_REGISTER_BLOB_ACL \
	"DjI1GAQDULI5d2YjA26ypc3ovkhjvhciTQVx3CS4nYgtBoUcsljiVpsErJENHaw0"

static bool mfs_encode_need(size_t need, size_t have)
{
	return need <= have;
}

static size_t mfs_encode_finalize(u8 *buf, u8 *ptr, u32 type)
{
	u32 payload_len;

	payload_len = (u32)(ptr - (buf + sizeof(struct mfs_packet_header)));
	mfs_pack_header(buf, type, payload_len);
	return sizeof(struct mfs_packet_header) + payload_len;
}

static bool mfs_encode_u32_name(const char *name, u8 *name_len)
{
	size_t len;

	if (!name || !name_len)
		return false;

	len = strnlen(name, MFS_NAME_MAX + 1U);
	if (len == 0 || len > MFS_NAME_MAX)
		return false;

	*name_len = (u8)len;
	return true;
}

static bool mfs_encode_symlink_target(const char *target, u32 *target_len)
{
	size_t len;

	if (!target || !target_len)
		return false;

	len = strnlen(target, MFS_SYMLINK_MAX);
	if (len == 0 || len >= MFS_SYMLINK_MAX)
		return false;

	*target_len = (u32)(len + 1U); /* include NUL terminator */
	return true;
}

static void mfs_encode_uid_gids_1(u8 **ptr, u32 uid, u32 gid)
{
	mfs_put32bit(ptr, uid);
	mfs_put32bit(ptr, 1);
	mfs_put32bit(ptr, gid);
}

size_t mfs_pack_header(u8 *buf, u32 type, u32 length)
{
	u8 *ptr = buf;

	if (!buf)
		return 0;

	mfs_put32bit(&ptr, type);
	mfs_put32bit(&ptr, length);
	return sizeof(struct mfs_packet_header);
}
EXPORT_SYMBOL_GPL(mfs_pack_header);

size_t mfs_encode_register(u8 *buf, size_t buf_len,
			   const struct mfs_register_info *info, u32 flags)
{
	u8 *ptr;
	const u8 *blob;
	u32 payload_len;

	if (!buf || !info)
		return 0;

	blob = info->blob ? info->blob : (const u8 *)FUSE_REGISTER_BLOB_ACL;
	payload_len = 64 + 1;

	switch (info->rcode) {
	case REGISTER_GETRANDOM:
		break;
	case REGISTER_NEWSESSION:
	case REGISTER_NEWMETASESSION:
		payload_len += 4 + 4 + info->info_len;
		if (info->rcode == REGISTER_NEWSESSION)
			payload_len += 4 + info->path_len;
		if (flags & MFS_REGISTER_FLAG_INCLUDE_SESSION)
			payload_len += 4;
		if (flags & MFS_REGISTER_FLAG_INCLUDE_METAID)
			payload_len += 8;
		if (flags & MFS_REGISTER_FLAG_INCLUDE_PASSCODE)
			payload_len += 16;
		break;
	case REGISTER_RECONNECT:
	case REGISTER_TOOLS:
		payload_len += 4 + 4;
		if (flags & MFS_REGISTER_FLAG_INCLUDE_METAID)
			payload_len += 8;
		break;
	case REGISTER_CLOSESESSION:
		payload_len += 4;
		if (flags & MFS_REGISTER_FLAG_INCLUDE_METAID)
			payload_len += 8;
		break;
	default:
		return 0;
	}

	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	memcpy(ptr, blob, 64);
	ptr += 64;
	mfs_put8bit(&ptr, info->rcode);

	switch (info->rcode) {
	case REGISTER_GETRANDOM:
		break;
	case REGISTER_NEWSESSION:
	case REGISTER_NEWMETASESSION:
		mfs_put32bit(&ptr, info->version);
		mfs_put32bit(&ptr, info->info_len);
		if (info->info_len && info->info) {
			memcpy(ptr, info->info, info->info_len);
			ptr += info->info_len;
		}
		if (info->rcode == REGISTER_NEWSESSION) {
			mfs_put32bit(&ptr, info->path_len);
			if (info->path_len && info->path) {
				memcpy(ptr, info->path, info->path_len);
				ptr += info->path_len;
			}
		}
		if (flags & MFS_REGISTER_FLAG_INCLUDE_SESSION)
			mfs_put32bit(&ptr, info->session_id);
		if (flags & MFS_REGISTER_FLAG_INCLUDE_METAID)
			mfs_put64bit(&ptr, info->meta_id);
		if (flags & MFS_REGISTER_FLAG_INCLUDE_PASSCODE) {
			if (!info->passcode16)
				return 0;
			memcpy(ptr, info->passcode16, 16);
			ptr += 16;
		}
		break;
	case REGISTER_RECONNECT:
	case REGISTER_TOOLS:
		mfs_put32bit(&ptr, info->session_id);
		mfs_put32bit(&ptr, info->version);
		if (flags & MFS_REGISTER_FLAG_INCLUDE_METAID)
			mfs_put64bit(&ptr, info->meta_id);
		break;
	case REGISTER_CLOSESESSION:
		mfs_put32bit(&ptr, info->session_id);
		if (flags & MFS_REGISTER_FLAG_INCLUDE_METAID)
			mfs_put64bit(&ptr, info->meta_id);
		break;
	default:
		return 0;
	}

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_REGISTER);
}
EXPORT_SYMBOL_GPL(mfs_encode_register);

size_t mfs_encode_lookup(u8 *buf, size_t buf_len, u32 parent_inode,
			 const char *name, u8 name_len)
{
	u8 *ptr;
	u32 payload_len = 17 + name_len;

	if (!buf || !name || !name_len || name_len > MFS_NAME_MAX)
		return 0;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, parent_inode);
	mfs_put8bit(&ptr, name_len);
	memcpy(ptr, name, name_len);
	ptr += name_len;
	mfs_encode_uid_gids_1(&ptr, MFS_DEFAULT_UID, MFS_DEFAULT_GID);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_LOOKUP);
}
EXPORT_SYMBOL_GPL(mfs_encode_lookup);

size_t mfs_encode_getattr(u8 *buf, size_t buf_len, u32 inode)
{
	u8 *ptr;
	u32 payload_len = 8;

	if (!buf)
		return 0;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_GETATTR);
}
EXPORT_SYMBOL_GPL(mfs_encode_getattr);

size_t mfs_encode_setattr(u8 *buf, size_t buf_len, u32 inode,
			  const struct mfs_setattr *attr, u8 setmask)
{
	u8 *ptr;
	u32 payload_len = 38;

	if (!buf || !attr)
		return 0;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	mfs_put8bit(&ptr, attr->opened);
	mfs_encode_uid_gids_1(&ptr, attr->uid, attr->gid);
	mfs_put8bit(&ptr, setmask);
	mfs_put16bit(&ptr, attr->mode);
	mfs_put32bit(&ptr, attr->attr_uid);
	mfs_put32bit(&ptr, attr->attr_gid);
	mfs_put32bit(&ptr, attr->atime);
	mfs_put32bit(&ptr, attr->mtime);
	mfs_put8bit(&ptr, attr->winattr);
	mfs_put8bit(&ptr, attr->sugidclearmode);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_SETATTR);
}
EXPORT_SYMBOL_GPL(mfs_encode_setattr);

size_t mfs_encode_readdir(u8 *buf, size_t buf_len, u32 inode,
			  u64 offset, u32 max_entries)
{
	u8 *ptr;
	u32 payload_len = 33;

	if (!buf)
		return 0;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	mfs_encode_uid_gids_1(&ptr, MFS_DEFAULT_UID, MFS_DEFAULT_GID);
	mfs_put8bit(&ptr, GETDIR_FLAG_WITHATTR);
	mfs_put32bit(&ptr, max_entries);
	mfs_put64bit(&ptr, offset);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_READDIR);
}
EXPORT_SYMBOL_GPL(mfs_encode_readdir);

size_t mfs_encode_create(u8 *buf, size_t buf_len, u32 parent,
			 const char *name, u16 mode, u16 umask,
			 u32 uid, u32 gid)
{
	u8 *ptr;
	u8 name_len;
	u32 payload_len;

	if (!mfs_encode_u32_name(name, &name_len))
		return 0;
	payload_len = 25 + name_len;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, parent);
	mfs_put8bit(&ptr, name_len);
	memcpy(ptr, name, name_len);
	ptr += name_len;
	mfs_put16bit(&ptr, mode);
	mfs_put16bit(&ptr, umask);
	mfs_encode_uid_gids_1(&ptr, uid, gid);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_CREATE);
}
EXPORT_SYMBOL_GPL(mfs_encode_create);

size_t mfs_encode_unlink(u8 *buf, size_t buf_len, u32 parent,
			 const char *name)
{
	u8 *ptr;
	u8 name_len;
	u32 payload_len;

	if (!mfs_encode_u32_name(name, &name_len))
		return 0;
	payload_len = 21 + name_len;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, parent);
	mfs_put8bit(&ptr, name_len);
	memcpy(ptr, name, name_len);
	ptr += name_len;
	mfs_encode_uid_gids_1(&ptr, MFS_DEFAULT_UID, MFS_DEFAULT_GID);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_UNLINK);
}
EXPORT_SYMBOL_GPL(mfs_encode_unlink);

size_t mfs_encode_mkdir(u8 *buf, size_t buf_len, u32 parent,
			const char *name, u16 mode, u16 umask,
			u32 uid, u32 gid)
{
	u8 *ptr;
	u8 name_len;
	u32 payload_len;

	if (!mfs_encode_u32_name(name, &name_len))
		return 0;
	payload_len = 26 + name_len;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, parent);
	mfs_put8bit(&ptr, name_len);
	memcpy(ptr, name, name_len);
	ptr += name_len;
	mfs_put16bit(&ptr, mode);
	mfs_put16bit(&ptr, umask);
	mfs_encode_uid_gids_1(&ptr, uid, gid);
	mfs_put8bit(&ptr, 0); /* copysgid */

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_MKDIR);
}
EXPORT_SYMBOL_GPL(mfs_encode_mkdir);

size_t mfs_encode_rmdir(u8 *buf, size_t buf_len, u32 parent,
			const char *name)
{
	u8 *ptr;
	u8 name_len;
	u32 payload_len;

	if (!mfs_encode_u32_name(name, &name_len))
		return 0;
	payload_len = 21 + name_len;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, parent);
	mfs_put8bit(&ptr, name_len);
	memcpy(ptr, name, name_len);
	ptr += name_len;
	mfs_encode_uid_gids_1(&ptr, MFS_DEFAULT_UID, MFS_DEFAULT_GID);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_RMDIR);
}
EXPORT_SYMBOL_GPL(mfs_encode_rmdir);

size_t mfs_encode_rename(u8 *buf, size_t buf_len, u32 src_parent,
			 const char *src_name, u32 dst_parent,
			 const char *dst_name)
{
	u8 *ptr;
	u8 src_len;
	u8 dst_len;
	u32 payload_len;

	if (!mfs_encode_u32_name(src_name, &src_len) ||
	    !mfs_encode_u32_name(dst_name, &dst_len))
		return 0;

	payload_len = 27 + src_len + dst_len;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, src_parent);
	mfs_put8bit(&ptr, src_len);
	memcpy(ptr, src_name, src_len);
	ptr += src_len;
	mfs_put32bit(&ptr, dst_parent);
	mfs_put8bit(&ptr, dst_len);
	memcpy(ptr, dst_name, dst_len);
	ptr += dst_len;
	mfs_encode_uid_gids_1(&ptr, MFS_DEFAULT_UID, MFS_DEFAULT_GID);
	mfs_put8bit(&ptr, 0); /* rename mode */

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_RENAME);
}
EXPORT_SYMBOL_GPL(mfs_encode_rename);

size_t mfs_encode_link(u8 *buf, size_t buf_len, u32 inode,
		       u32 dst_parent, const char *dst_name)
{
	u8 *ptr;
	u8 name_len;
	u32 payload_len;

	if (!mfs_encode_u32_name(dst_name, &name_len))
		return 0;
	payload_len = 25 + name_len;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	mfs_put32bit(&ptr, dst_parent);
	mfs_put8bit(&ptr, name_len);
	memcpy(ptr, dst_name, name_len);
	ptr += name_len;
	mfs_encode_uid_gids_1(&ptr, MFS_DEFAULT_UID, MFS_DEFAULT_GID);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_LINK);
}
EXPORT_SYMBOL_GPL(mfs_encode_link);

size_t mfs_encode_symlink(u8 *buf, size_t buf_len, u32 parent,
			  const char *name, const char *target)
{
	u8 *ptr;
	u8 name_len;
	u32 target_len;
	u32 payload_len;

	if (!mfs_encode_u32_name(name, &name_len) ||
	    !mfs_encode_symlink_target(target, &target_len))
		return 0;

	payload_len = 21 + name_len + target_len;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, parent);
	mfs_put8bit(&ptr, name_len);
	memcpy(ptr, name, name_len);
	ptr += name_len;
	mfs_put32bit(&ptr, target_len);
	memcpy(ptr, target, target_len - 1);
	ptr += target_len - 1;
	mfs_put8bit(&ptr, 0);
	mfs_encode_uid_gids_1(&ptr, MFS_DEFAULT_UID, MFS_DEFAULT_GID);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_SYMLINK);
}
EXPORT_SYMBOL_GPL(mfs_encode_symlink);

size_t mfs_encode_readlink(u8 *buf, size_t buf_len, u32 inode)
{
	u8 *ptr;
	u32 payload_len = 8;

	if (!buf)
		return 0;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_READLINK);
}
EXPORT_SYMBOL_GPL(mfs_encode_readlink);

size_t mfs_encode_statfs(u8 *buf, size_t buf_len)
{
	u8 *ptr;
	u32 payload_len = 4;

	if (!buf)
		return 0;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_STATFS);
}
EXPORT_SYMBOL_GPL(mfs_encode_statfs);

size_t mfs_encode_open(u8 *buf, size_t buf_len, u32 inode, u8 flags)
{
	u8 *ptr;
	u32 payload_len = 21;

	if (!buf)
		return 0;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	mfs_encode_uid_gids_1(&ptr, MFS_DEFAULT_UID, MFS_DEFAULT_GID);
	mfs_put8bit(&ptr, flags);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_OPEN);
}
EXPORT_SYMBOL_GPL(mfs_encode_open);

size_t mfs_encode_read_chunk(u8 *buf, size_t buf_len, u32 inode,
			     u32 chunk_index)
{
	u8 *ptr;
	u32 payload_len = 13;

	if (!buf)
		return 0;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	mfs_put32bit(&ptr, chunk_index);
	mfs_put8bit(&ptr, 0);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_READ_CHUNK);
}
EXPORT_SYMBOL_GPL(mfs_encode_read_chunk);

size_t mfs_encode_write_chunk(u8 *buf, size_t buf_len, u32 inode,
			      u32 chunk_index)
{
	u8 *ptr;
	u32 payload_len = 13;

	if (!buf)
		return 0;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	mfs_put32bit(&ptr, chunk_index);
	mfs_put8bit(&ptr, MFS_CHUNKOPFLAG_CANMODTIME);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_WRITE_CHUNK);
}
EXPORT_SYMBOL_GPL(mfs_encode_write_chunk);

size_t mfs_encode_write_chunk_end(u8 *buf, size_t buf_len,
				  u64 chunk_id, u32 inode, u64 length)
{
	u8 *ptr;
	u32 payload_len = 24;

	if (!buf)
		return 0;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put64bit(&ptr, chunk_id);
	mfs_put32bit(&ptr, inode);
	mfs_put64bit(&ptr, length);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_WRITE_CHUNK_END);
}
EXPORT_SYMBOL_GPL(mfs_encode_write_chunk_end);

size_t mfs_encode_flock(u8 *buf, size_t buf_len, u32 inode,
			u32 reqid, u64 owner, u8 cmd)
{
	u8 *ptr;
	u32 payload_len = 21;

	if (!buf)
		return 0;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	mfs_put32bit(&ptr, reqid);
	mfs_put64bit(&ptr, owner);
	mfs_put8bit(&ptr, cmd);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_FLOCK);
}
EXPORT_SYMBOL_GPL(mfs_encode_flock);

size_t mfs_encode_setxattr(u8 *buf, size_t buf_len, u32 inode,
			   const char *name, const void *value,
			   u32 value_len, u8 mode, u32 uid, u32 gid)
{
	u8 *ptr;
	u8 name_len;
	u32 payload_len;

	if (!mfs_encode_u32_name(name, &name_len) || !value)
		return 0;
	if (mode > MFS_XATTR_REMOVE)
		return 0;

	payload_len = 23 + name_len + value_len;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	mfs_put8bit(&ptr, name_len);
	memcpy(ptr, name, name_len);
	ptr += name_len;
	mfs_put32bit(&ptr, value_len);
	if (value_len) {
		memcpy(ptr, value, value_len);
		ptr += value_len;
	}
	mfs_put8bit(&ptr, mode);
	mfs_put8bit(&ptr, 0); /* opened */
	mfs_encode_uid_gids_1(&ptr, uid, gid);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_SETXATTR);
}
EXPORT_SYMBOL_GPL(mfs_encode_setxattr);

size_t mfs_encode_getxattr(u8 *buf, size_t buf_len, u32 inode,
			   const char *name, u8 mode, u32 uid, u32 gid)
{
	u8 *ptr;
	u8 name_len = 0;
	u32 payload_len;

	if (!buf)
		return 0;
	if (name && !mfs_encode_u32_name(name, &name_len))
		return 0;
	if (mode > MFS_XATTR_LENGTH_ONLY)
		return 0;

	payload_len = 23 + name_len;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	mfs_put8bit(&ptr, name_len);
	if (name_len) {
		memcpy(ptr, name, name_len);
		ptr += name_len;
	}
	mfs_put8bit(&ptr, mode);
	mfs_put8bit(&ptr, 0); /* opened */
	mfs_encode_uid_gids_1(&ptr, uid, gid);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_GETXATTR);
}
EXPORT_SYMBOL_GPL(mfs_encode_getxattr);

size_t mfs_encode_listxattr(u8 *buf, size_t buf_len, u32 inode,
			    u8 mode, u32 uid, u32 gid)
{
	/* empty xattr name selects list mode in MooseFS */
	return mfs_encode_getxattr(buf, buf_len, inode, NULL, mode, uid, gid);
}
EXPORT_SYMBOL_GPL(mfs_encode_listxattr);

size_t mfs_encode_removexattr(u8 *buf, size_t buf_len, u32 inode,
			      const char *name, u32 uid, u32 gid)
{
	u8 *ptr;
	u8 name_len;
	u32 payload_len;

	if (!mfs_encode_u32_name(name, &name_len))
		return 0;

	payload_len = 23 + name_len;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	mfs_put8bit(&ptr, name_len);
	memcpy(ptr, name, name_len);
	ptr += name_len;
	mfs_put32bit(&ptr, 0); /* value length */
	mfs_put8bit(&ptr, MFS_XATTR_REMOVE);
	mfs_put8bit(&ptr, 0); /* opened */
	mfs_encode_uid_gids_1(&ptr, uid, gid);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_SETXATTR);
}
EXPORT_SYMBOL_GPL(mfs_encode_removexattr);

size_t mfs_encode_access(u8 *buf, size_t buf_len, u32 inode,
			 u32 uid, u32 gid, u16 mask)
{
	u8 *ptr;
	u32 payload_len = 22;

	if (!buf)
		return 0;
	if (!mfs_encode_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put32bit(&ptr, MFS_DEFAULT_MSGID);
	mfs_put32bit(&ptr, inode);
	mfs_encode_uid_gids_1(&ptr, uid, gid);
	mfs_put16bit(&ptr, mask);

	return mfs_encode_finalize(buf, ptr, CLTOMA_FUSE_ACCESS);
}
EXPORT_SYMBOL_GPL(mfs_encode_access);
