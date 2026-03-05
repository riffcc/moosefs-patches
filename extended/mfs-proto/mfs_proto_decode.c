#include <linux/export.h>
#include <linux/string.h>

#include "mfs_proto.h"

static int mfs_type_from_legacy(u8 legacy)
{
	switch (legacy) {
	case 'f':
		return TYPE_FILE;
	case 'd':
		return TYPE_DIRECTORY;
	case 'l':
		return TYPE_SYMLINK;
	case 'q':
		return TYPE_FIFO;
	case 'b':
		return TYPE_BLOCKDEV;
	case 'c':
		return TYPE_CHARDEV;
	case 's':
		return TYPE_SOCKET;
	case 't':
		return TYPE_TRASH;
	default:
		if (legacy >= TYPE_FILE && legacy <= TYPE_TRASH)
			return legacy;
		return 0;
	}
}

static bool mfs_lookup_tail_valid(size_t tail)
{
	if (tail == 0 || tail == 2)
		return true;
	if (tail >= 15 && ((tail - 15) % 14) == 0)
		return true;
	return false;
}

static int mfs_parse_chunk_common(const u8 *buf, size_t buf_len, u64 *length,
				  u64 *chunk_id, u32 *version,
				  struct mfs_chunk_location *locations,
				  u32 *loc_count)
{
	const u8 *ptr = buf;
	u8 proto = 0;
	u32 count;
	u32 max_out;
	u32 i;
	u32 entry_size;
	u8 status;

	if (!buf || !chunk_id || !version || !loc_count)
		return -EINVAL;
	if (buf_len < 5)
		return -EMSGSIZE;

	(void)mfs_get32bit(&ptr); /* msgid */
	buf_len -= 4;

	if (buf_len == 1) {
		status = mfs_get8bit(&ptr);
		return mfs_status_to_errno(status);
	}

	if (buf_len & 1U) {
		proto = mfs_get8bit(&ptr);
		buf_len--;
	}

	if (buf_len < 20)
		return -EPROTO;

	if (length)
		*length = mfs_get64bit(&ptr);
	else
		(void)mfs_get64bit(&ptr);
	*chunk_id = mfs_get64bit(&ptr);
	*version = mfs_get32bit(&ptr);
	buf_len -= 20;

	switch (proto) {
	case 0:
		entry_size = 6;
		break;
	case 1:
		entry_size = 10;
		break;
	case 2:
	case 3:
		entry_size = 14;
		break;
	default:
		return -EPROTO;
	}

	if (entry_size == 0 || (buf_len % entry_size) != 0)
		return -EPROTO;

	count = (u32)(buf_len / entry_size);
	max_out = *loc_count;
	if (max_out < count)
		return -ENOSPC;

	for (i = 0; i < count; i++) {
		if (locations) {
			locations[i].ip = mfs_get32bit(&ptr);
			locations[i].port = mfs_get16bit(&ptr);
			locations[i].cs_ver = 0;
			locations[i].labelmask = 0;
			if (entry_size >= 10)
				locations[i].cs_ver = mfs_get32bit(&ptr);
			if (entry_size >= 14)
				locations[i].labelmask = mfs_get32bit(&ptr);
		} else {
			ptr += entry_size;
		}
	}

	*loc_count = count;
	return 0;
}

int mfs_decode_header(const u8 *buf, size_t buf_len, u32 *type, u32 *length)
{
	const u8 *ptr = buf;

	if (!buf || !type || !length)
		return -EINVAL;
	if (buf_len < sizeof(struct mfs_packet_header))
		return -EMSGSIZE;

	*type = mfs_get32bit(&ptr);
	*length = mfs_get32bit(&ptr);
	if (*length > MFS_MAX_PACKETSIZE)
		return -EMSGSIZE;

	return 0;
}
EXPORT_SYMBOL_GPL(mfs_decode_header);

int mfs_decode_status(const u8 *buf, size_t buf_len, u8 *status)
{
	if (!buf || !status)
		return -EINVAL;

	if (buf_len == 1) {
		*status = buf[0];
		return 0;
	}
	if (buf_len == 5) {
		*status = buf[4];
		return 0;
	}

	return -EPROTO;
}
EXPORT_SYMBOL_GPL(mfs_decode_status);

int mfs_decode_attr(const u8 *buf, size_t buf_len, struct mfs_attr *attr)
{
	const u8 *ptr = buf;
	u8 first;
	u16 mode_word;
	u16 major;
	u16 minor;

	if (!buf || !attr)
		return -EINVAL;
	if (buf_len < 35)
		return -EMSGSIZE;

	memset(attr, 0, sizeof(*attr));

	first = mfs_get8bit(&ptr);
	if (first < 64) {
		attr->flags = first;
		mode_word = mfs_get16bit(&ptr);
		attr->type = (mode_word >> 12) & 0x0F;
		attr->mode = mode_word & 0x0FFF;
	} else {
		attr->flags = 0;
		attr->type = mfs_type_from_legacy(first & 0x7F);
		attr->mode = mfs_get16bit(&ptr) & 0x0FFF;
	}

	attr->uid = mfs_get32bit(&ptr);
	attr->gid = mfs_get32bit(&ptr);
	attr->atime = mfs_get32bit(&ptr);
	attr->mtime = mfs_get32bit(&ptr);
	attr->ctime = mfs_get32bit(&ptr);
	attr->nlink = mfs_get32bit(&ptr);

	if (attr->type == TYPE_FILE || attr->type == TYPE_DIRECTORY ||
	    attr->type == TYPE_SYMLINK) {
		attr->size = mfs_get64bit(&ptr);
	} else if (attr->type == TYPE_BLOCKDEV || attr->type == TYPE_CHARDEV) {
		major = mfs_get16bit(&ptr);
		minor = mfs_get16bit(&ptr);
		(void)mfs_get32bit(&ptr);
		attr->rdev = ((u32)major << 16) | minor;
	} else {
		(void)mfs_get64bit(&ptr);
	}

	if ((size_t)(ptr - buf) < buf_len)
		attr->winattr = mfs_get8bit(&ptr);
	else
		attr->winattr = 0;

	return 0;
}
EXPORT_SYMBOL_GPL(mfs_decode_attr);

int mfs_decode_lookup(const u8 *buf, size_t buf_len, u32 *inode,
		      struct mfs_attr *attr)
{
	const u8 *ptr = buf;
	u8 status;
	size_t rem;
	size_t attr_size;
	bool valid35;
	bool valid36;

	if (!buf || !inode || !attr)
		return -EINVAL;
	if (buf_len < 5)
		return -EMSGSIZE;

	(void)mfs_get32bit(&ptr); /* msgid */
	buf_len -= 4;

	if (buf_len == 1) {
		status = mfs_get8bit(&ptr);
		return mfs_status_to_errno(status);
	}
	if (buf_len < 4 + 35)
		return -EPROTO;

	*inode = mfs_get32bit(&ptr);
	rem = buf_len - 4;

	valid35 = rem >= 35 && mfs_lookup_tail_valid(rem - 35);
	valid36 = rem >= 36 && mfs_lookup_tail_valid(rem - 36);
	if (valid36)
		attr_size = 36;
	else if (valid35)
		attr_size = 35;
	else
		return -EPROTO;

	return mfs_decode_attr(ptr, attr_size, attr);
}
EXPORT_SYMBOL_GPL(mfs_decode_lookup);

void mfs_readdir_entries_free(struct mfs_readdir_entry *entries, u32 count)
{
	u32 i;

	if (!entries)
		return;

	for (i = 0; i < count; i++) {
		kfree(entries[i].name);
		entries[i].name = NULL;
	}
}
EXPORT_SYMBOL_GPL(mfs_readdir_entries_free);

int mfs_decode_readdir(const u8 *buf, size_t buf_len,
		       struct mfs_readdir_entry *entries, u32 *count)
{
	const u8 *ptr;
	u8 status;
	u32 max_entries;
	u32 used = 0;

	if (!buf || !entries || !count)
		return -EINVAL;
	if (buf_len < 5)
		return -EMSGSIZE;

	max_entries = *count;
	ptr = buf;
	(void)mfs_get32bit(&ptr); /* msgid */
	buf_len -= 4;

	if (buf_len == 1) {
		status = mfs_get8bit(&ptr);
		return mfs_status_to_errno(status);
	}

	/* nedgeid present in modern GETDIR request format */
	if (buf_len >= 8) {
		(void)mfs_get64bit(&ptr);
		buf_len -= 8;
	}

	while (buf_len > 0) {
		u8 nlen;
		size_t attr_size;

		if (buf_len < 1 + 4 + 35) {
			mfs_readdir_entries_free(entries, used);
			return -EPROTO;
		}
		if (used >= max_entries) {
			mfs_readdir_entries_free(entries, used);
			return -ENOSPC;
		}

		nlen = mfs_get8bit(&ptr);
		buf_len -= 1;
		if (nlen == 0 || buf_len < (size_t)nlen + 4 + 35) {
			mfs_readdir_entries_free(entries, used);
			return -EPROTO;
		}

		entries[used].name = kmalloc(nlen + 1, GFP_KERNEL);
		if (!entries[used].name) {
			mfs_readdir_entries_free(entries, used);
			return -ENOMEM;
		}
		memcpy(entries[used].name, ptr, nlen);
		entries[used].name[nlen] = '\0';
		entries[used].name_len = nlen;
		ptr += nlen;
		buf_len -= nlen;

		entries[used].inode = mfs_get32bit(&ptr);
		buf_len -= 4;

		attr_size = (buf_len >= 36) ? 36 : 35;
		if (mfs_decode_attr(ptr, attr_size, &entries[used].attr) < 0) {
			mfs_readdir_entries_free(entries, used + 1);
			return -EPROTO;
		}
		entries[used].type = entries[used].attr.type;
		ptr += attr_size;
		buf_len -= attr_size;
		used++;
	}

	*count = used;
	return 0;
}
EXPORT_SYMBOL_GPL(mfs_decode_readdir);

int mfs_decode_statfs(const u8 *buf, size_t buf_len, struct mfs_statfs *stats)
{
	const u8 *ptr = buf;
	size_t rem = buf_len;

	if (!buf || !stats)
		return -EINVAL;
	if (buf_len < 28)
		return -EMSGSIZE;

	memset(stats, 0, sizeof(*stats));

	if (rem >= 4 && (rem - 4 == 28 || rem - 4 == 36 || rem - 4 == 40 ||
			   rem - 4 == 44)) {
		(void)mfs_get32bit(&ptr); /* msgid */
		rem -= 4;
	}

	if (rem == 28) {
		stats->total_space = mfs_get64bit(&ptr);
		stats->avail_space = mfs_get64bit(&ptr);
		stats->trash_space = mfs_get64bit(&ptr);
		stats->inodes = mfs_get32bit(&ptr);
		stats->free_space = stats->avail_space;
		stats->sustained_space = 0;
		return 0;
	}
	if (rem == 36) {
		stats->total_space = mfs_get64bit(&ptr);
		stats->avail_space = mfs_get64bit(&ptr);
		stats->trash_space = mfs_get64bit(&ptr);
		stats->sustained_space = mfs_get64bit(&ptr);
		stats->inodes = mfs_get32bit(&ptr);
		stats->free_space = stats->avail_space;
		return 0;
	}
	if (rem == 40 || rem == 44) {
		stats->total_space = mfs_get64bit(&ptr);
		stats->avail_space = mfs_get64bit(&ptr);
		stats->free_space = mfs_get64bit(&ptr);
		stats->trash_space = mfs_get64bit(&ptr);
		stats->sustained_space = mfs_get64bit(&ptr);
		stats->inodes = mfs_get32bit(&ptr);
		return 0;
	}

	return -EPROTO;
}
EXPORT_SYMBOL_GPL(mfs_decode_statfs);

int mfs_decode_chunk_info(const u8 *buf, size_t buf_len, u64 *chunk_id,
			  u32 *version, struct mfs_chunk_location *locations,
			  u32 *loc_count)
{
	return mfs_parse_chunk_common(buf, buf_len, NULL, chunk_id, version,
			      locations, loc_count);
}
EXPORT_SYMBOL_GPL(mfs_decode_chunk_info);

int mfs_decode_write_chunk(const u8 *buf, size_t buf_len, u64 *chunk_id,
			   u32 *version, u64 *length,
			   struct mfs_chunk_location *locations,
			   u32 *loc_count)
{
	return mfs_parse_chunk_common(buf, buf_len, length, chunk_id, version,
			      locations, loc_count);
}
EXPORT_SYMBOL_GPL(mfs_decode_write_chunk);

int mfs_decode_readlink(const u8 *buf, size_t buf_len,
		       char *target, u32 *target_len)
{
	const u8 *ptr = buf;
	u8 status;
	u32 wire_len;
	u32 copy_len;

	if (!buf || !target_len)
		return -EINVAL;
	if (buf_len < 5)
		return -EMSGSIZE;

	(void)mfs_get32bit(&ptr); /* msgid */
	buf_len -= 4;
	if (buf_len == 1) {
		status = mfs_get8bit(&ptr);
		return mfs_status_to_errno(status);
	}
	if (buf_len < 4)
		return -EPROTO;

	wire_len = mfs_get32bit(&ptr);
	buf_len -= 4;
	if (buf_len != wire_len)
		return -EPROTO;

	if (target && *target_len > 0) {
		copy_len = min_t(u32, wire_len, *target_len - 1);
		memcpy(target, ptr, copy_len);
		target[copy_len] = '\0';
	}
	*target_len = wire_len;
	return 0;
}
EXPORT_SYMBOL_GPL(mfs_decode_readlink);

int mfs_decode_xattr(const u8 *buf, size_t buf_len,
		     void *value, u32 *value_len)
{
	const u8 *ptr = buf;
	u8 status;
	u32 wire_len;

	if (!buf || !value_len)
		return -EINVAL;
	if (buf_len < 5)
		return -EMSGSIZE;

	(void)mfs_get32bit(&ptr); /* msgid */
	buf_len -= 4;
	if (buf_len == 1) {
		status = mfs_get8bit(&ptr);
		return mfs_status_to_errno(status);
	}
	if (buf_len < 4)
		return -EPROTO;

	wire_len = mfs_get32bit(&ptr);
	buf_len -= 4;
	if (buf_len != 0 && buf_len != wire_len)
		return -EPROTO;

	if (value && wire_len > 0) {
		if (*value_len < wire_len) {
			*value_len = wire_len;
			return -ERANGE;
		}
		memcpy(value, ptr, wire_len);
	}
	*value_len = wire_len;
	return 0;
}
EXPORT_SYMBOL_GPL(mfs_decode_xattr);
