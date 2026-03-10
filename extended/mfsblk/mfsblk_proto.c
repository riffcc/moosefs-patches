#include <linux/ctype.h>
#include <linux/errno.h>
#include <linux/inet.h>
#include <linux/overflow.h>
#include <linux/string.h>
#include <linux/types.h>
#if __has_include(<linux/unaligned.h>)
#include <linux/unaligned.h>
#elif __has_include(<asm/unaligned.h>)
#include <asm/unaligned.h>
#else
#include <linux/unaligned/be_byteshift.h>
#endif

#include "mfsblk.h"

static inline int mfsblk_check_buf(size_t need, size_t have)
{
	if (need > have)
		return -EMSGSIZE;
	return 0;
}

static int mfsblk_parse_ipv4_string(const u8 *src, u8 len, u32 *ip)
{
	u8 bytes[4];
	char host[MFSBLK_HOST_MAX];

	if (len >= sizeof(host))
		return -EINVAL;

	memcpy(host, src, len);
	host[len] = '\0';

	if (!in4_pton(host, -1, bytes, -1, NULL))
		return -EINVAL;

	*ip = ((u32)bytes[0] << 24) |
	      ((u32)bytes[1] << 16) |
	      ((u32)bytes[2] << 8) |
	      (u32)bytes[3];
	return 0;
}

u32 mfsblk_proto_path_fallback_inode(const char *path)
{
	const u8 *p = (const u8 *)path;
	u64 hash = 1469598103934665603ULL;

	while (*p) {
		hash ^= *p++;
		hash *= 1099511628211ULL;
	}

	hash ^= hash >> 32;
	return (u32)(hash ? hash : 1);
}

static inline u32 mfsblk_version_int(u16 maj, u8 mid, u8 min)
{
	return (u32)maj * 0x10000U + (u32)mid * 0x100U +
	       ((maj > 1) ? (u32)min * 2U : (u32)min);
}

static u8 mfsblk_type_from_legacy(u8 legacy)
{
	switch (legacy) {
	case 'f':
		return 1;
	case 'd':
		return 2;
	case 'l':
		return 3;
	case 'q':
		return 4;
	case 'b':
		return 5;
	case 'c':
		return 6;
	case 's':
		return 7;
	case 't':
		return 8;
	default:
		return legacy;
	}
}

int mfsblk_proto_build_register_getrandom_req(u8 *packet, size_t packet_sz)
{
	static const char blob[] =
		"DjI1GAQDULI5d2YjA26ypc3ovkhjvhciTQVx3CS4nYgtBoUcsljiVpsErJENHaw0";
	u8 *p = packet;

	if (mfsblk_check_buf(8 + 65, packet_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, MFSBLK_CLTOMA_FUSE_REGISTER, 65);
	p += 8;
	memcpy(p, blob, 64);
	p += 64;
	*p = 1;

	return 8 + 65;
}

int mfsblk_proto_build_register_req(u8 *packet, size_t packet_sz,
				    const char *subdir, const u8 *passdigest)
{
	static const char blob[] =
		"DjI1GAQDULI5d2YjA26ypc3ovkhjvhciTQVx3CS4nYgtBoUcsljiVpsErJENHaw0";
	static const char info[] = "mfsblk";
	const char *path = subdir ? subdir : "/";
	size_t info_len = sizeof(info);
	size_t path_len = strlen(path) + 1;
	size_t extra = passdigest ? 16 : 0;
	u8 *p = packet;

	if (path_len > U32_MAX)
		return -EINVAL;
	if (mfsblk_check_buf(8 + 77 + info_len + path_len + extra, packet_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, MFSBLK_CLTOMA_FUSE_REGISTER,
				  77 + info_len + path_len + extra);
	p += 8;
	memcpy(p, blob, 64);
	p += 64;
	*p++ = MFSBLK_REGISTER_NEWSESSION;
	put_unaligned_be16(4, p);
	p += 2;
	*p++ = 58;
	*p++ = 3;
	put_unaligned_be32((u32)info_len, p);
	p += 4;
	memcpy(p, info, info_len);
	p += info_len;
	put_unaligned_be32((u32)path_len, p);
	p += 4;
	memcpy(p, path, path_len);
	p += path_len;
	if (passdigest)
		memcpy(p, passdigest, 16);

	return 8 + 77 + info_len + path_len + extra;
}

u8 mfsblk_proto_attr_type(const u8 *attr, size_t attr_len)
{
	u8 first;
	u16 mode_word;

	if (!attr || attr_len < 3)
		return 0;

	first = attr[0];
	if (first < 64) {
		mode_word = get_unaligned_be16(attr + 1);
		return (mode_word >> 12) & 0x0F;
	}

	return mfsblk_type_from_legacy(first & 0x7F);
}

u64 mfsblk_proto_attr_size(const u8 *attr, size_t attr_len)
{
	if (!attr || attr_len < 35)
		return 0;
	return get_unaligned_be64(attr + 27);
}

int mfsblk_proto_parse_register_rsp(const u8 *payload, size_t payload_sz,
				    u32 *master_version, u32 *session_id)
{
	const u8 *p = payload;

	if (payload_sz == 1)
		return mfsblk_proto_status_to_errno(payload[0]);
	if (payload_sz != 35 && payload_sz != 43 && payload_sz != 45 &&
	    payload_sz != 49 && payload_sz != 57)
		return -EPROTO;

	if (payload_sz >= 4) {
		if (master_version)
			*master_version = mfsblk_version_int(get_unaligned_be16(p),
						      p[2], p[3]);
		p += 4;
	}

	if (payload_sz >= 8 && session_id)
		*session_id = get_unaligned_be32(p);

	return 0;
}

int mfsblk_proto_build_lookup_path_req(u8 *packet, size_t packet_sz, u32 msgid,
				       const char *path)
{
	size_t path_len = strlen(path);
	u8 *p = packet;

	(void)msgid;
	if (path_len > U32_MAX)
		return -EINVAL;
	if (mfsblk_check_buf(8 + 16 + path_len, packet_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, MFSBLK_CLTOMA_PATH_LOOKUP, 16 + path_len);
	p += 8;
	put_unaligned_be32(1, p); /* root inode */
	p += 4;
	put_unaligned_be32((u32)path_len, p);
	p += 4;
	memcpy(p, path, path_len);
	p += path_len;
	put_unaligned_be32(0, p); /* uid=0 */
	p += 4;
	put_unaligned_be32(0xFFFFFFFFU, p); /* no gids */

	return 8 + 16 + path_len;
}

int mfsblk_proto_build_simple_lookup_req(u8 *packet, size_t packet_sz,
					 u32 parent_inode, const char *name)
{
	size_t name_len = strlen(name);
	u8 *p = packet;

	if (!name_len || name_len > 255)
		return -EINVAL;
	if (mfsblk_check_buf(8 + 13 + name_len, packet_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, MFSBLK_CLTOMA_FUSE_LOOKUP, 13 + name_len);
	p += 8;
	put_unaligned_be32(parent_inode, p);
	p += 4;
	*p++ = (u8)name_len;
	memcpy(p, name, name_len);
	p += name_len;
	put_unaligned_be32(0, p); /* uid=0 */
	p += 4;
	put_unaligned_be32(0xFFFFFFFFU, p); /* no gids */

	return 8 + 13 + name_len;
}

int mfsblk_proto_parse_lookup_path_rsp(const u8 *payload, size_t payload_sz,
				       u32 expected_msgid, u32 *parent_inode,
				       u32 *inode, char *name, size_t name_sz,
				       size_t *name_len, u8 *attr,
				       size_t attr_sz)
{
	const u8 *p = payload;
	size_t rem;
	u8 nlen;
	size_t reply_attr_sz;

	if (!parent_inode || !inode || !name || !name_len || !attr)
		return -EINVAL;
	if (payload_sz < 1)
		return -EPROTO;
	(void)expected_msgid;
	rem = payload_sz;

	if (rem == 1)
		return mfsblk_proto_status_to_errno(*p);
	if (rem < 9 + 35)
		return -EPROTO;

	*parent_inode = get_unaligned_be32(p);
	p += 4;
	nlen = *p++;
	rem -= 5;
	if (nlen >= name_sz || rem < (size_t)nlen + 4 + 35)
		return -EPROTO;
	memcpy(name, p, nlen);
	name[nlen] = '\0';
	*name_len = nlen;
	p += nlen;
	rem -= nlen;
	*inode = get_unaligned_be32(p);
	p += 4;
	rem -= 4;
	reply_attr_sz = (rem >= 36) ? 36 : 35;
	if (reply_attr_sz > attr_sz)
		return -ENOSPC;
	memcpy(attr, p, reply_attr_sz);

	return 0;
}

int mfsblk_proto_parse_simple_lookup_rsp(const u8 *payload, size_t payload_sz,
					 u32 *inode, u8 *attr, size_t attr_sz)
{
	size_t reply_attr_sz;

	if (!inode || !attr)
		return -EINVAL;
	if (payload_sz < 1)
		return -EPROTO;
	if (payload_sz == 1)
		return mfsblk_proto_status_to_errno(payload[0]);

	if (payload_sz < 4 + 35)
		return -EPROTO;

	*inode = get_unaligned_be32(payload);
	reply_attr_sz = (payload_sz >= 4 + 36) ? 36 : 35;
	if (reply_attr_sz > attr_sz)
		return -ENOSPC;
	memcpy(attr, payload + 4, reply_attr_sz);
	return 0;
}

int mfsblk_proto_parse_create_path_rsp(const u8 *payload, size_t payload_sz,
				       u32 expected_msgid, u32 *inode, u8 *attr,
				       size_t attr_sz)
{
	const u8 *p = payload;
	size_t rem;
	size_t reply_attr_sz;

	if (!inode || !attr)
		return -EINVAL;
	if (payload_sz < 5)
		return -EPROTO;
	if (get_unaligned_be32(p) != expected_msgid)
		return -EPROTO;
	p += 4;
	rem = payload_sz - 4;

	if (rem == 1)
		return mfsblk_proto_status_to_errno(*p);
	if (rem < 4 + 35)
		return -EPROTO;

	*inode = get_unaligned_be32(p);
	p += 4;
	rem -= 4;
	reply_attr_sz = (rem >= 36) ? 36 : 35;
	if (reply_attr_sz > attr_sz)
		return -ENOSPC;
	memcpy(attr, p, reply_attr_sz);
	return 0;
}

int mfsblk_proto_build_truncate_req(u8 *packet, size_t packet_sz, u32 msgid,
				    u32 inode, u64 length)
{
	u8 *p = packet;

	if (mfsblk_check_buf(8 + 29, packet_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, MFSBLK_CLTOMA_FUSE_TRUNCATE, 29);
	p += 8;
	put_unaligned_be32(msgid, p);
	p += 4;
	put_unaligned_be32(inode, p);
	p += 4;
	*p++ = 0; /* flags */
	put_unaligned_be32(0, p); /* uid */
	p += 4;
	put_unaligned_be32(1, p); /* gids */
	p += 4;
	put_unaligned_be32(0, p); /* gid[0] */
	p += 4;
	put_unaligned_be64(length, p);

	return 8 + 29;
}

int mfsblk_proto_parse_truncate_rsp(const u8 *payload, size_t payload_sz,
				    u32 expected_msgid, u64 *length)
{
	const u8 *p = payload;

	if (payload_sz < 5)
		return -EPROTO;
	if (get_unaligned_be32(p) != expected_msgid)
		return -EPROTO;
	p += 4;
	payload_sz -= 4;

	if (payload_sz == 1)
		return mfsblk_proto_status_to_errno(*p);

	if (payload_sz >= 8 + 35) {
		if (length)
			*length = mfsblk_proto_attr_size(p + 8, payload_sz - 8);
		return 0;
	}

	if (payload_sz >= 35) {
		if (length)
			*length = mfsblk_proto_attr_size(p, payload_sz);
		return 0;
	}

	return -EPROTO;
}

int mfsblk_proto_build_create_path_req(u8 *packet, size_t packet_sz, u32 msgid,
				       u32 parent_inode, const char *name)
{
	size_t name_len = strlen(name);
	u8 *p = packet;

	if (name_len == 0 || name_len > U8_MAX)
		return -EINVAL;
	if (mfsblk_check_buf(8 + 25 + name_len, packet_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, MFSBLK_CLTOMA_FUSE_CREATE, 25 + name_len);
	p += 8;
	put_unaligned_be32(msgid, p);
	p += 4;
	put_unaligned_be32(parent_inode, p);
	p += 4;
	*p++ = (u8)name_len;
	memcpy(p, name, name_len);
	p += name_len;
	put_unaligned_be16(0600, p);
	p += 2;
	put_unaligned_be16(0, p);
	p += 2;
	put_unaligned_be32(0, p);
	p += 4;
	put_unaligned_be32(1, p);
	p += 4;
	put_unaligned_be32(0, p);

	return 8 + 25 + name_len;
}

void mfsblk_proto_build_header(u8 *hdr, u32 type, u32 len)
{
	put_unaligned_be32(type, hdr);
	put_unaligned_be32(len, hdr + 4);
}

void mfsblk_proto_parse_header(const u8 *hdr, u32 *type, u32 *len)
{
	*type = get_unaligned_be32(hdr);
	*len = get_unaligned_be32(hdr + 4);
}

int mfsblk_proto_status_to_errno(u8 status)
{
	switch (status) {
	case 0:
		return 0;
	case 1:
		return -EPERM;
	case 2:
		return -ENOTDIR;
	case 3:
		return -ENOENT;
	case 4:
		return -EACCES;
	case 5:
		return -EEXIST;
	case 6:
		return -EINVAL;
	case 7:
		return -ENOTEMPTY;
	case 8:
		return -ENODATA;
	case 9:
		return -ENOMEM;
	case 10:
		return -EFBIG;
	case 12:
		return -ENOSPC;
	case 14:
		return -EBUSY;
	case 22:
		return -EIO;
	case 25:
		return -ERANGE;
	case 26:
		return -ENOTCONN;
	case 29:
		return -EILSEQ;
	case 33:
		return -EROFS;
	case 43:
		return -ENODEV;
	case 45:
		return -EAGAIN;
	case 60:
		return -ETIMEDOUT;
	default:
		return -EIO;
	}
}

static int mfsblk_proto_parse_replicas(const u8 *p, size_t rem,
				       struct mfsblk_chunk_desc *out)
{
	u32 i;
	u32 count;
	size_t pos = 0;

	if (rem >= 2) {
		count = get_unaligned_be16(p);
		pos += 2;
	} else if (rem >= 1) {
		count = p[0];
		pos += 1;
	} else {
		count = 0;
	}

	out->server_count = 0;

	for (i = 0; i < count && out->server_count < MFSBLK_MAX_SERVERS; i++) {
		struct mfsblk_server *srv = &out->servers[out->server_count];

		if (pos >= rem)
			return -EPROTO;

		if (pos + 1 <= rem) {
			u8 host_len = p[pos];

			if (host_len > 0 && pos + 1 + host_len + 2 <= rem) {
				int ret = mfsblk_parse_ipv4_string(p + pos + 1, host_len,
								   &srv->ip);
				if (!ret) {
					pos += 1 + host_len;
					srv->port = get_unaligned_be16(p + pos);
					pos += 2;
					out->server_count++;
					continue;
				}
			}
		}

		if (pos + 6 > rem)
			return -EPROTO;

		srv->ip = get_unaligned_be32(p + pos);
		pos += 4;
		srv->port = get_unaligned_be16(p + pos);
		pos += 2;
		out->server_count++;
	}

	return out->server_count ? 0 : -ENOENT;
}

static int mfsblk_proto_parse_master_chunk_rsp_v2(const u8 *payload,
						  size_t payload_sz, bool write,
						  struct mfsblk_chunk_desc *out)
{
	const u8 *p = payload;
	size_t rem = payload_sz;
	u8 status = 0;

	memset(out, 0, sizeof(*out));

	if (rem >= 1 && p[0] <= 32) {
		status = *p++;
		rem--;
		if (status)
			return mfsblk_proto_status_to_errno(status);
	}

	if (rem < 12)
		return -EPROTO;

	out->chunk_id = get_unaligned_be64(p);
	p += 8;
	out->version = get_unaligned_be32(p);
	p += 4;
	rem -= 12;

	if (write) {
		if (rem < 8)
			return -EPROTO;
		out->write_id = get_unaligned_be64(p);
		p += 8;
		rem -= 8;
	}

	return mfsblk_proto_parse_replicas(p, rem, out);
}

int mfsblk_proto_build_master_chunk_req(u8 *payload, size_t payload_sz, u32 msgid,
				       u32 inode, u32 chunk_index, bool write)
{
	u8 *p = payload;
	u32 type = write ? MFSBLK_CLTOMA_FUSE_WRITE_CHUNK : MFSBLK_CLTOMA_FUSE_READ_CHUNK;

	if (mfsblk_check_buf(8 + 13, payload_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, type, 13);
	p += 8;
	put_unaligned_be32(msgid, p);
	p += 4;
	put_unaligned_be32(inode, p);
	p += 4;
	put_unaligned_be32(chunk_index, p);
	p += 4;
	*p = MFSBLK_CHUNKOPFLAG_CANMODTIME;

	return 8 + 13;
}

int mfsblk_proto_parse_master_chunk_rsp(const u8 *payload, size_t payload_sz,
				       u32 expected_msgid, bool write,
				       struct mfsblk_chunk_desc *out)
{
	const u8 *p = payload;
	size_t rem = payload_sz;
	u32 msgid;
	u32 entry_len;
	u32 i;

	if (mfsblk_check_buf(5, rem)) {
		if (rem >= 12)
			return mfsblk_proto_parse_master_chunk_rsp_v2(payload,
							      payload_sz, write,
							      out);
		return -EMSGSIZE;
	}

	msgid = get_unaligned_be32(p);
	if (msgid != expected_msgid) {
		if (payload_sz >= 12)
			return mfsblk_proto_parse_master_chunk_rsp_v2(payload,
							      payload_sz, write,
							      out);
		return -EPROTO;
	}
	p += 4;
	rem -= 4;

	if (rem == 1)
		return mfsblk_proto_status_to_errno(*p);

	memset(out, 0, sizeof(*out));

	if (rem & 1) {
		out->csdata_ver = *p;
		p++;
		rem--;
	} else {
		out->csdata_ver = 0;
	}

	if (mfsblk_check_buf(20, rem))
		return -EPROTO;

	out->file_length = get_unaligned_be64(p);
	p += 8;
	out->chunk_id = get_unaligned_be64(p);
	p += 8;
	out->version = get_unaligned_be32(p);
	p += 4;
	rem -= 20;

	if (out->csdata_ver >= 2)
		entry_len = 14;
	else if (out->csdata_ver == 1)
		entry_len = 10;
	else
		entry_len = 6;

	if (entry_len == 0 || (rem % entry_len))
		return -EPROTO;

	out->server_count = min_t(u32, rem / entry_len, MFSBLK_MAX_SERVERS);
	for (i = 0; i < out->server_count; i++) {
		out->servers[i].ip = get_unaligned_be32(p);
		p += 4;
		out->servers[i].port = get_unaligned_be16(p);
		p += 2;
		if (entry_len >= 10) {
			out->servers[i].cs_ver = get_unaligned_be32(p);
			p += 4;
		}
		if (entry_len >= 14) {
			out->servers[i].label_mask = get_unaligned_be32(p);
			p += 4;
		}
	}

	if (out->csdata_ver == 3 &&
	    (out->server_count == 4 || out->server_count == 8)) {
		out->split_parts = out->server_count;
	}

	return 0;
}

int mfsblk_proto_build_master_write_end_req(u8 *packet, size_t packet_sz,
					    u32 master_version, u32 msgid,
					    u64 chunk_id, u32 inode,
					    u32 chunk_index,
					    u64 length, u32 offset, u32 size)
{
	u8 *p = packet;
	u32 payload_len;

	if (master_version >= (4U * 0x10000U + 40U * 0x100U))
		payload_len = 37;
	else if (master_version >= (3U * 0x10000U + 74U * 0x100U))
		payload_len = 29;
	else if (master_version >= (3U * 0x10000U + 4U * 0x100U))
		payload_len = 25;
	else
		payload_len = 24;

	if (mfsblk_check_buf(8 + payload_len, packet_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, MFSBLK_CLTOMA_FUSE_WRITE_CHUNK_END,
				  payload_len);
	p += 8;
	put_unaligned_be32(msgid, p);
	p += 4;
	put_unaligned_be64(chunk_id, p);
	p += 8;
	put_unaligned_be32(inode, p);
	p += 4;
	if (payload_len >= 29) {
		put_unaligned_be32(chunk_index, p);
		p += 4;
	}
	put_unaligned_be64(length, p);
	p += 8;
	if (payload_len >= 25)
		*p++ = 0;
	if (payload_len >= 37) {
		put_unaligned_be32(offset, p);
		p += 4;
		put_unaligned_be32(size, p);
	}

	return 8 + payload_len;
}

int mfsblk_proto_parse_master_write_end_rsp(const u8 *payload, size_t payload_sz,
					    u32 expected_msgid)
{
	u32 msgid;

	if (payload_sz == 1)
		return mfsblk_proto_status_to_errno(payload[0]);

	if (payload_sz != 5)
		return -EPROTO;

	msgid = get_unaligned_be32(payload);
	if (msgid != expected_msgid)
		return -EPROTO;

	return mfsblk_proto_status_to_errno(payload[4]);
}

int mfsblk_proto_build_cs_read_req(u8 *packet, size_t packet_sz, u64 chunk_id,
				  u32 version, u32 offset, u32 len)
{
	u8 *p = packet;

	if (mfsblk_check_buf(8 + 21, packet_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, MFSBLK_CLTOCS_READ, 21);
	p += 8;
	*p++ = 1;
	put_unaligned_be64(chunk_id, p);
	p += 8;
	put_unaligned_be32(version, p);
	p += 4;
	put_unaligned_be32(offset, p);
	p += 4;
	put_unaligned_be32(len, p);

	return 8 + 21;
}

int mfsblk_proto_parse_cs_read_data(const u8 *payload, size_t payload_sz,
				   struct mfsblk_cs_read_data *out)
{
	if (mfsblk_check_buf(20, payload_sz))
		return -EMSGSIZE;

	out->chunk_id = get_unaligned_be64(payload);
	out->block_num = get_unaligned_be16(payload + 8);
	out->block_offset = get_unaligned_be16(payload + 10);
	out->size = get_unaligned_be32(payload + 12);
	out->crc = get_unaligned_be32(payload + 16);
	if (payload_sz != (size_t)(20 + out->size))
		return -EPROTO;
	out->data = payload + 20;
	return 0;
}

int mfsblk_proto_parse_cs_write_status(const u8 *payload, size_t payload_sz,
				      struct mfsblk_cs_write_status *out)
{
	if (payload_sz != 13)
		return -EPROTO;

	out->chunk_id = get_unaligned_be64(payload);
	out->write_id = get_unaligned_be32(payload + 8);
	out->status = payload[12];
	return 0;
}

int mfsblk_proto_build_cs_write_init(u8 *packet, size_t packet_sz, u64 chunk_id,
				    u32 version,
				    const struct mfsblk_server *servers,
				    u8 server_count)
{
	u8 *p = packet;
	u32 payload_len;
	u8 i;

	payload_len = 13 + (u32)server_count * 6;
	if (mfsblk_check_buf(8 + payload_len, packet_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, MFSBLK_CLTOCS_WRITE, payload_len);
	p += 8;
	*p++ = 1;
	put_unaligned_be64(chunk_id, p);
	p += 8;
	put_unaligned_be32(version, p);
	p += 4;
	for (i = 0; i < server_count; i++) {
		put_unaligned_be32(servers[i].ip, p);
		p += 4;
		put_unaligned_be16(servers[i].port, p);
		p += 2;
	}

	return 8 + payload_len;
}

int mfsblk_proto_build_cs_write_data(u8 *packet, size_t packet_sz, u64 chunk_id,
				    u32 write_id, u16 block_num,
				    u16 block_offset, const void *src,
				    u32 len, u32 crc)
{
	u8 *p = packet;

	if (len > MFSBLK_BLOCK_SIZE)
		return -EINVAL;
	if ((u32)block_offset + len > MFSBLK_BLOCK_SIZE)
		return -ERANGE;
	if (mfsblk_check_buf((size_t)(8 + 24 + len), packet_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, MFSBLK_CLTOCS_WRITE_DATA, 24 + len);
	p += 8;
	put_unaligned_be64(chunk_id, p);
	p += 8;
	put_unaligned_be32(write_id, p);
	p += 4;
	put_unaligned_be16(block_num, p);
	p += 2;
	put_unaligned_be16(block_offset, p);
	p += 2;
	put_unaligned_be32(len, p);
	p += 4;
	put_unaligned_be32(crc, p);
	p += 4;
	memcpy(p, src, len);

	return 8 + 24 + len;
}

int mfsblk_proto_build_cs_write_finish(u8 *packet, size_t packet_sz,
				      u64 chunk_id, u32 version)
{
	u8 *p = packet;

	if (mfsblk_check_buf(8 + 12, packet_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, MFSBLK_CLTOCS_WRITE_FINISH, 12);
	p += 8;
	put_unaligned_be64(chunk_id, p);
	p += 8;
	put_unaligned_be32(version, p);

	return 8 + 12;
}
