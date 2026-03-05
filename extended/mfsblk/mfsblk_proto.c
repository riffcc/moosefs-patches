#include <linux/errno.h>
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
	case 6:
		return -EINVAL;
	case 8:
		return -ENODATA;
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
				       u32 expected_msgid,
				       struct mfsblk_chunk_desc *out)
{
	const u8 *p = payload;
	size_t rem = payload_sz;
	u32 msgid;
	u32 entry_len;
	u32 i;

	if (mfsblk_check_buf(5, rem))
		return -EMSGSIZE;

	msgid = get_unaligned_be32(p);
	p += 4;
	rem -= 4;
	if (msgid != expected_msgid)
		return -EPROTO;

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
				    u32 version)
{
	u8 *p = packet;

	if (mfsblk_check_buf(8 + 13, packet_sz))
		return -EMSGSIZE;

	mfsblk_proto_build_header(p, MFSBLK_CLTOCS_WRITE, 13);
	p += 8;
	*p++ = 1;
	put_unaligned_be64(chunk_id, p);
	p += 8;
	put_unaligned_be32(version, p);

	return 8 + 13;
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
