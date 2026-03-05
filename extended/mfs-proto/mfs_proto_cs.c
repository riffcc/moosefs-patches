#include <linux/export.h>
#include <linux/mutex.h>
#include <linux/string.h>

#include "mfs_proto.h"

static DEFINE_MUTEX(mfs_crc32_lock);
static bool mfs_crc32_ready;
static u32 mfs_crc32_table[256];

static bool mfs_cs_need(size_t need, size_t have)
{
	return need <= have;
}

static size_t mfs_cs_finalize(u8 *buf, u8 *ptr, u32 type)
{
	u32 payload_len = (u32)(ptr - (buf + sizeof(struct mfs_packet_header)));

	mfs_pack_header(buf, type, payload_len);
	return sizeof(struct mfs_packet_header) + payload_len;
}

static void mfs_crc32_init_locked(void)
{
	u32 i;
	u32 j;
	u32 crc;

	if (mfs_crc32_ready)
		return;

	for (i = 0; i < 256; i++) {
		crc = i;
		for (j = 0; j < 8; j++) {
			if (crc & 1)
				crc = (crc >> 1) ^ 0xEDB88320U;
			else
				crc >>= 1;
		}
		mfs_crc32_table[i] = crc;
	}
	mfs_crc32_ready = true;
}

static void mfs_crc32_ensure(void)
{
	if (likely(mfs_crc32_ready))
		return;

	mutex_lock(&mfs_crc32_lock);
	mfs_crc32_init_locked();
	mutex_unlock(&mfs_crc32_lock);
}

u32 mfs_cs_crc32_block(const u8 *data, u32 size)
{
	u32 crc = ~0U;
	u32 i;

	if (!data)
		return 0;
	if (size > MFSBLOCKSIZE)
		return 0;

	mfs_crc32_ensure();
	for (i = 0; i < size; i++)
		crc = (crc >> 8) ^ mfs_crc32_table[(crc ^ data[i]) & 0xFF];
	return ~crc;
}
EXPORT_SYMBOL_GPL(mfs_cs_crc32_block);

size_t mfs_cs_encode_read(u8 *buf, size_t buf_len, u64 chunk_id,
			  u32 version, u32 offset, u32 size)
{
	u8 *ptr;
	u32 payload_len = 21;

	if (!buf)
		return 0;
	if (!mfs_cs_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put8bit(&ptr, 1); /* protocolid */
	mfs_put64bit(&ptr, chunk_id);
	mfs_put32bit(&ptr, version);
	mfs_put32bit(&ptr, offset);
	mfs_put32bit(&ptr, size);

	return mfs_cs_finalize(buf, ptr, CLTOCS_READ);
}
EXPORT_SYMBOL_GPL(mfs_cs_encode_read);

size_t mfs_cs_encode_write(u8 *buf, size_t buf_len, u64 chunk_id,
			   u32 version, u32 chain_length,
			   const struct mfs_chunk_location *chain_servers)
{
	u8 *ptr;
	u32 i;
	u32 payload_len;

	if (!buf)
		return 0;
	if (chain_length && !chain_servers)
		return 0;

	payload_len = 13 + (chain_length * 6);
	if (!mfs_cs_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put8bit(&ptr, 1); /* protocolid */
	mfs_put64bit(&ptr, chunk_id);
	mfs_put32bit(&ptr, version);
	for (i = 0; i < chain_length; i++) {
		mfs_put32bit(&ptr, chain_servers[i].ip);
		mfs_put16bit(&ptr, chain_servers[i].port);
	}

	return mfs_cs_finalize(buf, ptr, CLTOCS_WRITE);
}
EXPORT_SYMBOL_GPL(mfs_cs_encode_write);

size_t mfs_cs_encode_write_data(u8 *buf, size_t buf_len, u64 chunk_id,
				u32 write_id, u16 block_num,
				u16 offset, u32 size,
				u32 crc, const void *data)
{
	u8 *ptr;
	u32 payload_len;

	if (!buf || !data)
		return 0;
	if (size > MFSBLOCKSIZE)
		return 0;
	if ((u32)offset + size > MFSBLOCKSIZE)
		return 0;

	payload_len = 24 + size;
	if (!mfs_cs_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put64bit(&ptr, chunk_id);
	mfs_put32bit(&ptr, write_id);
	mfs_put16bit(&ptr, block_num);
	mfs_put16bit(&ptr, offset);
	mfs_put32bit(&ptr, size);
	mfs_put32bit(&ptr, crc);
	memcpy(ptr, data, size);
	ptr += size;

	return mfs_cs_finalize(buf, ptr, CLTOCS_WRITE_DATA);
}
EXPORT_SYMBOL_GPL(mfs_cs_encode_write_data);

size_t mfs_cs_encode_write_end(u8 *buf, size_t buf_len)
{
	if (!buf)
		return 0;
	if (!mfs_cs_need(sizeof(struct mfs_packet_header), buf_len))
		return 0;

	mfs_pack_header(buf, CLTOCS_WRITE_FINISH, 0);
	return sizeof(struct mfs_packet_header);
}
EXPORT_SYMBOL_GPL(mfs_cs_encode_write_end);

size_t mfs_cs_encode_write_finish(u8 *buf, size_t buf_len,
				  u64 chunk_id, u32 version)
{
	u8 *ptr;
	u32 payload_len = 12;

	if (!buf)
		return 0;
	if (!mfs_cs_need(sizeof(struct mfs_packet_header) + payload_len, buf_len))
		return 0;

	ptr = buf + sizeof(struct mfs_packet_header);
	mfs_put64bit(&ptr, chunk_id);
	mfs_put32bit(&ptr, version);
	return mfs_cs_finalize(buf, ptr, CLTOCS_WRITE_FINISH);
}
EXPORT_SYMBOL_GPL(mfs_cs_encode_write_finish);

int mfs_cs_decode_read_data(const u8 *buf, size_t buf_len,
			    struct mfs_cs_read_packet *packet,
			    const u8 **data, u32 *size)
{
	const u8 *ptr = buf;
	u32 crc;

	if (!buf || !packet || !data || !size)
		return -EINVAL;
	if (buf_len < 20)
		return -EMSGSIZE;

	packet->chunk_id = mfs_get64bit(&ptr);
	packet->block_num = mfs_get16bit(&ptr);
	packet->block_offset = mfs_get16bit(&ptr);
	packet->size = mfs_get32bit(&ptr);
	packet->crc = mfs_get32bit(&ptr);

	if (buf_len != (size_t)(20 + packet->size))
		return -EPROTO;
	if (packet->size > MFSBLOCKSIZE)
		return -ERANGE;

	crc = mfs_cs_crc32_block(ptr, packet->size);
	if (crc != packet->crc)
		return -EILSEQ;

	*data = ptr;
	*size = packet->size;
	return 0;
}
EXPORT_SYMBOL_GPL(mfs_cs_decode_read_data);

int mfs_cs_decode_write_status(const u8 *buf, size_t buf_len,
			       struct mfs_cs_write_status *status)
{
	const u8 *ptr = buf;

	if (!buf || !status)
		return -EINVAL;
	if (buf_len != 13)
		return -EPROTO;

	status->chunk_id = mfs_get64bit(&ptr);
	status->write_id = mfs_get32bit(&ptr);
	status->status = mfs_get8bit(&ptr);
	return mfs_status_to_errno(status->status);
}
EXPORT_SYMBOL_GPL(mfs_cs_decode_write_status);

u64 mfs_chunk_index_from_offset(u64 offset)
{
	return div_u64(offset, MFSCHUNKSIZE);
}
EXPORT_SYMBOL_GPL(mfs_chunk_index_from_offset);

u32 mfs_chunk_offset_in_chunk(u64 offset)
{
	return (u32)(offset & (MFSCHUNKSIZE - 1U));
}
EXPORT_SYMBOL_GPL(mfs_chunk_offset_in_chunk);

u16 mfs_block_index_from_chunk_offset(u32 chunk_offset)
{
	return (u16)(chunk_offset / MFSBLOCKSIZE);
}
EXPORT_SYMBOL_GPL(mfs_block_index_from_chunk_offset);

u16 mfs_block_offset_in_block(u32 chunk_offset)
{
	return (u16)(chunk_offset & (MFSBLOCKSIZE - 1U));
}
EXPORT_SYMBOL_GPL(mfs_block_offset_in_block);
