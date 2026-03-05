#include <linux/crc32.h>
#include <linux/errno.h>
#include <linux/in.h>
#include <linux/kernel.h>
#include <linux/minmax.h>
#include <linux/net.h>
#include <linux/slab.h>
#include <linux/tcp.h>
#include <net/net_namespace.h>
#include <net/sock.h>
#include <linux/socket.h>
/*
 * Kernel 6.8 moved unaligned helpers from linux/unaligned/ sub-dir
 * to a single <linux/unaligned.h>.  Use __has_include to pick the
 * right one; fall back to asm/unaligned.h for older kernels.
 */
#if __has_include(<linux/unaligned.h>)
#include <linux/unaligned.h>
#elif __has_include(<asm/unaligned.h>)
#include <asm/unaligned.h>
#else
#include <linux/unaligned/be_byteshift.h>
#endif

#include "mfsblk.h"

static void mfsblk_close_socket(struct socket **psock)
{
	if (*psock) {
		kernel_sock_shutdown(*psock, SHUT_RDWR);
		sock_release(*psock);
		*psock = NULL;
	}
}

static int mfsblk_open_socket(u32 ip, u16 port, struct socket **out)
{
	struct socket *sock;
	struct sockaddr_in sin = {
		.sin_family = AF_INET,
		.sin_addr.s_addr = htonl(ip),
		.sin_port = htons(port),
	};
	int ret;

	ret = sock_create_kern(&init_net, AF_INET, SOCK_STREAM, IPPROTO_TCP, &sock);
	if (ret)
		return ret;

	/* kernel_setsockopt() was removed in 5.9; use direct sock helpers */
	sock_set_keepalive(sock->sk);
	ret = kernel_connect(sock, (struct sockaddr *)&sin, sizeof(sin), 0);
	if (ret) {
		sock_release(sock);
		return ret;
	}

	*out = sock;
	return 0;
}

static int mfsblk_sock_sendall(struct socket *sock, const void *buf, size_t len)
{
	struct msghdr msg = {};
	struct kvec iov;
	size_t done = 0;
	int ret;

	while (done < len) {
		iov.iov_base = (void *)((const u8 *)buf + done);
		iov.iov_len = len - done;
		ret = kernel_sendmsg(sock, &msg, &iov, 1, iov.iov_len);
		if (ret <= 0)
			return ret ? ret : -ECONNRESET;
		done += ret;
	}

	return 0;
}

static int mfsblk_sock_recvall(struct socket *sock, void *buf, size_t len)
{
	struct msghdr msg = {};
	struct kvec iov;
	size_t done = 0;
	int ret;

	while (done < len) {
		iov.iov_base = (u8 *)buf + done;
		iov.iov_len = len - done;
		ret = kernel_recvmsg(sock, &msg, &iov, 1, iov.iov_len, 0);
		if (ret <= 0)
			return ret ? ret : -ECONNRESET;
		done += ret;
	}

	return 0;
}

static int mfsblk_recv_packet(struct socket *sock, u32 *type, u8 **payload,
			      u32 *payload_len)
{
	u8 hdr[8];
	int ret;

	ret = mfsblk_sock_recvall(sock, hdr, sizeof(hdr));
	if (ret)
		return ret;

	mfsblk_proto_parse_header(hdr, type, payload_len);
	if (*payload_len > (MFSBLK_MAX_PACKET - 8))
		return -EMSGSIZE;

	*payload = kmalloc(*payload_len ?: 1, GFP_NOIO);
	if (!*payload)
		return -ENOMEM;

	if (*payload_len) {
		ret = mfsblk_sock_recvall(sock, *payload, *payload_len);
		if (ret) {
			kfree(*payload);
			*payload = NULL;
			return ret;
		}
	}

	return 0;
}

void mfsblk_conn_close_master(struct mfsblk_dev *dev)
{
	mutex_lock(&dev->master_lock);
	mfsblk_close_socket(&dev->master_sock);
	mutex_unlock(&dev->master_lock);
}

static int mfsblk_master_ensure_connected(struct mfsblk_dev *dev)
{
	if (dev->master_sock)
		return 0;

	return mfsblk_open_socket(dev->master_ip, dev->master_port, &dev->master_sock);
}

int mfsblk_conn_master_fetch_chunk(struct mfsblk_dev *dev, u64 chunk_index,
				   bool write, struct mfsblk_chunk_desc *out)
{
	u8 req[8 + 13];
	u8 *rsp = NULL;
	u32 msgid;
	u32 rsp_type;
	u32 rsp_len;
	u32 expected;
	int req_len;
	int ret;

	if (chunk_index > U32_MAX)
		return -EFBIG;

	msgid = (u32)atomic_inc_return(&dev->next_msgid);
	req_len = mfsblk_proto_build_master_chunk_req(req, sizeof(req), msgid,
					      dev->inode, (u32)chunk_index,
					      write);
	if (req_len < 0)
		return req_len;

	mutex_lock(&dev->master_lock);
	ret = mfsblk_master_ensure_connected(dev);
	if (ret)
		goto out_unlock;

	ret = mfsblk_sock_sendall(dev->master_sock, req, req_len);
	if (ret)
		goto out_reset;

	ret = mfsblk_recv_packet(dev->master_sock, &rsp_type, &rsp, &rsp_len);
	if (ret)
		goto out_reset;

	expected = write ? MFSBLK_MATOCL_FUSE_WRITE_CHUNK : MFSBLK_MATOCL_FUSE_READ_CHUNK;
	if (rsp_type != expected) {
		ret = -EPROTO;
		goto out_reset;
	}

	ret = mfsblk_proto_parse_master_chunk_rsp(rsp, rsp_len, msgid, out);
	if (ret)
		goto out_reset;

	kfree(rsp);
	mutex_unlock(&dev->master_lock);
	return 0;

out_reset:
	mfsblk_close_socket(&dev->master_sock);
out_unlock:
	kfree(rsp);
	mutex_unlock(&dev->master_lock);
	return ret;
}

static int mfsblk_cs_read_one(struct mfsblk_cs_conn *conn,
			      const struct mfsblk_chunk_desc *chunk,
			      u32 chunk_offset, void *dst, u32 len)
{
	u8 req[8 + 21];
	u8 *payload = NULL;
	u8 status = 0;
	int req_len;
	u32 ptype;
	u32 plen;
	u64 rx_sum = 0;
	int ret;

	req_len = mfsblk_proto_build_cs_read_req(req, sizeof(req), chunk->chunk_id,
					 chunk->version, chunk_offset, len);
	if (req_len < 0)
		return req_len;

	ret = mfsblk_sock_sendall(conn->sock, req, req_len);
	if (ret)
		return ret;

	for (;;) {
		struct mfsblk_cs_read_data rd;

		ret = mfsblk_recv_packet(conn->sock, &ptype, &payload, &plen);
		if (ret)
			return ret;

		if (ptype == MFSBLK_CSTOCL_READ_DATA) {
			u32 data_off;
			u32 copy_len;

			ret = mfsblk_proto_parse_cs_read_data(payload, plen, &rd);
			if (ret)
				goto out;
			if (rd.chunk_id != chunk->chunk_id) {
				ret = -EPROTO;
				goto out;
			}

			data_off = (u32)rd.block_num * MFSBLK_BLOCK_SIZE + rd.block_offset;
			if (data_off < chunk_offset || data_off >= chunk_offset + len) {
				ret = -ERANGE;
				goto out;
			}

			copy_len = min_t(u32, rd.size, (chunk_offset + len) - data_off);
			memcpy((u8 *)dst + (data_off - chunk_offset), rd.data, copy_len);
			rx_sum += copy_len;
			kfree(payload);
			payload = NULL;
			continue;
		}

		if (ptype == MFSBLK_CSTOCL_READ_STATUS) {
			if (plen != 9) {
				ret = -EPROTO;
				goto out;
			}
			if (get_unaligned_be64(payload) != chunk->chunk_id) {
				ret = -EPROTO;
				goto out;
			}
			status = payload[8];
			ret = mfsblk_proto_status_to_errno(status);
			if (ret)
				goto out;
			if (rx_sum < len) {
				ret = -EIO;
				goto out;
			}
			ret = 0;
			goto out;
		}

		ret = -EPROTO;
out:
		kfree(payload);
		return ret;
	}
}

int mfsblk_conn_chunk_read(struct mfsblk_dev *dev,
			   const struct mfsblk_chunk_desc *chunk, u32 chunk_offset,
			   void *dst, u32 len)
{
	u8 i;
	int ret = -ENODEV;

	if (chunk->split_parts)
		return -EOPNOTSUPP;

	for (i = 0; i < chunk->server_count; i++) {
		struct mfsblk_cs_conn *conn = NULL;

		ret = mfsblk_cache_get_conn(dev, chunk->servers[i].ip,
					    chunk->servers[i].port, &conn);
		if (ret)
			continue;

		mutex_lock(&conn->io_lock);
		ret = mfsblk_cs_read_one(conn, chunk, chunk_offset, dst, len);
		if (ret)
			mfsblk_close_socket(&conn->sock);
		mutex_unlock(&conn->io_lock);
		mfsblk_cache_put_conn(conn);

		if (!ret)
			return 0;
	}

	return ret;
}

static int mfsblk_cs_recv_write_status(struct mfsblk_cs_conn *conn,
				       struct mfsblk_cs_write_status *st)
{
	u8 *payload = NULL;
	u32 type;
	u32 len;
	int ret;

	ret = mfsblk_recv_packet(conn->sock, &type, &payload, &len);
	if (ret)
		return ret;

	if (type != MFSBLK_CSTOCL_WRITE_STATUS) {
		kfree(payload);
		return -EPROTO;
	}

	ret = mfsblk_proto_parse_cs_write_status(payload, len, st);
	kfree(payload);
	if (ret)
		return ret;

	return mfsblk_proto_status_to_errno(st->status);
}

static int mfsblk_cs_write_one(struct mfsblk_cs_conn *conn,
			       const struct mfsblk_chunk_desc *chunk,
			       u32 chunk_offset, const void *src, u32 len)
{
	u8 *pkt;
	u32 write_id = 1;
	u32 done = 0;
	int pkt_len;
	int ret;
	struct mfsblk_cs_write_status st;

	pkt = kmalloc(MFSBLK_MAX_PACKET, GFP_NOIO);
	if (!pkt)
		return -ENOMEM;

	pkt_len = mfsblk_proto_build_cs_write_init(pkt, MFSBLK_MAX_PACKET,
					   chunk->chunk_id, chunk->version);
	if (pkt_len < 0) {
		ret = pkt_len;
		goto out;
	}

	ret = mfsblk_sock_sendall(conn->sock, pkt, pkt_len);
	if (ret)
		goto out;

	ret = mfsblk_cs_recv_write_status(conn, &st);
	if (ret)
		goto out;
	if (st.chunk_id != chunk->chunk_id) {
		ret = -EPROTO;
		goto out;
	}

	while (done < len) {
		u32 chunk_pos = chunk_offset + done;
		u16 block_num = chunk_pos / MFSBLK_BLOCK_SIZE;
		u16 block_off = chunk_pos % MFSBLK_BLOCK_SIZE;
		u32 part = min_t(u32, len - done, MFSBLK_BLOCK_SIZE - block_off);
		u32 crc = crc32(0, (const u8 *)src + done, part);

		pkt_len = mfsblk_proto_build_cs_write_data(pkt, MFSBLK_MAX_PACKET,
						   chunk->chunk_id, write_id,
						   block_num, block_off,
						   (const u8 *)src + done,
						   part, crc);
		if (pkt_len < 0) {
			ret = pkt_len;
			goto out;
		}

		ret = mfsblk_sock_sendall(conn->sock, pkt, pkt_len);
		if (ret)
			goto out;

		ret = mfsblk_cs_recv_write_status(conn, &st);
		if (ret)
			goto out;
		if (st.chunk_id != chunk->chunk_id || st.write_id != write_id) {
			ret = -EPROTO;
			goto out;
		}

		write_id++;
		done += part;
	}

	pkt_len = mfsblk_proto_build_cs_write_finish(pkt, MFSBLK_MAX_PACKET,
					     chunk->chunk_id, chunk->version);
	if (pkt_len < 0) {
		ret = pkt_len;
		goto out;
	}

	ret = mfsblk_sock_sendall(conn->sock, pkt, pkt_len);
	if (ret)
		goto out;

	ret = mfsblk_cs_recv_write_status(conn, &st);
	if (ret)
		goto out;
	if (st.chunk_id != chunk->chunk_id) {
		ret = -EPROTO;
		goto out;
	}

	ret = 0;
out:
	kfree(pkt);
	return ret;
}

int mfsblk_conn_chunk_write(struct mfsblk_dev *dev,
			    const struct mfsblk_chunk_desc *chunk, u32 chunk_offset,
			    const void *src, u32 len)
{
	u8 i;
	int ret = -ENODEV;

	if (chunk->split_parts)
		return -EOPNOTSUPP;

	for (i = 0; i < chunk->server_count; i++) {
		struct mfsblk_cs_conn *conn = NULL;

		ret = mfsblk_cache_get_conn(dev, chunk->servers[i].ip,
					    chunk->servers[i].port, &conn);
		if (ret)
			continue;

		mutex_lock(&conn->io_lock);
		ret = mfsblk_cs_write_one(conn, chunk, chunk_offset, src, len);
		if (ret)
			mfsblk_close_socket(&conn->sock);
		mutex_unlock(&conn->io_lock);
		mfsblk_cache_put_conn(conn);

		if (!ret)
			return 0;
	}

	return ret;
}

int mfsblk_conn_trim(struct mfsblk_dev *dev, u64 offset, u64 len)
{
	/*
	 * MooseFS supports truncate operations, but generic range hole-punching
	 * requires additional inode/session context not yet wired in this module.
	 * Caller falls back to zero-write discard emulation.
	 */
	(void)dev;
	(void)offset;
	(void)len;
	return -EOPNOTSUPP;
}
