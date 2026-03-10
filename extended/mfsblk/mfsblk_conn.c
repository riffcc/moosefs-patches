#include <crypto/hash.h>
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

static int mfsblk_md5_buffer(const u8 *data, size_t len, u8 *digest)
{
	struct crypto_shash *tfm;
	struct shash_desc *desc;
	int ret;

	tfm = crypto_alloc_shash("md5", 0, 0);
	if (IS_ERR(tfm))
		return PTR_ERR(tfm);

	desc = kmalloc(sizeof(*desc) + crypto_shash_descsize(tfm), GFP_NOIO);
	if (!desc) {
		crypto_free_shash(tfm);
		return -ENOMEM;
	}
	desc->tfm = tfm;
	ret = crypto_shash_digest(desc, data, len, digest);
	kfree(desc);
	crypto_free_shash(tfm);
	return ret;
}

static int mfsblk_md5_password_digest(const char *password, const u8 challenge[32],
				      u8 digest[16])
{
	u8 passdigest[16];
	u8 material[48];
	int ret;

	ret = mfsblk_md5_buffer((const u8 *)password, strlen(password), passdigest);
	if (ret)
		return ret;

	memcpy(material, challenge, 16);
	memcpy(material + 16, passdigest, 16);
	memcpy(material + 32, challenge + 16, 16);
	return mfsblk_md5_buffer(material, sizeof(material), digest);
}

static u32 mfsblk_crc32(const void *data, u32 len)
{
	return crc32_le(~0U, data, len) ^ ~0U;
}

static int mfsblk_master_write_end_locked(struct mfsblk_dev *dev, u32 chunk_index,
					  const struct mfsblk_chunk_desc *chunk,
					  u32 chunk_offset, u32 size);

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

static void mfsblk_split_session_path(const char *image_path, char *subdir,
				      size_t subdir_sz, const char **lookup_path)
{
	const char *path = image_path;
	const char *first;
	const char *rest;
	size_t sub_len;

	if (!image_path || !*image_path) {
		strscpy(subdir, "/", subdir_sz);
		*lookup_path = "";
		return;
	}

	while (*path == '/')
		path++;
	if (!*path) {
		strscpy(subdir, "/", subdir_sz);
		*lookup_path = "";
		return;
	}

	first = path;
	rest = strchr(first, '/');
	if (!rest) {
		strscpy(subdir, "/", subdir_sz);
		*lookup_path = first;
		return;
	}

	sub_len = (size_t)(rest - first);
	if (sub_len + 2 > subdir_sz) {
		strscpy(subdir, "/", subdir_sz);
		*lookup_path = path;
		return;
	}

	subdir[0] = '/';
	memcpy(subdir + 1, first, sub_len);
	subdir[sub_len + 1] = '\0';

	while (*rest == '/')
		rest++;
	*lookup_path = rest;
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

	*payload = kvmalloc(*payload_len ?: 1, GFP_NOIO);
	if (!*payload)
		return -ENOMEM;

	if (*payload_len) {
		ret = mfsblk_sock_recvall(sock, *payload, *payload_len);
		if (ret) {
			kvfree(*payload);
			*payload = NULL;
			return ret;
		}
	}

	return 0;
}

static int mfsblk_recv_master_packet(struct socket *sock, u32 *type, u8 **payload,
				     u32 *payload_len)
{
	int ret;

	do {
		ret = mfsblk_recv_packet(sock, type, payload, payload_len);
		if (ret)
			return ret;
		if (*type == 0 && *payload_len == 4) {
			kvfree(*payload);
			*payload = NULL;
			continue;
		}
		return 0;
	} while (1);
}

void mfsblk_conn_close_master(struct mfsblk_dev *dev)
{
	mutex_lock(&dev->master_lock);
	mfsblk_close_socket(&dev->master_sock);
	dev->master_registered = false;
	dev->master_version = 0;
	dev->master_session_id = 0;
	mutex_unlock(&dev->master_lock);
}

static int mfsblk_master_register_locked(struct mfsblk_dev *dev)
{
	u8 req[8 + 128];
	u8 *rsp = NULL;
	u8 challenge[32];
	u8 passdigest[16];
	char register_subdir[MFSBLK_PATH_MAX];
	const char *lookup_path;
	u32 rsp_type;
	u32 rsp_len;
	u32 master_version = 0;
	u32 session_id = 0;
	int req_len;
	int ret;

	if (dev->master_registered)
		return 0;

	mfsblk_split_session_path(dev->image_path, register_subdir,
				  sizeof(register_subdir), &lookup_path);

	if (dev->password[0]) {
		req_len = mfsblk_proto_build_register_getrandom_req(req, sizeof(req));
		if (req_len < 0)
			return req_len;

		ret = mfsblk_sock_sendall(dev->master_sock, req, req_len);
		if (ret) {
			pr_err("mfsblk: register getrandom send failed ret=%d\n", ret);
			return ret;
		}

		ret = mfsblk_recv_master_packet(dev->master_sock, &rsp_type, &rsp, &rsp_len);
		if (ret) {
			pr_err("mfsblk: register getrandom recv failed ret=%d\n", ret);
			return ret;
		}
		if (rsp_type != MFSBLK_MATOCL_FUSE_REGISTER || rsp_len != 32) {
			pr_err("mfsblk: register getrandom unexpected rsp_type=%u rsp_len=%u\n",
			       rsp_type, rsp_len);
			ret = -EPROTO;
			goto out;
		}
		memcpy(challenge, rsp, sizeof(challenge));
		kvfree(rsp);
		rsp = NULL;

		ret = mfsblk_md5_password_digest(dev->password, challenge, passdigest);
		if (ret)
			return ret;
	}

	req_len = mfsblk_proto_build_register_req(req, sizeof(req), register_subdir,
						  dev->password[0] ? passdigest : NULL);
	if (req_len < 0)
		return req_len;

	ret = mfsblk_sock_sendall(dev->master_sock, req, req_len);
	if (ret) {
		pr_err("mfsblk: register send failed ret=%d\n", ret);
		return ret;
	}

	ret = mfsblk_recv_master_packet(dev->master_sock, &rsp_type, &rsp, &rsp_len);
	if (ret) {
		pr_err("mfsblk: register recv failed ret=%d\n", ret);
		return ret;
	}

	if (rsp_type != MFSBLK_MATOCL_FUSE_REGISTER) {
		pr_err("mfsblk: register unexpected rsp_type=%u rsp_len=%u\n",
		       rsp_type, rsp_len);
		ret = -EPROTO;
		goto out;
	}

	ret = mfsblk_proto_parse_register_rsp(rsp, rsp_len, &master_version,
					      &session_id);
	if (ret) {
		if (rsp_len == 1)
			pr_err("mfsblk: register status=%u\n", rsp[0]);
		else
			pr_err("mfsblk: register parse failed ret=%d rsp_len=%u\n",
			       ret, rsp_len);
	}
	if (!ret) {
		dev->master_version = master_version;
		dev->master_session_id = session_id;
		dev->master_registered = true;
	}
out:
	kvfree(rsp);
	return ret;
}

static int mfsblk_master_ensure_connected(struct mfsblk_dev *dev)
{
	int ret;

	if (!dev->master_sock) {
		ret = mfsblk_open_socket(dev->master_ip, dev->master_port,
					 &dev->master_sock);
		if (ret) {
			pr_err("mfsblk: open master socket %s:%u failed ret=%d\n",
			       dev->master_host, dev->master_port, ret);
			return ret;
		}
		dev->master_registered = false;
		dev->master_version = 0;
		dev->master_session_id = 0;
	}

	ret = mfsblk_master_register_locked(dev);
	if (ret) {
		pr_err("mfsblk: master register failed ret=%d\n", ret);
		mfsblk_close_socket(&dev->master_sock);
		dev->master_registered = false;
	}

	return ret;
}

static int mfsblk_master_lookup_path_locked(struct mfsblk_dev *dev, u32 *inode,
					    u64 *size, u32 *parent_inode,
					    char *leaf_name, size_t leaf_name_sz)
{
	u8 *req = NULL;
	u8 *rsp = NULL;
	u8 attr[36];
	char register_subdir[MFSBLK_PATH_MAX];
	const char *lookup_path;
	size_t leaf_len = 0;
	u32 msgid;
	u32 rsp_type;
	u32 rsp_len;
	int req_len;
	int ret;
	size_t req_cap;

	mfsblk_split_session_path(dev->image_path, register_subdir,
				  sizeof(register_subdir), &lookup_path);
	req_cap = 8 + 24 + strlen(lookup_path);

	req = kmalloc(req_cap, GFP_NOIO);
	if (!req)
		return -ENOMEM;

	msgid = (u32)atomic_inc_return(&dev->next_msgid);
	req_len = mfsblk_proto_build_lookup_path_req(req, req_cap, msgid,
						     lookup_path);
	if (req_len < 0) {
		ret = req_len;
		goto out;
	}

	ret = mfsblk_sock_sendall(dev->master_sock, req, req_len);
	if (ret) {
		pr_err("mfsblk: PATH_LOOKUP send failed ret=%d path=%s lookup=%s\n",
		       ret, dev->image_path, lookup_path);
		goto out;
	}

	ret = mfsblk_recv_master_packet(dev->master_sock, &rsp_type, &rsp, &rsp_len);
	if (ret) {
		pr_err("mfsblk: PATH_LOOKUP recv failed ret=%d path=%s lookup=%s\n",
		       ret, dev->image_path, lookup_path);
		goto out;
	}

	if (rsp_type != MFSBLK_MATOCL_PATH_LOOKUP) {
		pr_err("mfsblk: PATH_LOOKUP unexpected rsp_type=%u rsp_len=%u path=%s lookup=%s\n",
		       rsp_type, rsp_len, dev->image_path, lookup_path);
		ret = -EPROTO;
		goto out;
	}

	ret = mfsblk_proto_parse_lookup_path_rsp(rsp, rsp_len, msgid, parent_inode,
						 inode, leaf_name, leaf_name_sz,
						 &leaf_len, attr, sizeof(attr));
	if (ret) {
		pr_err("mfsblk: PATH_LOOKUP parse failed ret=%d rsp_len=%u path=%s lookup=%s\n",
		       ret, rsp_len, dev->image_path, lookup_path);
		goto out;
	}
	if (!ret) {
		*size = (*inode != 0) ? mfsblk_proto_attr_size(attr, sizeof(attr)) : 0;
		if (*inode != 0 && mfsblk_proto_attr_type(attr, sizeof(attr)) != 1)
			ret = -EINVAL;
	}
out:
	kvfree(rsp);
	kfree(req);
	return ret;
}

static int mfsblk_master_simple_lookup_locked(struct mfsblk_dev *dev,
					      const char *name, u32 *inode,
					      u64 *size)
{
	u8 req[8 + 13 + 255];
	u8 *rsp = NULL;
	u8 attr[36];
	u32 rsp_type;
	u32 rsp_len;
	int req_len;
	int ret;

	req_len = mfsblk_proto_build_simple_lookup_req(req, sizeof(req), 1, name);
	if (req_len < 0)
		return req_len;

	ret = mfsblk_sock_sendall(dev->master_sock, req, req_len);
	if (ret) {
		pr_err("mfsblk: SIMPLE_LOOKUP send failed ret=%d name=%s\n",
		       ret, name);
		goto out;
	}

	ret = mfsblk_recv_master_packet(dev->master_sock, &rsp_type, &rsp, &rsp_len);
	if (ret) {
		pr_err("mfsblk: SIMPLE_LOOKUP recv failed ret=%d name=%s\n",
		       ret, name);
		goto out;
	}

	if (rsp_type != MFSBLK_MATOCL_FUSE_LOOKUP) {
		pr_err("mfsblk: SIMPLE_LOOKUP unexpected rsp_type=%u rsp_len=%u name=%s\n",
		       rsp_type, rsp_len, name);
		ret = -EPROTO;
		goto out;
	}

	ret = mfsblk_proto_parse_simple_lookup_rsp(rsp, rsp_len, inode, attr,
						 sizeof(attr));
	if (ret) {
		if (rsp_len == 1)
			pr_err("mfsblk: SIMPLE_LOOKUP status=%u name=%s\n", rsp[0], name);
		else
			pr_err("mfsblk: SIMPLE_LOOKUP parse failed ret=%d rsp_len=%u name=%s\n",
			       ret, rsp_len, name);
		goto out;
	}

	*size = mfsblk_proto_attr_size(attr, sizeof(attr));
	if (mfsblk_proto_attr_type(attr, sizeof(attr)) != 1)
		ret = -EINVAL;

out:
	kvfree(rsp);
	return ret;
}

static int mfsblk_master_create_path_locked(struct mfsblk_dev *dev, u64 size,
					    u32 parent_inode, const char *name,
					    u32 *inode)
{
	u8 *req = NULL;
	u8 *rsp = NULL;
	u8 attr[36];
	u32 msgid;
	u32 rsp_type;
	u32 rsp_len;
	int req_len;
	int ret;
	size_t req_cap = 8 + 25 + strlen(name);

	req = kmalloc(req_cap, GFP_NOIO);
	if (!req)
		return -ENOMEM;

	msgid = (u32)atomic_inc_return(&dev->next_msgid);
	req_len = mfsblk_proto_build_create_path_req(req, req_cap, msgid,
						     parent_inode, name);
	if (req_len < 0) {
		ret = req_len;
		goto out;
	}

	ret = mfsblk_sock_sendall(dev->master_sock, req, req_len);
	if (ret)
		goto out;

	ret = mfsblk_recv_master_packet(dev->master_sock, &rsp_type, &rsp, &rsp_len);
	if (ret)
		goto out;

	if (rsp_type != MFSBLK_MATOCL_FUSE_CREATE) {
		ret = -EPROTO;
		goto out;
	}

	ret = mfsblk_proto_parse_create_path_rsp(rsp, rsp_len, msgid, inode, attr,
						 sizeof(attr));
	if (!ret && mfsblk_proto_attr_type(attr, sizeof(attr)) != 1)
		ret = -EINVAL;
	if (!ret && size)
		dev->size_bytes = size;
out:
	kvfree(rsp);
	kfree(req);
	return ret;
}

static int mfsblk_master_truncate_locked(struct mfsblk_dev *dev, u64 size,
					 u64 *actual_size)
{
	u8 req[8 + 29];
	u8 *rsp = NULL;
	u64 new_size = size;
	u32 msgid;
	u32 rsp_type;
	u32 rsp_len;
	int req_len;
	int ret;
	int attempt;

	msgid = (u32)atomic_inc_return(&dev->next_msgid);
	req_len = mfsblk_proto_build_truncate_req(req, sizeof(req), msgid,
						  dev->inode, size);
	if (req_len < 0)
		return req_len;

	for (attempt = 0; attempt < 2; attempt++) {
		ret = mfsblk_master_ensure_connected(dev);
		if (ret)
			return ret;

		ret = mfsblk_sock_sendall(dev->master_sock, req, req_len);
		if (ret)
			goto out_reset_retry;

		ret = mfsblk_recv_master_packet(dev->master_sock, &rsp_type, &rsp, &rsp_len);
		if (ret)
			goto out_reset_retry;

		if (rsp_type != MFSBLK_MATOCL_FUSE_TRUNCATE) {
			ret = -EPROTO;
			goto out_reset_retry;
		}

		ret = mfsblk_proto_parse_truncate_rsp(rsp, rsp_len, msgid, &new_size);
		kvfree(rsp);
		mfsblk_close_socket(&dev->master_sock);
		dev->master_registered = false;
		dev->master_version = 0;
		dev->master_session_id = 0;
		if (!ret && actual_size)
			*actual_size = new_size;
		return ret;

out_reset_retry:
		kvfree(rsp);
		rsp = NULL;
		mfsblk_close_socket(&dev->master_sock);
		dev->master_registered = false;
		dev->master_version = 0;
		dev->master_session_id = 0;
		if (attempt == 0 &&
		    (ret == -ECONNRESET || ret == -EPIPE || ret == -ENOTCONN))
			continue;
		return ret;
	}

	return -EIO;
}

int mfsblk_conn_resolve_image(struct mfsblk_dev *dev, bool inode_explicit,
			      bool size_explicit)
{
	u32 inode = dev->inode;
	u64 size = 0;
	u32 parent_inode = 0;
	char leaf_name[256];
	char register_subdir[MFSBLK_PATH_MAX];
	const char *lookup_path;
	int ret;

	mfsblk_split_session_path(dev->image_path, register_subdir,
				  sizeof(register_subdir), &lookup_path);

	mutex_lock(&dev->master_lock);
	ret = mfsblk_master_ensure_connected(dev);
	if (ret)
		goto out_log;

	if (inode_explicit && size_explicit) {
		mutex_unlock(&dev->master_lock);
		return 0;
	}

	if (lookup_path[0] != '\0' && strchr(lookup_path, '/') == NULL) {
		strscpy(leaf_name, lookup_path, sizeof(leaf_name));
		parent_inode = 1;
		ret = mfsblk_master_simple_lookup_locked(dev, lookup_path, &inode, &size);
	} else {
		ret = mfsblk_master_lookup_path_locked(dev, &inode, &size, &parent_inode,
						       leaf_name, sizeof(leaf_name));
	}
	if (!ret && inode == 0 && dev->size_bytes) {
		ret = mfsblk_master_create_path_locked(dev, dev->size_bytes,
						       parent_inode, leaf_name,
						       &inode);
		if (!ret)
			size = dev->size_bytes;
	}
	if (!ret && inode)
		dev->inode = inode;
	if (!ret && size_explicit && inode) {
		ret = mfsblk_master_truncate_locked(dev, dev->size_bytes, &size);
		if (ret)
			goto out_reset;
	}
	if (ret && ret != -EPROTO && ret != -EOPNOTSUPP && ret != -ENOTCONN)
		goto out_reset;

	if (!inode_explicit) {
		if (!ret && inode)
			dev->inode = inode;
		else
			dev->inode = mfsblk_proto_path_fallback_inode(dev->image_path);
	}

	if (!size_explicit) {
		if (!ret && size)
			dev->size_bytes = size;
		else if (!dev->size_bytes)
			dev->size_bytes = size;
	}

	if (!dev->size_bytes) {
		ret = -EINVAL;
		goto out_log;
	}

	mutex_unlock(&dev->master_lock);
	return 0;

out_reset:
	mfsblk_close_socket(&dev->master_sock);
	dev->master_registered = false;
	dev->master_version = 0;
	dev->master_session_id = 0;
out_log:
	if (ret)
		pr_err("mfsblk: resolve_image path=%s inode=%u size=%llu ret=%d\n",
		       dev->image_path, inode,
		       (unsigned long long)(size ? size : dev->size_bytes), ret);
out_unlock:
	mutex_unlock(&dev->master_lock);
	return ret;
}

int mfsblk_conn_set_file_size(struct mfsblk_dev *dev, u64 size_bytes,
			      u64 *actual_size)
{
	int ret;

	if (!dev)
		return -ENODEV;

	mutex_lock(&dev->master_lock);
	ret = mfsblk_master_truncate_locked(dev, size_bytes, actual_size);
	mutex_unlock(&dev->master_lock);
	return ret;
}

static int mfsblk_conn_flush_writes_locked(struct mfsblk_dev *dev)
{
	int ret;

	if (!dev->write_active)
		return 0;

	mutex_lock(&dev->master_lock);
	ret = mfsblk_master_write_end_locked(dev,
					     (u32)dev->write_chunk.chunk_index,
					     &dev->write_chunk,
					     dev->write_min_chunk_off,
					     dev->write_max_chunk_end -
					     dev->write_min_chunk_off);
	mutex_unlock(&dev->master_lock);
	if (!ret)
		mfsblk_cache_invalidate_chunk(dev, dev->write_chunk.chunk_index);
	dev->write_active = false;
	memset(&dev->write_chunk, 0, sizeof(dev->write_chunk));
	dev->write_file_size = 0;
	dev->write_min_chunk_off = 0;
	dev->write_max_chunk_end = 0;
	return ret;
}

int mfsblk_conn_get_write_chunk(struct mfsblk_dev *dev, u64 chunk_index,
				struct mfsblk_chunk_desc *out)
{
	struct mfsblk_chunk_desc chunk;
	int ret;

	if (dev->write_active && dev->write_chunk.chunk_index == chunk_index) {
		*out = dev->write_chunk;
		return 0;
	}

	if (dev->write_active) {
		ret = mfsblk_conn_flush_writes_locked(dev);
		if (ret)
			return ret;
	}

	ret = mfsblk_conn_master_fetch_chunk(dev, chunk_index, true, &chunk);
	if (ret)
		return ret;

	chunk.chunk_index = chunk_index;
	dev->write_active = true;
	dev->write_chunk = chunk;
	dev->write_file_size = chunk.file_length;
	dev->write_min_chunk_off = UINT_MAX;
	dev->write_max_chunk_end = 0;
	*out = chunk;
	return 0;
}

void mfsblk_conn_note_written(struct mfsblk_dev *dev, u64 chunk_index,
			      u32 chunk_offset, u32 len)
{
	u64 end = chunk_index * (u64)MFSBLK_CHUNK_SIZE + chunk_offset + len;

	if (!dev->write_active || dev->write_chunk.chunk_index != chunk_index)
		return;

	if (dev->write_min_chunk_off > chunk_offset)
		dev->write_min_chunk_off = chunk_offset;
	if (dev->write_max_chunk_end < chunk_offset + len)
		dev->write_max_chunk_end = chunk_offset + len;
	if (dev->write_file_size < end)
		dev->write_file_size = end;
	if (dev->write_chunk.file_length < end)
		dev->write_chunk.file_length = end;
}

int mfsblk_conn_flush_writes(struct mfsblk_dev *dev)
{
	return mfsblk_conn_flush_writes_locked(dev);
}

static int mfsblk_master_write_end_locked(struct mfsblk_dev *dev, u32 chunk_index,
					  const struct mfsblk_chunk_desc *chunk,
					  u32 chunk_offset, u32 size)
{
	u8 req[8 + 37];
	u8 *rsp = NULL;
	u64 new_length;
	u32 msgid;
	u32 rsp_type;
	u32 rsp_len;
	int req_len;
	int ret;
	int attempt;

	new_length = max_t(u64, chunk->file_length,
			   chunk_index * (u64)MFSBLK_CHUNK_SIZE +
			   chunk_offset + size);
	msgid = (u32)atomic_inc_return(&dev->next_msgid);

	req_len = mfsblk_proto_build_master_write_end_req(req, sizeof(req),
							  dev->master_version,
							  msgid,
							  chunk->chunk_id,
							  dev->inode, chunk_index,
							  new_length,
							  chunk_offset, size);
	if (req_len < 0)
		return req_len;

	for (attempt = 0; attempt < 2; attempt++) {
		ret = mfsblk_master_ensure_connected(dev);
		if (ret)
			return ret;

		ret = mfsblk_sock_sendall(dev->master_sock, req, req_len);
		if (ret)
			goto out_reset_retry;

		ret = mfsblk_recv_master_packet(dev->master_sock, &rsp_type, &rsp, &rsp_len);
		if (ret)
			goto out_reset_retry;

		if (rsp_type != MFSBLK_MATOCL_FUSE_WRITE_CHUNK_END) {
			ret = -EPROTO;
			goto out_reset_retry;
		}

		ret = mfsblk_proto_parse_master_write_end_rsp(rsp, rsp_len, msgid);
		kvfree(rsp);
		mfsblk_close_socket(&dev->master_sock);
		dev->master_registered = false;
		return ret;

out_reset_retry:
		kvfree(rsp);
		rsp = NULL;
		mfsblk_close_socket(&dev->master_sock);
		dev->master_registered = false;
		dev->master_version = 0;
		dev->master_session_id = 0;
		if (attempt == 0 &&
		    (ret == -ECONNRESET || ret == -EPIPE || ret == -ENOTCONN))
			continue;
		return ret;
	}

	return -EIO;
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
	int attempt;

	if (chunk_index > U32_MAX)
		return -EFBIG;

	msgid = (u32)atomic_inc_return(&dev->next_msgid);
	req_len = mfsblk_proto_build_master_chunk_req(req, sizeof(req), msgid,
					      dev->inode, (u32)chunk_index,
					      write);
	if (req_len < 0)
		return req_len;

	mutex_lock(&dev->master_lock);
	for (attempt = 0; attempt < 2; attempt++) {
		ret = mfsblk_master_ensure_connected(dev);
		if (ret)
			goto out_unlock;

		ret = mfsblk_sock_sendall(dev->master_sock, req, req_len);
		if (ret)
			goto out_reset_retry;

		ret = mfsblk_recv_master_packet(dev->master_sock, &rsp_type, &rsp, &rsp_len);
		if (ret)
			goto out_reset_retry;

		expected = write ? MFSBLK_MATOCL_FUSE_WRITE_CHUNK : MFSBLK_MATOCL_FUSE_READ_CHUNK;
		if (rsp_type != expected) {
			ret = -EPROTO;
			goto out_reset_retry;
		}

		ret = mfsblk_proto_parse_master_chunk_rsp(rsp, rsp_len, msgid, write, out);
		if (ret)
			goto out_reset_retry;

		kvfree(rsp);
		mfsblk_close_socket(&dev->master_sock);
		dev->master_registered = false;
		mutex_unlock(&dev->master_lock);
		return 0;

out_reset_retry:
		kvfree(rsp);
		rsp = NULL;
		mfsblk_close_socket(&dev->master_sock);
		dev->master_registered = false;
		dev->master_version = 0;
		dev->master_session_id = 0;
		if (attempt == 0 &&
		    (ret == -ECONNRESET || ret == -EPIPE || ret == -ENOTCONN))
			continue;
		goto out_unlock;
	}

	ret = -EIO;
out_unlock:
	kvfree(rsp);
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
			kvfree(payload);
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
			if (ret) {
				goto out;
			}
			if (rx_sum < len) {
				ret = -EIO;
				goto out;
			}
			ret = 0;
			goto out;
		}

		ret = -EPROTO;
out:
		kvfree(payload);
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
	for (;;) {
		u8 *payload = NULL;
		u32 type;
		u32 len;
		int ret;

		ret = mfsblk_recv_packet(conn->sock, &type, &payload, &len);
		if (ret)
			return ret;

		if (type != MFSBLK_CSTOCL_WRITE_STATUS) {
			kvfree(payload);
			continue;
		}

		ret = mfsblk_proto_parse_cs_write_status(payload, len, st);
		kvfree(payload);
		if (ret)
			return ret;

		return mfsblk_proto_status_to_errno(st->status);
	}
}

static int mfsblk_cs_write_one(struct mfsblk_cs_conn *conn,
			       const struct mfsblk_chunk_desc *chunk,
			       u32 start_server, u32 chunk_offset,
			       const void *src, u32 len)
{
	u8 *pkt;
	u32 write_id = 1;
	u32 done = 0;
	int pkt_len;
	int ret;
	struct mfsblk_cs_write_status st;

	pkt = kvmalloc(MFSBLK_MAX_PACKET, GFP_NOIO);
	if (!pkt)
		return -ENOMEM;

	pkt_len = mfsblk_proto_build_cs_write_init(pkt, MFSBLK_MAX_PACKET,
					   chunk->chunk_id, chunk->version,
					   chunk->servers + start_server + 1,
					   chunk->server_count - start_server - 1);
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
		u32 crc = mfsblk_crc32((const u8 *)src + done, part);

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

	/*
	 * Chunkservers may tear down the write socket after WRITE_FINISH.
	 * Drop the cached connection so the next write starts with a fresh
	 * session instead of racing a remote close into -ECONNRESET.
	 */
	mfsblk_close_socket(&conn->sock);
	ret = 0;
out:
	kvfree(pkt);
	return ret;
}

int mfsblk_conn_chunk_write(struct mfsblk_dev *dev,
			    const struct mfsblk_chunk_desc *chunk, u32 chunk_offset,
			    const void *src, u32 len)
{
	u8 i;
	int ret = -ENODEV;
	u32 chunk_index = chunk->chunk_index;

	if (chunk->split_parts)
		return -EOPNOTSUPP;

	for (i = 0; i < chunk->server_count; i++) {
		struct mfsblk_cs_conn *conn = NULL;

		ret = mfsblk_cache_get_conn(dev, chunk->servers[i].ip,
					    chunk->servers[i].port, &conn);
		if (ret)
			continue;

		mutex_lock(&conn->io_lock);
		ret = mfsblk_cs_write_one(conn, chunk, i, chunk_offset, src, len);
		if (ret)
			mfsblk_close_socket(&conn->sock);
		mutex_unlock(&conn->io_lock);
		mfsblk_cache_put_conn(conn);

		if (!ret) {
			mutex_lock(&dev->master_lock);
			ret = mfsblk_master_write_end_locked(dev, chunk_index,
							     chunk, chunk_offset,
							     len);
			mutex_unlock(&dev->master_lock);
			if (ret) {
				mfsblk_cache_invalidate_chunk(dev, chunk_index);
				return ret;
			}
			mfsblk_cache_invalidate_chunk(dev, chunk_index);
			return 0;
		}
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
