#include <linux/errno.h>
#include <linux/hash.h>
#include <linux/in.h>
#include <linux/init.h>
#include <linux/jiffies.h>
#include <linux/kernel.h>
#include <linux/net.h>
#include <linux/slab.h>
#include <linux/socket.h>
#include <linux/tcp.h>
#include <net/net_namespace.h>
#include <net/sock.h>

#include "mfsblk.h"

#define MFSBLK_CONN_IDLE_TIMEOUT (30 * HZ)

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

void mfsblk_cache_init(struct mfsblk_dev *dev)
{
	hash_init(dev->chunk_cache);
	INIT_LIST_HEAD(&dev->conn_pool);
}

void mfsblk_cache_cleanup(struct mfsblk_dev *dev)
{
	struct mfsblk_chunk_cache_entry *entry;
	struct hlist_node *tmp;
	struct mfsblk_cs_conn *conn;
	struct mfsblk_cs_conn *conn_tmp;
	int bkt;

	mutex_lock(&dev->cache_lock);
	hash_for_each_safe(dev->chunk_cache, bkt, tmp, entry, node) {
		hash_del(&entry->node);
		kfree(entry);
	}
	mutex_unlock(&dev->cache_lock);

	mutex_lock(&dev->conn_lock);
	list_for_each_entry_safe(conn, conn_tmp, &dev->conn_pool, link) {
		list_del(&conn->link);
		mfsblk_close_socket(&conn->sock);
		kfree(conn);
	}
	mutex_unlock(&dev->conn_lock);
}

static struct mfsblk_chunk_cache_entry *
mfsblk_cache_find_locked(struct mfsblk_dev *dev, u64 chunk_index)
{
	struct mfsblk_chunk_cache_entry *entry;

	hash_for_each_possible(dev->chunk_cache, entry, node,
			      hash_64(chunk_index, MFSBLK_CACHE_BITS)) {
		if (entry->chunk_index == chunk_index)
			return entry;
	}
	return NULL;
}

void mfsblk_cache_invalidate_chunk(struct mfsblk_dev *dev, u64 chunk_index)
{
	struct mfsblk_chunk_cache_entry *entry;

	mutex_lock(&dev->cache_lock);
	entry = mfsblk_cache_find_locked(dev, chunk_index);
	if (entry) {
		hash_del(&entry->node);
		kfree(entry);
	}
	mutex_unlock(&dev->cache_lock);
}

int mfsblk_cache_get_chunk(struct mfsblk_dev *dev, u64 chunk_index, bool write,
			 struct mfsblk_chunk_desc *out)
{
	struct mfsblk_chunk_cache_entry *entry;
	struct mfsblk_chunk_desc fetched;
	int ret;

	if (!write) {
		mutex_lock(&dev->cache_lock);
		entry = mfsblk_cache_find_locked(dev, chunk_index);
		if (entry && time_before(jiffies, entry->expires)) {
			*out = entry->desc;
			mutex_unlock(&dev->cache_lock);
			return 0;
		}
		mutex_unlock(&dev->cache_lock);
	}

	ret = mfsblk_conn_master_fetch_chunk(dev, chunk_index, write, &fetched);
	if (ret)
		return ret;

	fetched.chunk_index = chunk_index;
	*out = fetched;

	mutex_lock(&dev->cache_lock);
	entry = mfsblk_cache_find_locked(dev, chunk_index);
	if (!entry) {
		entry = kzalloc(sizeof(*entry), GFP_NOIO);
		if (!entry) {
			mutex_unlock(&dev->cache_lock);
			return -ENOMEM;
		}
		entry->chunk_index = chunk_index;
		hash_add(dev->chunk_cache, &entry->node,
			 hash_64(chunk_index, MFSBLK_CACHE_BITS));
	}
	entry->desc = fetched;
	entry->expires = jiffies + MFSBLK_CACHE_TTL;
	mutex_unlock(&dev->cache_lock);

	return 0;
}

static int mfsblk_conn_ensure_open(struct mfsblk_cs_conn *conn)
{
	int ret;

	if (conn->sock)
		return 0;

	ret = mfsblk_open_socket(conn->ip, conn->port, &conn->sock);
	if (ret)
		return ret;

	conn->last_used = jiffies;
	return 0;
}

int mfsblk_cache_get_conn(struct mfsblk_dev *dev, u32 ip, u16 port,
			 struct mfsblk_cs_conn **out)
{
	struct mfsblk_cs_conn *conn;
	int ret;

	mutex_lock(&dev->conn_lock);
	list_for_each_entry(conn, &dev->conn_pool, link) {
		if (conn->ip == ip && conn->port == port) {
			if (time_after(jiffies, conn->last_used + MFSBLK_CONN_IDLE_TIMEOUT)) {
				mfsblk_close_socket(&conn->sock);
			}
			ret = mfsblk_conn_ensure_open(conn);
			if (ret) {
				mutex_unlock(&dev->conn_lock);
				return ret;
			}
			*out = conn;
			mutex_unlock(&dev->conn_lock);
			return 0;
		}
	}

	conn = kzalloc(sizeof(*conn), GFP_NOIO);
	if (!conn) {
		mutex_unlock(&dev->conn_lock);
		return -ENOMEM;
	}

	conn->ip = ip;
	conn->port = port;
	mutex_init(&conn->io_lock);
	ret = mfsblk_conn_ensure_open(conn);
	if (ret) {
		kfree(conn);
		mutex_unlock(&dev->conn_lock);
		return ret;
	}

	list_add_tail(&conn->link, &dev->conn_pool);
	*out = conn;
	mutex_unlock(&dev->conn_lock);
	return 0;
}

void mfsblk_cache_put_conn(struct mfsblk_cs_conn *conn)
{
	if (!conn)
		return;
	conn->last_used = jiffies;
}
