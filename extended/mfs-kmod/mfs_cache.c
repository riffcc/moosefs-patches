#include "mfs.h"

static u32 mfs_cache_hash(u32 inode, u32 chunk_index)
{
	u64 key = ((u64)inode << 32) | chunk_index;
	return hash_64(key, MFS_CHUNK_CACHE_BITS);
}

void mfs_cache_init(struct mfs_chunk_cache *cache)
{
	u32 i;

	spin_lock_init(&cache->lock);
	for (i = 0; i < MFS_CHUNK_CACHE_BUCKETS; i++)
		INIT_HLIST_HEAD(&cache->buckets[i]);
}

void mfs_cache_destroy(struct mfs_chunk_cache *cache)
{
	u32 i;
	struct mfs_chunk_cache_entry *entry;
	struct hlist_node *tmp;

	spin_lock(&cache->lock);
	for (i = 0; i < MFS_CHUNK_CACHE_BUCKETS; i++) {
		hlist_for_each_entry_safe(entry, tmp, &cache->buckets[i], hnode) {
			hlist_del(&entry->hnode);
			kfree(entry);
		}
	}
	spin_unlock(&cache->lock);
}

void mfs_cache_purge_inode(struct mfs_chunk_cache *cache, u32 inode)
{
	u32 i;
	struct mfs_chunk_cache_entry *entry;
	struct hlist_node *tmp;

	spin_lock(&cache->lock);
	for (i = 0; i < MFS_CHUNK_CACHE_BUCKETS; i++) {
		hlist_for_each_entry_safe(entry, tmp, &cache->buckets[i], hnode) {
			if (entry->inode == inode) {
				hlist_del(&entry->hnode);
				kfree(entry);
			}
		}
	}
	spin_unlock(&cache->lock);
}

void mfs_cache_store(struct mfs_chunk_cache *cache, u32 inode, u32 chunk_index,
		     u64 chunk_id, u32 chunk_version,
		     const struct mfs_chunk_location *locs, u32 loc_count)
{
	u32 bucket;
	struct mfs_chunk_cache_entry *entry;

	if (!locs || loc_count == 0)
		return;
	if (loc_count > ARRAY_SIZE(entry->locs))
		loc_count = ARRAY_SIZE(entry->locs);

	bucket = mfs_cache_hash(inode, chunk_index);
	spin_lock(&cache->lock);
	hlist_for_each_entry(entry, &cache->buckets[bucket], hnode) {
		if (entry->inode == inode && entry->chunk_index == chunk_index)
			goto update;
	}

	entry = kzalloc(sizeof(*entry), GFP_ATOMIC);
	if (!entry)
		goto out;

	entry->inode = inode;
	entry->chunk_index = chunk_index;
	hlist_add_head(&entry->hnode, &cache->buckets[bucket]);

update:
	entry->chunk_id = chunk_id;
	entry->chunk_version = chunk_version;
	entry->loc_count = loc_count;
	memcpy(entry->locs, locs, sizeof(entry->locs[0]) * loc_count);
	entry->expires = jiffies + MFS_CHUNK_CACHE_TTL;
out:
	spin_unlock(&cache->lock);
}

bool mfs_cache_lookup(struct mfs_chunk_cache *cache, u32 inode, u32 chunk_index,
		      u64 *chunk_id, u32 *chunk_version,
		      struct mfs_chunk_location *locs, u32 *loc_count)
{
	u32 bucket;
	struct mfs_chunk_cache_entry *entry;
	bool found = false;

	if (!chunk_id || !chunk_version || !locs || !loc_count || *loc_count == 0)
		return false;

	bucket = mfs_cache_hash(inode, chunk_index);
	spin_lock(&cache->lock);
	hlist_for_each_entry(entry, &cache->buckets[bucket], hnode) {
		u32 n;

		if (entry->inode != inode || entry->chunk_index != chunk_index)
			continue;
		if (time_after(jiffies, entry->expires)) {
			hlist_del(&entry->hnode);
			kfree(entry);
			break;
		}
		n = min(*loc_count, entry->loc_count);
		*chunk_id = entry->chunk_id;
		*chunk_version = entry->chunk_version;
		*loc_count = n;
		memcpy(locs, entry->locs, sizeof(entry->locs[0]) * n);
		found = true;
		break;
	}
	spin_unlock(&cache->lock);

	return found;
}
