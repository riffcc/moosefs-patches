#include "mfs.h"

#include <linux/writeback.h>

#define MFS_READAHEAD_BATCH_PAGES 2048U

/*
 * Kernel API compatibility boundaries for this file:
 *
 *   5.16+   read_folio replaces readpage
 *   6.8+    dirty_folio replaces set_page_dirty / __set_page_dirty_nobuffers
 *           grab_cache_page_write_begin deprecated in favour of
 *           __filemap_get_folio(FGP_WRITEBEGIN); SetPageError removed
 *   6.12+   address_space_operations is folio-native: write_begin /
 *           write_end take const struct kiocb * and writeback uses
 *           writepages instead of writepage
 *           no_llseek removed
 */

static int mfs_call_checked(u16 op, const void *req, u32 req_len,
			    void **rsp, u32 *rsp_len)
{
	s32 status;
	int ret;

	ret = mfs_helper_call(op, req, req_len, rsp, rsp_len, &status,
			      MFS_HELPER_TIMEOUT);
	if (ret) {
		pr_err("mfs: call_checked: op=%u helper_call ret=%d\n", op, ret);
		return ret;
	}
	ret = mfs_moosefs_error_to_errno(status);
	if (ret) {
		pr_err("mfs: call_checked: op=%u status=%d errno=%d\n", op, status, ret);
		kfree(*rsp);
		*rsp = NULL;
		*rsp_len = 0;
	}
	return ret;
}

static int mfs_read_remote(struct inode *inode, loff_t offset, u32 size,
		   void **raw, u32 *raw_len)
{
	struct mfs_ctrl_read_req req = {
		.session_id = MFS_SB(inode->i_sb)->session_id,
		.inode = inode->i_ino,
		.offset = offset,
		.size = size,
		/*
		 * Carry a stable per-task stream hint so the helper can keep
		 * sequential read identity attached to one logical lane instead
		 * of inferring it from offsets alone under fanout.
		 */
		.flags = (u32)task_pid_nr(current),
	};

	return mfs_call_checked(MFS_CTRL_OP_READ, &req, sizeof(req), raw, raw_len);
}

static int mfs_write_remote(struct inode *inode, loff_t offset,
		    const void *data, u32 size,
		    struct mfs_ctrl_write_rsp *wrsp)
{
	struct mfs_ctrl_write_req *req;
	void *rsp = NULL;
	u32 rsp_len = 0;
	u32 req_len;
	int ret;

	req_len = sizeof(*req) + size;
	req = kmalloc(req_len, GFP_KERNEL);
	if (!req)
		return -ENOMEM;

	req->session_id = MFS_SB(inode->i_sb)->session_id;
	req->inode = inode->i_ino;
	req->offset = offset;
	req->size = size;
	req->flags = 0;
	memcpy(req + 1, data, size);

	ret = mfs_call_checked(MFS_CTRL_OP_WRITE, req, req_len, &rsp, &rsp_len);
	kfree(req);
	if (ret)
		return ret;
	if (rsp_len < sizeof(*wrsp)) {
		kfree(rsp);
		return -EIO;
	}
	memcpy(wrsp, rsp, sizeof(*wrsp));
	kfree(rsp);
	return 0;
}

static int mfs_fill_page_from_remote(struct inode *inode, struct page *page)
{
	struct mfs_ctrl_read_rsp *rrsp;
	loff_t off = page_offset(page);
	loff_t isize = i_size_read(inode);
	void *raw = NULL;
	u32 raw_len = 0;
	void *kaddr;
	int ret;

	/*
	 * If the page is entirely beyond the current file size, there is
	 * nothing to read — just zero-fill.  This covers newly-created
	 * files and pages being written past EOF.
	 */
	if (off >= isize) {
		kaddr = kmap_local_page(page);
		memset(kaddr, 0, PAGE_SIZE);
		kunmap_local(kaddr);
		flush_dcache_page(page);
		return 0;
	}

	ret = mfs_read_remote(inode, off, PAGE_SIZE, &raw, &raw_len);
	if (ret) {
		/*
		 * MFS_ERROR_NOCHUNKSERVERS (-ENOSPC) can mean the
		 * chunk simply doesn't exist yet (new file, sparse
		 * region).  Zero-fill instead of propagating the error.
		 */
		if (ret == -ENOSPC) {
			pr_info("mfs: fill_page: NOCHUNKSERVERS for ino=%lu off=%lld, zero-filling\n",
				inode->i_ino, off);
			kaddr = kmap_local_page(page);
			memset(kaddr, 0, PAGE_SIZE);
			kunmap_local(kaddr);
			flush_dcache_page(page);
			return 0;
		}
		return ret;
	}

	if (raw_len < sizeof(*rrsp)) {
		kfree(raw);
		return -EIO;
	}
	rrsp = raw;
	if (rrsp->size > PAGE_SIZE || raw_len < sizeof(*rrsp) + rrsp->size) {
		kfree(raw);
		return -EIO;
	}

	kaddr = kmap_local_page(page);
	memcpy(kaddr, rrsp + 1, rrsp->size);
	if (rrsp->size < PAGE_SIZE)
		memset(kaddr + rrsp->size, 0, PAGE_SIZE - rrsp->size);
	kunmap_local(kaddr);
	flush_dcache_page(page);
	kfree(raw);
	return 0;
}

static int mfs_fill_pages_from_remote(struct inode *inode, struct page **pages,
				      unsigned int nr_pages)
{
	struct mfs_ctrl_read_rsp *rrsp;
	loff_t off;
	loff_t isize = i_size_read(inode);
	u32 want;
	void *raw = NULL;
	u32 raw_len = 0;
	u8 *src;
	u32 copied = 0;
	unsigned int i;
	int ret;

	if (!nr_pages)
		return 0;

	off = page_offset(pages[0]);
	if (off >= isize) {
		for (i = 0; i < nr_pages; i++) {
			void *kaddr = kmap_local_page(pages[i]);
			memset(kaddr, 0, PAGE_SIZE);
			kunmap_local(kaddr);
			flush_dcache_page(pages[i]);
		}
		return 0;
	}

	want = (u32)min_t(loff_t, (loff_t)nr_pages * PAGE_SIZE, isize - off);
	ret = mfs_read_remote(inode, off, want, &raw, &raw_len);
	if (ret) {
		if (ret == -ENOSPC) {
			for (i = 0; i < nr_pages; i++) {
				void *kaddr = kmap_local_page(pages[i]);
				memset(kaddr, 0, PAGE_SIZE);
				kunmap_local(kaddr);
				flush_dcache_page(pages[i]);
			}
			return 0;
		}
		return ret;
	}

	if (raw_len < sizeof(*rrsp)) {
		kfree(raw);
		return -EIO;
	}
	rrsp = raw;
	if (rrsp->size > want || raw_len < sizeof(*rrsp) + rrsp->size) {
		kfree(raw);
		return -EIO;
	}

	src = (u8 *)(rrsp + 1);
	for (i = 0; i < nr_pages; i++) {
		u32 page_bytes = 0;
		void *kaddr = kmap_local_page(pages[i]);

		if (copied < rrsp->size)
			page_bytes = min_t(u32, PAGE_SIZE, rrsp->size - copied);
		if (page_bytes)
			memcpy(kaddr, src + copied, page_bytes);
		if (page_bytes < PAGE_SIZE)
			memset((u8 *)kaddr + page_bytes, 0, PAGE_SIZE - page_bytes);
		kunmap_local(kaddr);
		flush_dcache_page(pages[i]);
		copied += page_bytes;
	}

	kfree(raw);
	return 0;
}

/* ── Read path ───────────────────────────────────────────────── */

static int mfs_read_page_common(struct inode *inode, struct page *page)
{
	int ret = mfs_fill_page_from_remote(inode, page);
	if (ret) {
		/* SetPageError removed in 6.8+; error signalled via ret */
#if LINUX_VERSION_CODE < KERNEL_VERSION(6, 8, 0)
		SetPageError(page);
#endif
		unlock_page(page);
		return ret;
	}

	SetPageUptodate(page);
	unlock_page(page);
	return 0;
}

static void mfs_readahead(struct readahead_control *ractl)
{
	struct page **pages;
	struct inode *inode = ractl->mapping->host;
	unsigned int nr;

	pages = kmalloc_array(MFS_READAHEAD_BATCH_PAGES, sizeof(*pages), GFP_KERNEL);
	if (!pages)
		return;

	while ((nr = __readahead_batch(ractl, pages, MFS_READAHEAD_BATCH_PAGES)) != 0) {
		int ret = mfs_fill_pages_from_remote(inode, pages, nr);
		unsigned int i;

		for (i = 0; i < nr; i++) {
			if (ret) {
#if LINUX_VERSION_CODE < KERNEL_VERSION(6, 8, 0)
				SetPageError(pages[i]);
#endif
			} else {
				SetPageUptodate(pages[i]);
			}
			unlock_page(pages[i]);
		}

		if (ret)
			break;
	}

	kfree(pages);
}

#if LINUX_VERSION_CODE < KERNEL_VERSION(5, 16, 0)
static int mfs_readpage(struct file *file, struct page *page)
{
	(void)file;
	return mfs_read_page_common(page->mapping->host, page);
}
#else
static int mfs_read_folio(struct file *file, struct folio *folio)
{
	(void)file;
	return mfs_read_page_common(folio->mapping->host, &folio->page);
}
#endif

/* ── Write path ──────────────────────────────────────────────── */

struct mfs_pending_write {
	struct list_head list;
	loff_t offset;
	u32 len;
	u8 data[];
};

static struct mfs_pending_write *mfs_take_pending_write(struct mfs_inode_info *mi)
{
	struct mfs_pending_write *pw = NULL;

	mutex_lock(&mi->write_lock);
	if (!list_empty(&mi->write_queue)) {
		pw = list_first_entry(&mi->write_queue, struct mfs_pending_write, list);
		list_del(&pw->list);
	} else {
		mi->write_worker_running = false;
	}
	mutex_unlock(&mi->write_lock);
	return pw;
}

static struct mfs_pending_write *mfs_merge_pending_write(struct mfs_pending_write *base,
							 struct mfs_pending_write *next)
{
	struct mfs_pending_write *merged;

	merged = kmalloc(struct_size(merged, data, base->len + next->len), GFP_KERNEL);
	if (!merged)
		return NULL;

	INIT_LIST_HEAD(&merged->list);
	merged->offset = base->offset;
	merged->len = base->len + next->len;
	memcpy(merged->data, base->data, base->len);
	memcpy(merged->data + base->len, next->data, next->len);
	kfree(base);
	kfree(next);
	return merged;
}

static struct mfs_pending_write *mfs_take_merged_pending_write(struct mfs_inode_info *mi)
{
	struct mfs_pending_write *pw;

	pw = mfs_take_pending_write(mi);
	if (!pw)
		return NULL;

	for (;;) {
		struct mfs_pending_write *next = NULL;

		if (pw->len >= (128U * 1024U))
			break;

		mutex_lock(&mi->write_lock);
		if (!list_empty(&mi->write_queue)) {
			struct mfs_pending_write *cand;

			cand = list_first_entry(&mi->write_queue, struct mfs_pending_write, list);
			if (cand->offset == pw->offset + pw->len &&
			    pw->len + cand->len <= (128U * 1024U)) {
				list_del(&cand->list);
				next = cand;
			}
		}
		mutex_unlock(&mi->write_lock);

		if (!next)
			break;

		next = mfs_merge_pending_write(pw, next);
		if (!next)
			break;
		pw = next;
	}

	return pw;
}

static void mfs_async_write_worker(struct work_struct *work)
{
	struct mfs_inode_info *mi = container_of(work, struct mfs_inode_info, write_work);
	struct inode *inode = &mi->vfs_inode;

	for (;;) {
		struct mfs_pending_write *pw;
		struct mfs_ctrl_write_rsp wrsp;
		int ret;

		pw = mfs_take_merged_pending_write(mi);
		if (!pw) {
			wake_up_all(&mi->write_wait);
			return;
		}

		pr_info("mfs: async_write: ino=%lu off=%lld len=%u\n",
			inode->i_ino, pw->offset, pw->len);
		ret = mfs_write_remote(inode, pw->offset, pw->data, pw->len, &wrsp);
		pr_info("mfs: async_write: ino=%lu off=%lld ret=%d\n",
			inode->i_ino, pw->offset, ret);
		if (!ret && wrsp.attr_valid)
			mfs_wire_attr_to_inode(inode, &wrsp.attr);
		if (ret) {
			mutex_lock(&mi->write_lock);
			if (!mi->write_error)
				mi->write_error = ret;
			mutex_unlock(&mi->write_lock);
		}
		kfree(pw);
		wake_up_all(&mi->write_wait);
	}
}

void mfs_inode_async_init(struct mfs_inode_info *mi)
{
	mutex_init(&mi->write_lock);
	INIT_LIST_HEAD(&mi->write_queue);
	init_waitqueue_head(&mi->write_wait);
	INIT_WORK(&mi->write_work, mfs_async_write_worker);
	mi->write_error = 0;
	mi->write_worker_running = false;
}

void mfs_inode_async_destroy(struct mfs_inode_info *mi)
{
	struct mfs_pending_write *pw;
	struct mfs_pending_write *tmp;

	cancel_work_sync(&mi->write_work);
	mutex_lock(&mi->write_lock);
	list_for_each_entry_safe(pw, tmp, &mi->write_queue, list) {
		list_del(&pw->list);
		kfree(pw);
	}
	mi->write_error = 0;
	mi->write_worker_running = false;
	mutex_unlock(&mi->write_lock);
}

int mfs_inode_async_queue_write(struct inode *inode, loff_t offset,
				const void *data, u32 len)
{
	struct mfs_inode_info *mi = MFS_I(inode);
	struct mfs_pending_write *pw;
	bool need_schedule = false;

	pw = kmalloc(struct_size(pw, data, len), GFP_KERNEL);
	if (!pw)
		return -ENOMEM;

	INIT_LIST_HEAD(&pw->list);
	pw->offset = offset;
	pw->len = len;
	memcpy(pw->data, data, len);

	mutex_lock(&mi->write_lock);
	list_add_tail(&pw->list, &mi->write_queue);
	if (!mi->write_worker_running) {
		mi->write_worker_running = true;
		need_schedule = true;
	}
	mutex_unlock(&mi->write_lock);

	if (need_schedule)
		schedule_work(&mi->write_work);
	return 0;
}

int mfs_inode_async_flush(struct inode *inode)
{
	struct mfs_inode_info *mi = MFS_I(inode);
	int ret;

	flush_work(&mi->write_work);
	mutex_lock(&mi->write_lock);
	ret = mi->write_error;
	mi->write_error = 0;
	mutex_unlock(&mi->write_lock);
	return ret;
}

/*
 * write_begin / write_end compatibility:
 *
 *   < 6.8    grab_cache_page_write_begin (page-based)
 *   6.8+     __filemap_get_folio(FGP_WRITEBEGIN) but signature still page **
 *   6.12+    address_space_operations switched to folio-based
 *            write_begin / write_end callbacks
 *   6.18+    the first parameter changed from struct file * to
 *            const struct kiocb *
 *            address_space_operations uses .writepages, not .writepage
 *
 * We provide three compatibility variants:
 *   < 6.12: page-based callbacks
 *   6.12 - 6.17: folio callbacks with struct file *
 *   6.18+: folio callbacks with const struct kiocb *
 */

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 17, 0)
/* ---- New folio write_begin/write_end signatures for kernel 6.17+ ---- */

static int mfs_write_begin(const struct kiocb *iocb,
			   struct address_space *mapping,
			   loff_t pos, unsigned int len,
			   struct folio **foliop, void **fsdata)
{
	struct folio *folio;
	int ret = 0;

	(void)fsdata;
	(void)iocb;

	pr_info("mfs: write_begin: ino=%lu pos=%lld len=%u\n",
		mapping->host->i_ino, pos, len);

	folio = __filemap_get_folio(mapping, pos >> PAGE_SHIFT,
				    FGP_WRITEBEGIN,
				    mapping_gfp_mask(mapping));
	if (IS_ERR(folio)) {
		pr_err("mfs: write_begin: get_folio failed %ld\n", PTR_ERR(folio));
		return PTR_ERR(folio);
	}

	if (!folio_test_uptodate(folio)) {
		ret = mfs_fill_page_from_remote(mapping->host, &folio->page);
		pr_info("mfs: write_begin: fill_page ret=%d uptodate=%d\n",
			ret, folio_test_uptodate(folio));
		if (ret && (len != PAGE_SIZE || (pos & (PAGE_SIZE - 1)))) {
			folio_unlock(folio);
			folio_put(folio);
			return ret;
		}
	}

	*foliop = folio;
	return 0;
}

static int mfs_write_end(const struct kiocb *iocb,
			 struct address_space *mapping,
			 loff_t pos, unsigned int len, unsigned int copied,
			 struct folio *folio, void *fsdata)
{
	struct inode *inode = mapping->host;
	loff_t end_pos = pos + copied;

	(void)iocb;
	(void)len;
	(void)fsdata;

	pr_info("mfs: write_end: ino=%lu pos=%lld len=%u copied=%u isize=%lld\n",
		inode->i_ino, pos, len, copied, i_size_read(inode));

	if (copied) {
		void *kaddr;
		unsigned int page_off = pos & (PAGE_SIZE - 1);
		int ret;

		if (!folio_test_uptodate(folio))
			folio_mark_uptodate(folio);
		kaddr = kmap_local_folio(folio, 0);
		ret = mfs_inode_async_queue_write(inode, pos,
						  (u8 *)kaddr + page_off, copied);
		kunmap_local(kaddr);
		if (ret) {
			folio_unlock(folio);
			folio_put(folio);
			return ret;
		}
		if (end_pos > i_size_read(inode)) {
			pr_info("mfs: write_end: updating isize %lld -> %lld\n",
				i_size_read(inode), end_pos);
			i_size_write(inode, end_pos);
		}
	}

	folio_unlock(folio);
	folio_put(folio);
	return copied;
}

#elif LINUX_VERSION_CODE >= KERNEL_VERSION(6, 12, 0)
/* ---- Folio callbacks with struct file * for kernel 6.12 - 6.17 ---- */

static int mfs_write_begin(struct file *file, struct address_space *mapping,
			   loff_t pos, unsigned int len,
			   struct folio **foliop, void **fsdata)
{
	struct folio *folio;
	int ret = 0;

	(void)fsdata;
	(void)file;

	pr_info("mfs: write_begin: ino=%lu pos=%lld len=%u\n",
		mapping->host->i_ino, pos, len);

	folio = __filemap_get_folio(mapping, pos >> PAGE_SHIFT,
				    FGP_WRITEBEGIN,
				    mapping_gfp_mask(mapping));
	if (IS_ERR(folio)) {
		pr_err("mfs: write_begin: get_folio failed %ld\n", PTR_ERR(folio));
		return PTR_ERR(folio);
	}

	if (!folio_test_uptodate(folio)) {
		ret = mfs_fill_page_from_remote(mapping->host, &folio->page);
		pr_info("mfs: write_begin: fill_page ret=%d uptodate=%d\n",
			ret, folio_test_uptodate(folio));
		if (ret && (len != PAGE_SIZE || (pos & (PAGE_SIZE - 1)))) {
			folio_unlock(folio);
			folio_put(folio);
			return ret;
		}
	}

	*foliop = folio;
	return 0;
}

static int mfs_write_end(struct file *file, struct address_space *mapping,
			 loff_t pos, unsigned int len, unsigned int copied,
			 struct folio *folio, void *fsdata)
{
	struct inode *inode = mapping->host;
	loff_t end_pos = pos + copied;

	(void)file;
	(void)len;
	(void)fsdata;

	pr_info("mfs: write_end: ino=%lu pos=%lld len=%u copied=%u isize=%lld\n",
		inode->i_ino, pos, len, copied, i_size_read(inode));

	if (copied) {
		void *kaddr;
		unsigned int page_off = pos & (PAGE_SIZE - 1);
		int ret;

		if (!folio_test_uptodate(folio))
			folio_mark_uptodate(folio);
		kaddr = kmap_local_folio(folio, 0);
		ret = mfs_inode_async_queue_write(inode, pos,
						  (u8 *)kaddr + page_off, copied);
		kunmap_local(kaddr);
		if (ret) {
			folio_unlock(folio);
			folio_put(folio);
			return ret;
		}
		if (end_pos > i_size_read(inode)) {
			pr_info("mfs: write_end: updating isize %lld -> %lld\n",
				i_size_read(inode), end_pos);
			i_size_write(inode, end_pos);
		}
	}

	folio_unlock(folio);
	folio_put(folio);
	return copied;
}

#else
/* ---- Page-based callbacks for kernel < 6.12 ---- */

static int mfs_write_begin(struct file *file, struct address_space *mapping,
			   loff_t pos, unsigned int len,
			   struct page **pagep, void **fsdata)
{
	struct page *page;
	int ret = 0;

	(void)fsdata;
	(void)file;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 8, 0)
	{
		struct folio *folio;
		folio = __filemap_get_folio(mapping, pos >> PAGE_SHIFT,
					    FGP_WRITEBEGIN,
					    mapping_gfp_mask(mapping));
		if (IS_ERR(folio))
			return PTR_ERR(folio);
		page = &folio->page;
	}
#else
	page = grab_cache_page_write_begin(mapping, pos >> PAGE_SHIFT, 0);
	if (!page)
		return -ENOMEM;
#endif

	if (!PageUptodate(page)) {
		ret = mfs_fill_page_from_remote(mapping->host, page);
		if (ret && (len != PAGE_SIZE || (pos & (PAGE_SIZE - 1)))) {
			unlock_page(page);
			put_page(page);
			return ret;
		}
	}

	*pagep = page;
	return 0;
}

static int mfs_write_end(struct file *file, struct address_space *mapping,
			 loff_t pos, unsigned int len, unsigned int copied,
			 struct page *page, void *fsdata)
{
	struct inode *inode = mapping->host;
	loff_t end_pos = pos + copied;

	(void)file;
	(void)len;
	(void)fsdata;

	if (copied) {
		void *kaddr;
		unsigned int page_off = pos & (PAGE_SIZE - 1);
		int ret;

		if (!PageUptodate(page))
			SetPageUptodate(page);
		kaddr = kmap_local_page(page);
		ret = mfs_inode_async_queue_write(inode, pos,
						  (u8 *)kaddr + page_off, copied);
		kunmap_local(kaddr);
		if (ret) {
			unlock_page(page);
			put_page(page);
			return ret;
		}
		if (end_pos > i_size_read(inode))
			i_size_write(inode, end_pos);
	}

	unlock_page(page);
	put_page(page);
	return copied;
}

#endif /* KERNEL_VERSION(6, 12, 0) */

/* ── Writeback ───────────────────────────────────────────────── */

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 12, 0)

static int mfs_write_folio(struct folio *folio, struct writeback_control *wbc)
{
	struct inode *inode = folio->mapping->host;
	loff_t off = folio_pos(folio);
	loff_t isize = i_size_read(inode);
	u32 len;
	void *kaddr;
	struct mfs_ctrl_write_rsp wrsp;
	int ret = 0;

	pr_info("mfs: write_folio: ino=%lu off=%lld isize=%lld\n",
		inode->i_ino, off, isize);

	if (off >= isize) {
		pr_info("mfs: write_folio: off >= isize, skipping\n");
		folio_unlock(folio);
		return 0;
	}

	len = min_t(loff_t, folio_size(folio), isize - off);
	pr_info("mfs: write_folio: writing %u bytes at off=%lld\n", len, off);

	folio_start_writeback(folio);
	kaddr = kmap_local_folio(folio, 0);
	ret = mfs_write_remote(inode, off, kaddr, len, &wrsp);
	kunmap_local(kaddr);

	pr_info("mfs: write_folio: write_remote ret=%d\n", ret);
	if (!ret && wrsp.attr_valid)
		mfs_wire_attr_to_inode(inode, &wrsp.attr);
	if (ret)
		folio_redirty_for_writepage(wbc, folio);

	folio_end_writeback(folio);
	folio_unlock(folio);
	return ret;
}

static int mfs_writepages(struct address_space *mapping,
			  struct writeback_control *wbc)
{
	int ret;

	pr_info("mfs: writepages: ino=%lu sync_mode=%d nr_to_write=%ld range=%lld-%lld\n",
		mapping->host->i_ino, wbc->sync_mode, wbc->nr_to_write,
		(long long)wbc->range_start, (long long)wbc->range_end);
	ret = mfs_inode_async_flush(mapping->host);

	pr_info("mfs: writepages: ino=%lu ret=%d iter_err=%d\n",
		mapping->host->i_ino, ret, 0);
	return ret;
}

#else

static int mfs_writepage(struct page *page, struct writeback_control *wbc)
{
	struct inode *inode = page->mapping->host;
	loff_t off = page_offset(page);
	loff_t isize = i_size_read(inode);
	u32 len;
	void *kaddr;
	struct mfs_ctrl_write_rsp wrsp;
	int ret = 0;

	pr_info("mfs: writepage: ino=%lu off=%lld isize=%lld\n",
		inode->i_ino, off, isize);

	if (off >= isize) {
		pr_info("mfs: writepage: off >= isize, skipping\n");
		unlock_page(page);
		return 0;
	}

	len = min_t(loff_t, PAGE_SIZE, isize - off);

	/*
	 * The writeback infrastructure (write_cache_pages) already called
	 * clear_page_dirty_for_io() before invoking us, so we must NOT
	 * call it again — it would return false and we would skip the
	 * write.  Just proceed directly with the I/O.
	 */
	pr_info("mfs: writepage: writing %u bytes at off=%lld\n", len, off);
	set_page_writeback(page);
	kaddr = kmap_local_page(page);
	ret = mfs_write_remote(inode, off, kaddr, len, &wrsp);
	kunmap_local(kaddr);

	pr_info("mfs: writepage: write_remote ret=%d\n", ret);
	if (!ret && wrsp.attr_valid)
		mfs_wire_attr_to_inode(inode, &wrsp.attr);
	if (ret)
		redirty_page_for_writepage(wbc, page);

	end_page_writeback(page);
	unlock_page(page);
	return ret;
}

#endif

/* ── fsync / open / release ──────────────────────────────────── */

static int mfs_fsync(struct file *file, loff_t start, loff_t end, int datasync)
{
	int ret;

	pr_info("mfs: fsync: ino=%lu start=%lld end=%lld datasync=%d\n",
		file_inode(file)->i_ino, start, end, datasync);

	ret = file_write_and_wait_range(file, start, end);
	if (ret) {
		pr_err("mfs: fsync: write_and_wait_range failed ret=%d\n", ret);
		return ret;
	}

	ret = mfs_inode_async_flush(file_inode(file));
	if (ret) {
		pr_err("mfs: fsync: async_flush failed ret=%d\n", ret);
		return ret;
	}

	ret = filemap_write_and_wait(file->f_mapping);
	if (ret) {
		pr_err("mfs: fsync: filemap_write_and_wait failed ret=%d\n", ret);
		return ret;
	}

	/*
	 * Upstream MooseFS clients no longer rely on a dedicated FUSE_FSYNC
	 * RPC to the master in the common path. The kernel driver's writeback
	 * already pushed dirty pages before we get here, and sending the extra
	 * RPC causes the current master to tear down the session.
	 */
	pr_info("mfs: fsync: writeback complete, skipping master fsync rpc\n");
	return 0;
}

static int mfs_open(struct inode *inode, struct file *file)
{
	return generic_file_open(inode, file);
}

static int mfs_release(struct inode *inode, struct file *file)
{
	(void)inode;
	(void)file;
	return 0;
}

/* ── Operations tables ───────────────────────────────────────── */

const struct address_space_operations mfs_aops = {
#if LINUX_VERSION_CODE < KERNEL_VERSION(5, 16, 0)
	.readpage = mfs_readpage,
#else
	.read_folio = mfs_read_folio,
#endif
	.readahead = mfs_readahead,
	.write_begin = mfs_write_begin,
	.write_end = mfs_write_end,
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 12, 0)
	.writepages = mfs_writepages,
#else
	.writepage = mfs_writepage,
#endif
	/*
	 * 6.8+ replaced .set_page_dirty with .dirty_folio and removed
	 * __set_page_dirty_nobuffers.  For older kernels we use the
	 * page-based callback.
	 */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 8, 0)
	.dirty_folio = filemap_dirty_folio,
#else
	.set_page_dirty = __set_page_dirty_nobuffers,
#endif
};

const struct file_operations mfs_file_ops = {
	.owner = THIS_MODULE,
	.open = mfs_open,
	.release = mfs_release,
	.read_iter = generic_file_read_iter,
	.write_iter = generic_file_write_iter,
	.mmap = generic_file_mmap,
	.fsync = mfs_fsync,
	.llseek = generic_file_llseek,
};
