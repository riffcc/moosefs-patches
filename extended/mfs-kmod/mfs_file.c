#include "mfs.h"

#include <linux/writeback.h>

static int mfs_call_checked(u16 op, const void *req, u32 req_len,
			    void **rsp, u32 *rsp_len)
{
	s32 status;
	int ret;

	ret = mfs_helper_call(op, req, req_len, rsp, rsp_len, &status,
			      MFS_HELPER_TIMEOUT);
	if (ret)
		return ret;
	ret = mfs_moosefs_error_to_errno(status);
	if (ret) {
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
		.flags = 0,
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
	void *raw = NULL;
	u32 raw_len = 0;
	void *kaddr;
	int ret;

	ret = mfs_read_remote(inode, off, PAGE_SIZE, &raw, &raw_len);
	if (ret)
		return ret;

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

static int mfs_readpage(struct file *file, struct page *page)
{
	struct inode *inode = page->mapping->host;
	int ret;

	(void)file;

	ret = mfs_fill_page_from_remote(inode, page);
	if (ret) {
		SetPageError(page);
		unlock_page(page);
		return ret;
	}

	SetPageUptodate(page);
	unlock_page(page);
	return 0;
}

#if LINUX_VERSION_CODE >= KERNEL_VERSION(5,16,0)
static int mfs_read_folio(struct file *file, struct folio *folio)
{
	return mfs_readpage(file, &folio->page);
}
#endif

static int mfs_write_begin(struct file *file, struct address_space *mapping,
			   loff_t pos, unsigned int len,
			   struct page **pagep, void **fsdata)
{
	struct page *page;
	int ret = 0;

	(void)fsdata;
	(void)file;

	/*
	 * grab_cache_page_write_begin() was deprecated in kernel 6.8.
	 * Use pagecache_get_page with the appropriate flags instead.
	 */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 8, 0)
	page = pagecache_get_page(mapping, pos >> PAGE_SHIFT,
				  FGP_WRITEBEGIN,
				  mapping_gfp_mask(mapping));
#else
	page = grab_cache_page_write_begin(mapping, pos >> PAGE_SHIFT, 0);
#endif
	if (!page)
		return -ENOMEM;

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
		if (!PageUptodate(page))
			SetPageUptodate(page);
		set_page_dirty(page);
		if (end_pos > i_size_read(inode))
			i_size_write(inode, end_pos);
	}

	unlock_page(page);
	put_page(page);
	return copied;
}

static int mfs_writepage(struct page *page, struct writeback_control *wbc)
{
	struct inode *inode = page->mapping->host;
	loff_t off = page_offset(page);
	loff_t isize = i_size_read(inode);
	u32 len;
	void *kaddr;
	struct mfs_ctrl_write_rsp wrsp;
	int ret = 0;

	if (off >= isize) {
		clear_page_dirty_for_io(page);
		unlock_page(page);
		return 0;
	}

	len = min_t(loff_t, PAGE_SIZE, isize - off);
	if (!clear_page_dirty_for_io(page)) {
		unlock_page(page);
		return 0;
	}

	set_page_writeback(page);
	kaddr = kmap_local_page(page);
	ret = mfs_write_remote(inode, off, kaddr, len, &wrsp);
	kunmap_local(kaddr);

	if (!ret && wrsp.attr_valid)
		mfs_wire_attr_to_inode(inode, &wrsp.attr);
	if (ret)
		redirty_page_for_writepage(wbc, page);

	end_page_writeback(page);
	unlock_page(page);
	return ret;
}

static int mfs_fsync(struct file *file, loff_t start, loff_t end, int datasync)
{
	struct mfs_ctrl_fsync_req req;
	void *rsp = NULL;
	u32 rsp_len = 0;
	int ret;

	ret = file_write_and_wait_range(file, start, end);
	if (ret)
		return ret;

	memset(&req, 0, sizeof(req));
	req.session_id = MFS_SB(file_inode(file)->i_sb)->session_id;
	req.inode = file_inode(file)->i_ino;
	req.datasync = datasync ? 1 : 0;

	ret = mfs_call_checked(MFS_CTRL_OP_FSYNC, &req, sizeof(req), &rsp, &rsp_len);
	kfree(rsp);
	return ret;
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

const struct address_space_operations mfs_aops = {
#if LINUX_VERSION_CODE >= KERNEL_VERSION(5,16,0)
	.read_folio = mfs_read_folio,
#else
	.readpage = mfs_readpage,
#endif
	.write_begin = mfs_write_begin,
	.write_end = mfs_write_end,
	.writepage = mfs_writepage,
	/*
	 * Kernel 6.8 replaced .set_page_dirty with .dirty_folio in
	 * address_space_operations, and __set_page_dirty_nobuffers was
	 * superseded by filemap_dirty_folio.
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
