#include "mfs.h"

static struct kmem_cache *mfs_inode_cachep;

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

static void mfs_setup_inode_ops(struct inode *inode)
{
	switch (inode->i_mode & S_IFMT) {
	case S_IFDIR:
		inode->i_op = &mfs_dir_inode_ops;
		inode->i_fop = &mfs_dir_ops;
		break;
	case S_IFLNK:
		inode->i_op = &mfs_symlink_inode_ops;
		inode_nohighmem(inode);
		break;
	case S_IFREG:
		inode->i_op = &mfs_file_inode_ops;
		inode->i_fop = &mfs_file_ops;
		inode->i_mapping->a_ops = &mfs_aops;
		break;
	case S_IFCHR:
	case S_IFBLK:
	case S_IFIFO:
	case S_IFSOCK:
		inode->i_op = &mfs_special_inode_ops;
		init_special_inode(inode, inode->i_mode, inode->i_rdev);
		break;
	default:
		inode->i_op = &mfs_special_inode_ops;
		break;
	}
}

struct inode *mfs_alloc_inode(struct super_block *sb)
{
	struct mfs_inode_info *mi;

	mi = kmem_cache_alloc(mfs_inode_cachep, GFP_KERNEL);
	if (!mi)
		return NULL;
	memset(mi, 0, sizeof(*mi));
	spin_lock_init(&mi->lock);
	mfs_inode_async_init(mi);
	inode_init_once(&mi->vfs_inode);
	return &mi->vfs_inode;
}

void mfs_free_inode(struct inode *inode)
{
	kmem_cache_free(mfs_inode_cachep, MFS_I(inode));
}

int mfs_inode_cache_init(void)
{
	mfs_inode_cachep = kmem_cache_create("mfs_inode_cache",
					    sizeof(struct mfs_inode_info),
					    0, SLAB_RECLAIM_ACCOUNT,
					    NULL);
	if (!mfs_inode_cachep)
		return -ENOMEM;
	return 0;
}

void mfs_inode_cache_destroy(void)
{
	if (mfs_inode_cachep)
		kmem_cache_destroy(mfs_inode_cachep);
	mfs_inode_cachep = NULL;
}

int mfs_do_getattr(struct inode *inode, struct mfs_wire_attr *attr,
		   kuid_t uid, kgid_t gid)
{
	struct mfs_ctrl_inode_req req = {
		.session_id = MFS_SB(inode->i_sb)->session_id,
		.inode = inode->i_ino,
		.uid = from_kuid(current_user_ns(), uid),
		.gid = from_kgid(current_user_ns(), gid),
	};
	void *rsp = NULL;
	u32 rsp_len = 0;
	int ret;

	ret = mfs_call_checked(MFS_CTRL_OP_GETATTR, &req, sizeof(req), &rsp, &rsp_len);
	if (ret)
		return ret;

	if (rsp_len < sizeof(*attr)) {
		kfree(rsp);
		return -EIO;
	}
	memcpy(attr, rsp, sizeof(*attr));
	kfree(rsp);
	return 0;
}

int mfs_refresh_inode(struct inode *inode, kuid_t uid, kgid_t gid)
{
	struct mfs_wire_attr attr;
	int ret;

	ret = mfs_do_getattr(inode, &attr, uid, gid);
	if (ret)
		return ret;

	mfs_wire_attr_to_inode(inode, &attr);
	mfs_setup_inode_ops(inode);
	return 0;
}

struct inode *mfs_iget(struct super_block *sb, u32 ino)
{
	struct inode *inode;
	int ret;

	inode = iget_locked(sb, ino);
	if (!inode)
		return ERR_PTR(-ENOMEM);
	if (!(inode->i_state & I_NEW))
		return inode;

	ret = mfs_refresh_inode(inode, MFS_SB(sb)->mount_uid, MFS_SB(sb)->mount_gid);
	if (ret) {
		iget_failed(inode);
		return ERR_PTR(ret);
	}

	unlock_new_inode(inode);
	return inode;
}

struct inode *mfs_iget_with_attr(struct super_block *sb, u32 ino,
				 const struct mfs_wire_attr *attr)
{
	struct inode *inode;

	inode = iget_locked(sb, ino);
	if (!inode)
		return ERR_PTR(-ENOMEM);
	if (!(inode->i_state & I_NEW)) {
		mfs_wire_attr_to_inode(inode, attr);
		mfs_setup_inode_ops(inode);
		return inode;
	}

	mfs_wire_attr_to_inode(inode, attr);
	mfs_setup_inode_ops(inode);
	unlock_new_inode(inode);
	return inode;
}

static struct dentry *mfs_lookup(struct inode *dir, struct dentry *dentry,
				 unsigned int flags)
{
	struct mfs_ctrl_lookup_req *req;
	struct mfs_ctrl_lookup_rsp *rsp;
	struct inode *inode;
	void *raw = NULL;
	u32 raw_len = 0;
	u16 nlen;
	int ret;

	(void)flags;

	nlen = dentry->d_name.len;
	if (nlen > MFS_NAME_MAX)
		return ERR_PTR(-ENAMETOOLONG);

	req = kzalloc(sizeof(*req) + nlen, GFP_KERNEL);
	if (!req)
		return ERR_PTR(-ENOMEM);

	req->session_id = MFS_SB(dir->i_sb)->session_id;
	req->parent_inode = dir->i_ino;
	req->uid = from_kuid(current_user_ns(), current_fsuid());
	req->gid = from_kgid(current_user_ns(), current_fsgid());
	req->name_len = nlen;
	memcpy(req + 1, dentry->d_name.name, nlen);

	ret = mfs_call_checked(MFS_CTRL_OP_LOOKUP, req, sizeof(*req) + nlen,
			       &raw, &raw_len);
	kfree(req);
	if (ret == -ENOENT) {
		d_add(dentry, NULL);
		return NULL;
	}
	if (ret)
		return ERR_PTR(ret);

	if (raw_len < sizeof(*rsp)) {
		kfree(raw);
		return ERR_PTR(-EIO);
	}

	rsp = raw;
	inode = mfs_iget_with_attr(dir->i_sb, rsp->inode, &rsp->attr);
	kfree(raw);
	if (IS_ERR(inode))
		return ERR_CAST(inode);

	return d_splice_alias(inode, dentry);
}

/*
 * create operation - always returns int in 6.18 and earlier
 */
static int mfs_create(struct mnt_idmap *idmap, struct inode *dir,
		      struct dentry *dentry, umode_t mode, bool excl)
{
	struct mfs_ctrl_create_req *req;
	struct mfs_ctrl_create_rsp *rsp;
	struct inode *inode;
	void *raw = NULL;
	u32 raw_len = 0;
	u16 nlen;
	int ret;

	(void)idmap;
	(void)excl;

	nlen = dentry->d_name.len;
	if (nlen > MFS_NAME_MAX)
		return -ENAMETOOLONG;

	req = kzalloc(sizeof(*req) + nlen, GFP_KERNEL);
	if (!req)
		return -ENOMEM;

	req->session_id = MFS_SB(dir->i_sb)->session_id;
	req->parent_inode = dir->i_ino;
	req->uid = from_kuid(current_user_ns(), current_fsuid());
	req->gid = from_kgid(current_user_ns(), current_fsgid());
	req->mode = mode & 07777;
	req->name_len = nlen;
	memcpy(req + 1, dentry->d_name.name, nlen);

	ret = mfs_call_checked(MFS_CTRL_OP_CREATE, req, sizeof(*req) + nlen,
			       &raw, &raw_len);
	kfree(req);
	if (ret)
		return ret;
	if (raw_len < sizeof(*rsp)) {
		kfree(raw);
		return -EIO;
	}

	rsp = raw;
	inode = mfs_iget_with_attr(dir->i_sb, rsp->inode, &rsp->attr);
	kfree(raw);
	if (IS_ERR(inode))
		return PTR_ERR(inode);

	d_instantiate(dentry, inode);
	mfs_update_dir_times(dir);
	inode_inc_iversion(dir);
	return 0;
}

static int mfs_unlink(struct inode *dir, struct dentry *dentry)
{
	struct mfs_ctrl_unlink_req *req;
	u16 nlen;
	void *rsp = NULL;
	u32 rsp_len = 0;
	int ret;

	nlen = dentry->d_name.len;
	if (nlen > MFS_NAME_MAX)
		return -ENAMETOOLONG;

	req = kzalloc(sizeof(*req) + nlen, GFP_KERNEL);
	if (!req)
		return -ENOMEM;

	req->session_id = MFS_SB(dir->i_sb)->session_id;
	req->parent_inode = dir->i_ino;
	req->uid = from_kuid(current_user_ns(), current_fsuid());
	req->gid = from_kgid(current_user_ns(), current_fsgid());
	req->name_len = nlen;
	memcpy(req + 1, dentry->d_name.name, nlen);

	ret = mfs_call_checked(MFS_CTRL_OP_UNLINK, req, sizeof(*req) + nlen,
			       &rsp, &rsp_len);
	kfree(req);
	kfree(rsp);
	if (ret)
		return ret;

	if (d_inode(dentry))
		drop_nlink(d_inode(dentry));
	mfs_update_dir_times(dir);
	inode_inc_iversion(dir);
	return 0;
}

static int mfs_link(struct dentry *old_dentry, struct inode *dir,
		    struct dentry *dentry)
{
	struct mfs_ctrl_link_req *req;
	u16 nlen;
	void *rsp = NULL;
	u32 rsp_len = 0;
	int ret;

	nlen = dentry->d_name.len;
	if (nlen > MFS_NAME_MAX)
		return -ENAMETOOLONG;

	req = kzalloc(sizeof(*req) + nlen, GFP_KERNEL);
	if (!req)
		return -ENOMEM;

	req->session_id = MFS_SB(dir->i_sb)->session_id;
	req->inode = d_inode(old_dentry)->i_ino;
	req->new_parent_inode = dir->i_ino;
	req->uid = from_kuid(current_user_ns(), current_fsuid());
	req->gid = from_kgid(current_user_ns(), current_fsgid());
	req->name_len = nlen;
	memcpy(req + 1, dentry->d_name.name, nlen);

	ret = mfs_call_checked(MFS_CTRL_OP_LINK, req, sizeof(*req) + nlen,
			       &rsp, &rsp_len);
	kfree(req);
	kfree(rsp);
	if (ret)
		return ret;

	ihold(d_inode(old_dentry));
	d_instantiate(dentry, d_inode(old_dentry));
	inode_inc_link_count(d_inode(old_dentry));
	mfs_update_dir_times(dir);
	inode_inc_iversion(dir);
	return 0;
}

static int mfs_rename(struct mnt_idmap *idmap,
		      struct inode *old_dir, struct dentry *old_dentry,
		      struct inode *new_dir, struct dentry *new_dentry,
		      unsigned int flags)
{
	struct mfs_ctrl_rename_req *req;
	u16 old_len = old_dentry->d_name.len;
	u16 new_len = new_dentry->d_name.len;
	u32 req_len;
	void *rsp = NULL;
	u32 rsp_len = 0;
	int ret;

	(void)idmap;

	if (old_len > MFS_NAME_MAX || new_len > MFS_NAME_MAX)
		return -ENAMETOOLONG;

	if (flags & ~(RENAME_NOREPLACE | RENAME_EXCHANGE))
		return -EINVAL;

	req_len = sizeof(*req) + old_len + new_len;
	req = kzalloc(req_len, GFP_KERNEL);
	if (!req)
		return -ENOMEM;

	req->session_id = MFS_SB(old_dir->i_sb)->session_id;
	req->old_parent_inode = old_dir->i_ino;
	req->new_parent_inode = new_dir->i_ino;
	req->uid = from_kuid(current_user_ns(), current_fsuid());
	req->gid = from_kgid(current_user_ns(), current_fsgid());
	req->flags = flags;
	req->old_name_len = old_len;
	req->new_name_len = new_len;
	memcpy(req + 1, old_dentry->d_name.name, old_len);
	memcpy((u8 *)(req + 1) + old_len, new_dentry->d_name.name, new_len);

	ret = mfs_call_checked(MFS_CTRL_OP_RENAME, req, req_len, &rsp, &rsp_len);
	kfree(req);
	kfree(rsp);
	if (ret)
		return ret;

	mfs_update_dir_times(old_dir);
	mfs_update_dir_times(new_dir);
	inode_inc_iversion(old_dir);
	inode_inc_iversion(new_dir);
	return 0;
}

int mfs_inode_getattr(struct mnt_idmap *idmap, const struct path *path,
		      struct kstat *stat, u32 request_mask,
		      unsigned int query_flags)
{
	struct mfs_wire_attr attr;
	int ret;

	(void)idmap;
	(void)request_mask;
	(void)query_flags;

	ret = mfs_do_getattr(d_inode(path->dentry), &attr, current_fsuid(), current_fsgid());
	if (ret)
		return ret;

	mfs_wire_attr_to_inode(d_inode(path->dentry), &attr);
	mfs_setup_inode_ops(d_inode(path->dentry));
	return mfs_wire_attr_to_kstat(d_inode(path->dentry)->i_ino, &attr, stat);
}

int mfs_inode_setattr(struct mnt_idmap *idmap, struct dentry *dentry,
		      struct iattr *iattr)
{
	struct inode *inode = d_inode(dentry);
	struct mfs_ctrl_setattr_req req;
	struct mfs_ctrl_truncate_req treq;
	struct mfs_wire_attr attr;
	void *rsp = NULL;
	u32 rsp_len = 0;
	u32 valid = 0;
	int ret;

	ret = setattr_prepare(idmap, dentry, iattr);
	if (ret)
		return ret;

	if (iattr->ia_valid & ATTR_SIZE) {
		treq.session_id = MFS_SB(inode->i_sb)->session_id;
		treq.inode = inode->i_ino;
		treq.uid = from_kuid(current_user_ns(), current_fsuid());
		treq.gid = from_kgid(current_user_ns(), current_fsgid());
		treq.size = iattr->ia_size;

		ret = mfs_call_checked(MFS_CTRL_OP_TRUNCATE, &treq, sizeof(treq), &rsp, &rsp_len);
		if (ret)
			return ret;
		if (rsp_len >= sizeof(attr)) {
			memcpy(&attr, rsp, sizeof(attr));
			mfs_wire_attr_to_inode(inode, &attr);
		}
		kfree(rsp);
		rsp = NULL;
		rsp_len = 0;
	}

	if (iattr->ia_valid & ATTR_MODE)
		valid |= MFS_SETATTR_MODE;
	if (iattr->ia_valid & ATTR_UID)
		valid |= MFS_SETATTR_UID;
	if (iattr->ia_valid & ATTR_GID)
		valid |= MFS_SETATTR_GID;
	if (iattr->ia_valid & (ATTR_ATIME | ATTR_ATIME_SET))
		valid |= MFS_SETATTR_ATIME;
	if (iattr->ia_valid & (ATTR_MTIME | ATTR_MTIME_SET))
		valid |= MFS_SETATTR_MTIME;
#ifdef ATTR_ATIME_NOW
	if (iattr->ia_valid & ATTR_ATIME_NOW)
		valid |= MFS_SETATTR_ATIME_NOW;
#endif
#ifdef ATTR_MTIME_NOW
	if (iattr->ia_valid & ATTR_MTIME_NOW)
		valid |= MFS_SETATTR_MTIME_NOW;
#endif

	if (!valid)
		return 0;

	memset(&req, 0, sizeof(req));
	req.session_id = MFS_SB(inode->i_sb)->session_id;
	req.inode = inode->i_ino;
	req.uid = from_kuid(current_user_ns(), current_fsuid());
	req.gid = from_kgid(current_user_ns(), current_fsgid());
	req.valid = valid;
	req.mode = iattr->ia_mode & 07777;
	req.attr_uid = from_kuid(current_user_ns(), iattr->ia_uid);
	req.attr_gid = from_kgid(current_user_ns(), iattr->ia_gid);
	req.atime_ns = timespec64_to_ns(&iattr->ia_atime);
	req.mtime_ns = timespec64_to_ns(&iattr->ia_mtime);
	req.ctime_ns = timespec64_to_ns(&iattr->ia_ctime);

	ret = mfs_call_checked(MFS_CTRL_OP_SETATTR, &req, sizeof(req), &rsp, &rsp_len);
	if (ret)
		return ret;

	if (rsp_len >= sizeof(attr)) {
		memcpy(&attr, rsp, sizeof(attr));
		mfs_wire_attr_to_inode(inode, &attr);
		mfs_setup_inode_ops(inode);
	}
	kfree(rsp);

	setattr_copy(idmap, inode, iattr);
	mark_inode_dirty(inode);
	return 0;
}

void mfs_evict_inode(struct inode *inode)
{
	mfs_inode_async_destroy(MFS_I(inode));
	truncate_inode_pages_final(&inode->i_data);
	clear_inode(inode);
	mfs_cache_purge_inode(&MFS_SB(inode->i_sb)->chunk_cache, inode->i_ino);
}

const struct inode_operations mfs_file_inode_ops = {
	.getattr = mfs_inode_getattr,
	.setattr = mfs_inode_setattr,
	.listxattr = mfs_listxattr,
};

const struct inode_operations mfs_special_inode_ops = {
	.getattr = mfs_inode_getattr,
	.setattr = mfs_inode_setattr,
	.listxattr = mfs_listxattr,
};

const struct inode_operations mfs_dir_inode_ops = {
	.lookup = mfs_lookup,
	.create = mfs_create,
	.unlink = mfs_unlink,
	.link = mfs_link,
	.rename = mfs_rename,
	.mkdir = mfs_mkdir_op,
	.rmdir = mfs_rmdir_op,
	.symlink = mfs_symlink_op,
	.getattr = mfs_inode_getattr,
	.setattr = mfs_inode_setattr,
	.listxattr = mfs_listxattr,
};
