#include "mfs.h"

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

int mfs_symlink_op(struct mnt_idmap *idmap, struct inode *dir,
		   struct dentry *dentry, const char *symname)
{
	struct mfs_ctrl_symlink_req *req;
	struct mfs_ctrl_create_rsp *rsp;
	struct inode *inode;
	void *raw = NULL;
	u32 raw_len = 0;
	u16 nlen;
	u16 tlen;
	u32 req_len;
	int ret;

	(void)idmap;

	nlen = dentry->d_name.len;
	if (nlen > MFS_NAME_MAX)
		return -ENAMETOOLONG;
	tlen = strnlen(symname, MFS_SYMLINK_MAX + 1);
	if (tlen > MFS_SYMLINK_MAX)
		return -ENAMETOOLONG;

	req_len = sizeof(*req) + nlen + tlen;
	req = kzalloc(req_len, GFP_KERNEL);
	if (!req)
		return -ENOMEM;

	req->session_id = MFS_SB(dir->i_sb)->session_id;
	req->parent_inode = dir->i_ino;
	req->uid = from_kuid(current_user_ns(), current_fsuid());
	req->gid = from_kgid(current_user_ns(), current_fsgid());
	req->name_len = nlen;
	req->target_len = tlen;
	memcpy(req + 1, dentry->d_name.name, nlen);
	memcpy((u8 *)(req + 1) + nlen, symname, tlen);

	ret = mfs_call_checked(MFS_CTRL_OP_SYMLINK, req, req_len, &raw, &raw_len);
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
	inode_inc_iversion(dir);
	mfs_update_dir_times(dir);
	return 0;
}

static const char *mfs_get_link(struct dentry *dentry, struct inode *inode,
				struct delayed_call *done)
{
	struct mfs_ctrl_inode_req req;
	struct mfs_ctrl_readlink_rsp *rsp;
	void *raw = NULL;
	u32 raw_len = 0;
	char *target;
	int ret;

	if (!inode)
		inode = d_inode(dentry);

	memset(&req, 0, sizeof(req));
	req.session_id = MFS_SB(inode->i_sb)->session_id;
	req.inode = inode->i_ino;
	req.uid = from_kuid(current_user_ns(), current_fsuid());
	req.gid = from_kgid(current_user_ns(), current_fsgid());

	ret = mfs_call_checked(MFS_CTRL_OP_READLINK, &req, sizeof(req), &raw, &raw_len);
	if (ret)
		return ERR_PTR(ret);

	if (raw_len < sizeof(*rsp)) {
		kfree(raw);
		return ERR_PTR(-EIO);
	}

	rsp = raw;
	if (rsp->size > MFS_SYMLINK_MAX || raw_len < sizeof(*rsp) + rsp->size) {
		kfree(raw);
		return ERR_PTR(-EIO);
	}

	target = kmalloc(rsp->size + 1, GFP_KERNEL);
	if (!target) {
		kfree(raw);
		return ERR_PTR(-ENOMEM);
	}
	memcpy(target, rsp + 1, rsp->size);
	target[rsp->size] = '\0';
	kfree(raw);

	set_delayed_call(done, kfree_link, target);
	return target;
}

const struct inode_operations mfs_symlink_inode_ops = {
	.get_link = mfs_get_link,
	.getattr = mfs_inode_getattr,
	.setattr = mfs_inode_setattr,
	.listxattr = mfs_listxattr,
};
