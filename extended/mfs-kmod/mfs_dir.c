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

static int mfs_iterate_shared(struct file *file, struct dir_context *ctx)
{
	struct inode *inode = file_inode(file);
	struct mfs_ctrl_readdir_req req;
	bool done = false;

	if (ctx->pos == 0) {
		if (!dir_emit_dots(file, ctx))
			return 0;
	}

	while (!done) {
		void *raw = NULL;
		u32 raw_len = 0;
		struct mfs_ctrl_readdir_rsp *rsp;
		u8 *ptr;
		u8 *end;
		u32 i;
		int ret;

		memset(&req, 0, sizeof(req));
		req.session_id = MFS_SB(inode->i_sb)->session_id;
		req.inode = inode->i_ino;
		req.uid = from_kuid(current_user_ns(), current_fsuid());
		req.gid = from_kgid(current_user_ns(), current_fsgid());
		req.offset = ctx->pos;
		req.max_entries = 256;

		ret = mfs_call_checked(MFS_CTRL_OP_READDIR, &req, sizeof(req),
				       &raw, &raw_len);
		if (ret)
			return ret;

		if (raw_len < sizeof(*rsp)) {
			kfree(raw);
			return -EIO;
		}

		rsp = raw;
		ptr = (u8 *)(rsp + 1);
		end = (u8 *)raw + raw_len;

		for (i = 0; i < rsp->count; i++) {
			struct mfs_ctrl_dirent_wire *de;
			char *name;

			if (ptr + sizeof(*de) > end) {
				kfree(raw);
				return -EIO;
			}
			de = (struct mfs_ctrl_dirent_wire *)ptr;
			ptr += sizeof(*de);
			if (ptr + de->name_len > end) {
				kfree(raw);
				return -EIO;
			}
			name = (char *)ptr;
			ptr += de->name_len;

			if (!dir_emit(ctx, name, de->name_len, de->inode,
				      mfs_wire_type_to_dtype(de->type))) {
				kfree(raw);
				return 0;
			}
			ctx->pos = de->next_offset;
		}

		done = !!rsp->eof;
		if (!rsp->count)
			done = true;
		if (!done && rsp->next_offset > ctx->pos)
			ctx->pos = rsp->next_offset;

		kfree(raw);
	}

	return 0;
}

/*
 * Implementation of mkdir operation - always returns int
 */
static int mfs_mkdir_op_impl(struct mnt_idmap *idmap, struct inode *dir,
			     struct dentry *dentry, umode_t mode)
{
	struct mfs_ctrl_mkdir_req *req;
	struct mfs_ctrl_create_rsp *rsp;
	struct inode *inode;
	void *raw = NULL;
	u32 raw_len = 0;
	u16 nlen;
	int ret;

	(void)idmap;

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

	ret = mfs_call_checked(MFS_CTRL_OP_MKDIR, req, sizeof(*req) + nlen,
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
	inode_inc_iversion(dir);
	mfs_update_dir_times(dir);
	return 0;
}

/*
 * Wrapper for inode_operations.mkdir callback.
 * Kernel 6.15+ expects struct dentry * return; older kernels expect int.
 */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 15, 0)
struct dentry *mfs_mkdir_op(struct mnt_idmap *idmap, struct inode *dir,
			    struct dentry *dentry, umode_t mode)
{
	return mfs_int_to_dentry_result(mfs_mkdir_op_impl(idmap, dir, dentry, mode));
}
#else
int mfs_mkdir_op(struct mnt_idmap *idmap, struct inode *dir,
		 struct dentry *dentry, umode_t mode)
{
	return mfs_mkdir_op_impl(idmap, dir, dentry, mode);
}
#endif

int mfs_rmdir_op(struct inode *dir, struct dentry *dentry)
{
	struct mfs_ctrl_rmdir_req *req;
	void *rsp = NULL;
	u32 rsp_len = 0;
	u16 nlen;
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

	ret = mfs_call_checked(MFS_CTRL_OP_RMDIR, req, sizeof(*req) + nlen,
			       &rsp, &rsp_len);
	kfree(req);
	kfree(rsp);
	if (ret)
		return ret;

	clear_nlink(d_inode(dentry));
	inode_inc_iversion(dir);
	mfs_update_dir_times(dir);
	return 0;
}

const struct file_operations mfs_dir_ops = {
	.owner = THIS_MODULE,
	.llseek = generic_file_llseek,
	.iterate_shared = mfs_iterate_shared,
};
