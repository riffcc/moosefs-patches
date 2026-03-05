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

static int mfs_do_xattr_get(struct inode *inode, const char *full_name,
			    void *buffer, size_t size)
{
	struct mfs_ctrl_xattr_req *req;
	struct mfs_ctrl_xattr_rsp *rsp;
	void *raw = NULL;
	u32 raw_len = 0;
	u16 nlen;
	u32 req_len;
	int ret;

	nlen = strlen(full_name);
	if (nlen > MFS_NAME_MAX)
		return -ENAMETOOLONG;

	req_len = sizeof(*req) + nlen;
	req = kzalloc(req_len, GFP_KERNEL);
	if (!req)
		return -ENOMEM;

	req->session_id = MFS_SB(inode->i_sb)->session_id;
	req->inode = inode->i_ino;
	req->uid = from_kuid(current_user_ns(), current_fsuid());
	req->gid = from_kgid(current_user_ns(), current_fsgid());
	req->flags = (size == 0) ? 1 : 0;
	req->name_len = nlen;
	req->value_len = 0;
	memcpy(req + 1, full_name, nlen);

	ret = mfs_call_checked(MFS_CTRL_OP_GETXATTR, req, req_len, &raw, &raw_len);
	kfree(req);
	if (ret)
		return ret;
	if (raw_len < sizeof(*rsp)) {
		kfree(raw);
		return -EIO;
	}

	rsp = raw;
	if (raw_len < sizeof(*rsp) + rsp->size) {
		kfree(raw);
		return -EIO;
	}
	if (!buffer) {
		ret = rsp->size;
		kfree(raw);
		return ret;
	}
	if (size < rsp->size) {
		kfree(raw);
		return -ERANGE;
	}
	memcpy(buffer, rsp + 1, rsp->size);
	ret = rsp->size;
	kfree(raw);
	return ret;
}

static int mfs_xattr_get(const struct xattr_handler *handler,
			 struct dentry *dentry, struct inode *inode,
			 const char *name, void *buffer, size_t size)
{
	char full_name[XATTR_NAME_MAX + 16];

	if (!name || !*name)
		return -EINVAL;
	if (!handler || !handler->prefix)
		return -EOPNOTSUPP;
	if (snprintf(full_name, sizeof(full_name), "%s%s", handler->prefix, name) >=
	    sizeof(full_name))
		return -ENAMETOOLONG;

	(void)dentry;
	return mfs_do_xattr_get(inode, full_name, buffer, size);
}

static int mfs_do_xattr_set(struct inode *inode, const char *full_name,
			    const void *value, size_t size, int flags)
{
	struct mfs_ctrl_xattr_req *req;
	void *rsp = NULL;
	u32 rsp_len = 0;
	u16 nlen;
	u32 req_len;
	int ret;
	u32 mode = 0;

	nlen = strlen(full_name);
	if (nlen > MFS_NAME_MAX)
		return -ENAMETOOLONG;

	if (flags & XATTR_CREATE)
		mode = 1;
	else if (flags & XATTR_REPLACE)
		mode = 2;
	else
		mode = 0;

	req_len = sizeof(*req) + nlen + size;
	req = kzalloc(req_len, GFP_KERNEL);
	if (!req)
		return -ENOMEM;

	req->session_id = MFS_SB(inode->i_sb)->session_id;
	req->inode = inode->i_ino;
	req->uid = from_kuid(current_user_ns(), current_fsuid());
	req->gid = from_kgid(current_user_ns(), current_fsgid());
	req->flags = mode;
	req->name_len = nlen;
	req->value_len = size;
	memcpy(req + 1, full_name, nlen);
	if (size)
		memcpy((u8 *)(req + 1) + nlen, value, size);

	ret = mfs_call_checked(MFS_CTRL_OP_SETXATTR, req, req_len, &rsp, &rsp_len);
	kfree(req);
	kfree(rsp);
	return ret;
}

static int mfs_xattr_set(const struct xattr_handler *handler,
			 struct mnt_idmap *idmap, struct dentry *dentry,
			 struct inode *inode, const char *name,
			 const void *value, size_t size, int flags)
{
	char full_name[XATTR_NAME_MAX + 16];

	(void)idmap;
	(void)dentry;

	if (!name || !*name)
		return -EINVAL;
	if (!handler || !handler->prefix)
		return -EOPNOTSUPP;
	if (snprintf(full_name, sizeof(full_name), "%s%s", handler->prefix, name) >=
	    sizeof(full_name))
		return -ENAMETOOLONG;

	return mfs_do_xattr_set(inode, full_name, value, size, flags);
}

ssize_t mfs_listxattr(struct dentry *dentry, char *buffer, size_t size)
{
	struct inode *inode = d_inode(dentry);
	struct mfs_ctrl_inode_req req;
	struct mfs_ctrl_xattr_rsp *rsp;
	void *raw = NULL;
	u32 raw_len = 0;
	int ret;

	memset(&req, 0, sizeof(req));
	req.session_id = MFS_SB(inode->i_sb)->session_id;
	req.inode = inode->i_ino;
	req.uid = from_kuid(current_user_ns(), current_fsuid());
	req.gid = from_kgid(current_user_ns(), current_fsgid());

	ret = mfs_call_checked(MFS_CTRL_OP_LISTXATTR, &req, sizeof(req), &raw, &raw_len);
	if (ret)
		return ret;
	if (raw_len < sizeof(*rsp)) {
		kfree(raw);
		return -EIO;
	}

	rsp = raw;
	if (raw_len < sizeof(*rsp) + rsp->size) {
		kfree(raw);
		return -EIO;
	}
	if (!buffer) {
		ret = rsp->size;
		kfree(raw);
		return ret;
	}
	if (size < rsp->size) {
		kfree(raw);
		return -ERANGE;
	}
	memcpy(buffer, rsp + 1, rsp->size);
	ret = rsp->size;
	kfree(raw);
	return ret;
}

static const struct xattr_handler mfs_xattr_user_handler = {
	.prefix = XATTR_USER_PREFIX,
	.get = mfs_xattr_get,
	.set = mfs_xattr_set,
};

static const struct xattr_handler mfs_xattr_trusted_handler = {
	.prefix = XATTR_TRUSTED_PREFIX,
	.get = mfs_xattr_get,
	.set = mfs_xattr_set,
};

const struct xattr_handler * const mfs_xattr_handlers[] = {
	&mfs_xattr_user_handler,
	&mfs_xattr_trusted_handler,
	NULL,
};
