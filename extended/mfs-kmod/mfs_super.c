#include "mfs.h"

enum {
	Opt_master,
	Opt_port,
	Opt_subdir,
	Opt_uid,
	Opt_gid,
	Opt_password,
	Opt_err,
};

static const match_table_t mfs_tokens = {
	{ Opt_master, "master=%s" },
	{ Opt_port, "port=%u" },
	{ Opt_subdir, "subdir=%s" },
	{ Opt_uid, "uid=%u" },
	{ Opt_gid, "gid=%u" },
	{ Opt_password, "password=%s" },
	{ Opt_err, NULL },
};

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

static int mfs_statfs(struct dentry *dentry, struct kstatfs *buf)
{
	struct super_block *sb = dentry->d_sb;
	struct mfs_ctrl_statfs_req req = {
		.session_id = MFS_SB(sb)->session_id,
	};
	struct mfs_ctrl_statfs_rsp *rsp;
	void *raw = NULL;
	u32 raw_len = 0;
	int ret;

	ret = mfs_call_checked(MFS_CTRL_OP_STATFS, &req, sizeof(req), &raw, &raw_len);
	if (ret)
		return ret;
	if (raw_len < sizeof(*rsp)) {
		kfree(raw);
		return -EIO;
	}
	rsp = raw;

	buf->f_type = MFS_SUPER_MAGIC;
	buf->f_bsize = MFS_BLOCK_SIZE;
	buf->f_blocks = div_u64(rsp->total_space, MFS_BLOCK_SIZE);
	buf->f_bfree = div_u64(rsp->free_space, MFS_BLOCK_SIZE);
	buf->f_bavail = div_u64(rsp->avail_space, MFS_BLOCK_SIZE);
	buf->f_files = rsp->inodes;
	buf->f_ffree = 0;
	buf->f_namelen = MFS_NAME_MAX;
	kfree(raw);
	return 0;
}

const struct super_operations mfs_super_ops = {
	.alloc_inode = mfs_alloc_inode,
	.free_inode = mfs_free_inode,
	.evict_inode = mfs_evict_inode,
	.statfs = mfs_statfs,
	.drop_inode = generic_delete_inode,
};

static int mfs_parse_options(struct mfs_sb_info *sbi, char *options)
{
	char *p;

	if (!options)
		return 0;

	while ((p = strsep(&options, ",")) != NULL) {
		substring_t args[MAX_OPT_ARGS];
		int token;

		if (!*p)
			continue;
		token = match_token(p, mfs_tokens, args);
		switch (token) {
		case Opt_master: {
			char *host = match_strdup(&args[0]);
			char *colon;
			if (!host)
				return -ENOMEM;
			colon = strrchr(host, ':');
			if (colon) {
				unsigned int port;
				*colon++ = '\0';
				if (kstrtouint(colon, 10, &port) == 0 && port <= 65535)
					sbi->master_port = (u16)port;
			}
			strscpy(sbi->master_host, host, sizeof(sbi->master_host));
			kfree(host);
			break;
		}
		case Opt_port: {
			unsigned int port;
			if (match_uint(&args[0], &port) || port > 65535)
				return -EINVAL;
			sbi->master_port = (u16)port;
			break;
		}
		case Opt_subdir: {
			char *v = match_strdup(&args[0]);
			if (!v)
				return -ENOMEM;
			strscpy(sbi->subdir, v, sizeof(sbi->subdir));
			kfree(v);
			break;
		}
		case Opt_uid: {
			unsigned int uid;
			if (match_uint(&args[0], &uid))
				return -EINVAL;
			sbi->mount_uid = make_kuid(current_user_ns(), uid);
			if (!uid_valid(sbi->mount_uid))
				return -EINVAL;
			break;
		}
		case Opt_gid: {
			unsigned int gid;
			if (match_uint(&args[0], &gid))
				return -EINVAL;
			sbi->mount_gid = make_kgid(current_user_ns(), gid);
			if (!gid_valid(sbi->mount_gid))
				return -EINVAL;
			break;
		}
		case Opt_password: {
			char *v = match_strdup(&args[0]);
			if (!v)
				return -ENOMEM;
			strscpy(sbi->password, v, sizeof(sbi->password));
			kfree(v);
			break;
		}
		default:
			return -EINVAL;
		}
	}

	if (sbi->subdir[0] == '\0')
		strscpy(sbi->subdir, MFS_DEFAULT_SUBDIR, sizeof(sbi->subdir));
	if (sbi->master_host[0] == '\0')
		strscpy(sbi->master_host, MFS_DEFAULT_MASTER_HOST,
			sizeof(sbi->master_host));
	if (!sbi->master_port)
		sbi->master_port = MFS_DEFAULT_MASTER_PORT;

	return 0;
}

static int mfs_register_session(struct super_block *sb, u32 *root_ino,
				struct mfs_wire_attr *root_attr)
{
	struct mfs_sb_info *sbi = MFS_SB(sb);
	struct mfs_ctrl_register_req *req;
	struct mfs_ctrl_register_rsp *rsp;
	void *raw = NULL;
	u16 master_len, subdir_len, password_len;
	u32 req_len;
	u32 raw_len = 0;
	int ret;

	master_len = strnlen(sbi->master_host, sizeof(sbi->master_host));
	subdir_len = strnlen(sbi->subdir, sizeof(sbi->subdir));
	password_len = strnlen(sbi->password, sizeof(sbi->password));

	req_len = sizeof(*req) + master_len + subdir_len + password_len;
	req = kzalloc(req_len, GFP_KERNEL);
	if (!req)
		return -ENOMEM;

	req->master_len = master_len;
	req->subdir_len = subdir_len;
	req->password_len = password_len;
	req->master_port = sbi->master_port;
	req->mount_uid = from_kuid(current_user_ns(), sbi->mount_uid);
	req->mount_gid = from_kgid(current_user_ns(), sbi->mount_gid);
	memcpy(req + 1, sbi->master_host, master_len);
	memcpy((u8 *)(req + 1) + master_len, sbi->subdir, subdir_len);
	memcpy((u8 *)(req + 1) + master_len + subdir_len,
	       sbi->password, password_len);

	ret = mfs_call_checked(MFS_CTRL_OP_REGISTER, req, req_len, &raw, &raw_len);
	kfree(req);
	if (ret)
		return ret;
	if (raw_len < sizeof(*rsp)) {
		kfree(raw);
		return -EIO;
	}

	rsp = raw;
	sbi->session_id = rsp->session_id;
	*root_ino = rsp->root_inode ? rsp->root_inode : 1;
	memcpy(root_attr, &rsp->root_attr, sizeof(*root_attr));
	kfree(raw);
	return 0;
}

static int mfs_fill_super(struct super_block *sb, void *data, int silent)
{
	struct mfs_sb_info *sbi;
	struct inode *root_inode;
	struct mfs_wire_attr root_attr;
	u32 root_ino;
	int ret;

	(void)silent;

	sbi = kzalloc(sizeof(*sbi), GFP_KERNEL);
	if (!sbi)
		return -ENOMEM;

	sbi->master_port = MFS_DEFAULT_MASTER_PORT;
	strscpy(sbi->master_host, MFS_DEFAULT_MASTER_HOST, sizeof(sbi->master_host));
	strscpy(sbi->subdir, MFS_DEFAULT_SUBDIR, sizeof(sbi->subdir));
	sbi->mount_uid = current_fsuid();
	sbi->mount_gid = current_fsgid();
	mfs_cache_init(&sbi->chunk_cache);

	sb->s_magic = MFS_SUPER_MAGIC;
	sb->s_fs_info = sbi;
	sb->s_time_gran = 1;
	sb->s_op = &mfs_super_ops;
	sb->s_maxbytes = MAX_LFS_FILESIZE;
	sb->s_xattr = mfs_xattr_handlers;

	ret = mfs_parse_options(sbi, data);
	if (ret)
		goto err;

	ret = mfs_register_session(sb, &root_ino, &root_attr);
	if (ret)
		goto err;

	root_inode = mfs_iget_with_attr(sb, root_ino, &root_attr);
	if (IS_ERR(root_inode)) {
		ret = PTR_ERR(root_inode);
		goto err;
	}

	sb->s_root = d_make_root(root_inode);
	if (!sb->s_root) {
		ret = -ENOMEM;
		goto err;
	}

	return 0;

err:
	mfs_cache_destroy(&sbi->chunk_cache);
	kfree(sbi);
	sb->s_fs_info = NULL;
	return ret;
}

static struct dentry *mfs_mount(struct file_system_type *fs_type,
				int flags, const char *dev_name,
				void *data)
{
	(void)dev_name;
	return mount_nodev(fs_type, flags, data, mfs_fill_super);
}

static void mfs_kill_sb(struct super_block *sb)
{
	struct mfs_sb_info *sbi = MFS_SB(sb);

	kill_anon_super(sb);
	if (sbi) {
		mfs_cache_destroy(&sbi->chunk_cache);
		kfree(sbi);
	}
}

struct file_system_type mfs_fs_type = {
	.owner = THIS_MODULE,
	.name = "mfs",
	.mount = mfs_mount,
	.kill_sb = mfs_kill_sb,
	.fs_flags = FS_USERNS_MOUNT,
};

static int __init mfs_init(void)
{
	int ret;

	ret = mfs_inode_cache_init();
	if (ret)
		return ret;

	ret = mfs_helper_comm_init();
	if (ret)
		goto err_inode;

	ret = register_filesystem(&mfs_fs_type);
	if (ret)
		goto err_helper;

	pr_info("mfs: kernel module loaded\n");
	return 0;

err_helper:
	mfs_helper_comm_exit();
err_inode:
	mfs_inode_cache_destroy();
	return ret;
}

static void __exit mfs_exit(void)
{
	unregister_filesystem(&mfs_fs_type);
	mfs_helper_comm_exit();
	mfs_inode_cache_destroy();
	pr_info("mfs: kernel module unloaded\n");
}

module_init(mfs_init);
module_exit(mfs_exit);

MODULE_DESCRIPTION("MooseFS kernel module with userspace helper transport");
MODULE_AUTHOR("Codex");
MODULE_LICENSE("GPL");
