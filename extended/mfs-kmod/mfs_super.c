#include "mfs.h"
#include <linux/fs_context.h>
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 10, 0)
#include <linux/fs_parser.h>
#endif

/*
 * Compat: mount_nodev() was removed in 6.15, but the fs_context API
 * (init_fs_context / get_tree_nodev) has been available since ~5.x.
 * We switch to fs_context at 6.10 to get on the modern path early,
 * while keeping the legacy .mount path for Proxmox 8/9 kernels (6.8.x).
 */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 10, 0)
#define MFS_USE_FS_CONTEXT 1
#else
#define MFS_USE_FS_CONTEXT 0
#endif

#define MFS_BUILD_ID "2026-03-09-evict-inode-v1"

enum {
	Opt_master,
	Opt_port,
	Opt_subdir,
	Opt_uid,
	Opt_gid,
	Opt_password,
	Opt_err,
};

#if MFS_USE_FS_CONTEXT
/* New fs_parameter_spec for 6.10+ */
static const struct fs_parameter_spec mfs_fs_parameters[] = {
	fsparam_string("master",	Opt_master),
	fsparam_string("source",	Opt_master),
	fsparam_u32("port",		Opt_port),
	fsparam_string("subdir",	Opt_subdir),
	fsparam_u32("uid",		Opt_uid),
	fsparam_u32("gid",		Opt_gid),
	fsparam_string("password",	Opt_password),
	{}
};
#else
/* Legacy match_table for pre-6.10 kernels */
static const match_table_t mfs_tokens = {
	{ Opt_master, "master=%s" },
	{ Opt_port, "port=%u" },
	{ Opt_subdir, "subdir=%s" },
	{ Opt_uid, "uid=%u" },
	{ Opt_gid, "gid=%u" },
	{ Opt_password, "password=%s" },
	{ Opt_err, NULL },
};
#endif

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

static int mfs_show_devname(struct seq_file *m, struct dentry *root)
{
	struct mfs_sb_info *sbi = MFS_SB(root->d_sb);

	seq_printf(m, "%s:%u%s",
		   sbi->master_host[0] ? sbi->master_host : MFS_DEFAULT_MASTER_HOST,
		   sbi->master_port ? sbi->master_port : MFS_DEFAULT_MASTER_PORT,
		   sbi->subdir[0] ? sbi->subdir : MFS_DEFAULT_SUBDIR);
	return 0;
}

static int mfs_show_options(struct seq_file *m, struct dentry *root)
{
	struct mfs_sb_info *sbi = MFS_SB(root->d_sb);

	if (sbi->master_host[0])
		seq_printf(m, ",master=%s", sbi->master_host);
	if (sbi->master_port)
		seq_printf(m, ",port=%u", sbi->master_port);
	if (sbi->subdir[0] && strcmp(sbi->subdir, MFS_DEFAULT_SUBDIR) != 0)
		seq_printf(m, ",subdir=%s", sbi->subdir);
	if (!uid_eq(sbi->mount_uid, GLOBAL_ROOT_UID))
		seq_printf(m, ",uid=%u", from_kuid(current_user_ns(), sbi->mount_uid));
	if (!gid_eq(sbi->mount_gid, GLOBAL_ROOT_GID))
		seq_printf(m, ",gid=%u", from_kgid(current_user_ns(), sbi->mount_gid));
	if (sbi->password[0])
		seq_puts(m, ",password=***");
	return 0;
}

const struct super_operations mfs_super_ops = {
	.alloc_inode = mfs_alloc_inode,
	.free_inode = mfs_free_inode,
	.evict_inode = mfs_evict_inode,
	.statfs = mfs_statfs,
	.show_devname = mfs_show_devname,
	.show_options = mfs_show_options,
	/* In modern kernels, default drop_inode behaviour is fine */
};

/* Apply defaults for any unset mount options */
static void mfs_apply_defaults(struct mfs_sb_info *sbi)
{
	pr_info("mfs: apply_defaults: BEFORE master_host='%s' port=%u subdir='%s'\n",
		sbi->master_host, sbi->master_port, sbi->subdir);
	if (sbi->subdir[0] == '\0')
		strscpy(sbi->subdir, MFS_DEFAULT_SUBDIR, sizeof(sbi->subdir));
	if (sbi->master_host[0] == '\0')
		strscpy(sbi->master_host, MFS_DEFAULT_MASTER_HOST,
			sizeof(sbi->master_host));
	if (!sbi->master_port)
		sbi->master_port = MFS_DEFAULT_MASTER_PORT;
	pr_info("mfs: apply_defaults: AFTER master_host='%s' port=%u subdir='%s'\n",
		sbi->master_host, sbi->master_port, sbi->subdir);
}

/* Parse "host:port" combined value into sbi */
static int mfs_parse_master_value(struct mfs_sb_info *sbi, const char *val)
{
	char tmp[256];
	char *colon;

	strscpy(tmp, val, sizeof(tmp));
	colon = strrchr(tmp, ':');
	if (colon) {
		unsigned int port;
		*colon++ = '\0';
		if (kstrtouint(colon, 10, &port) == 0 && port <= 65535)
			sbi->master_port = (u16)port;
	}
	strscpy(sbi->master_host, tmp, sizeof(sbi->master_host));
	return 0;
}

static int mfs_parse_source_value(struct mfs_sb_info *sbi, const char *val)
{
	char tmp[256];
	char *subdir;
	char *colon;

	if (!val || !*val)
		return -EINVAL;

	strscpy(tmp, val, sizeof(tmp));
	subdir = strchr(tmp, '/');
	if (subdir) {
		*subdir++ = '\0';
		if (*subdir == '\0')
			strscpy(sbi->subdir, "/", sizeof(sbi->subdir));
		else
			snprintf(sbi->subdir, sizeof(sbi->subdir), "/%s", subdir);
	}

	colon = strrchr(tmp, ':');
	if (colon) {
		unsigned int port;

		*colon++ = '\0';
		if (kstrtouint(colon, 10, &port) == 0 && port <= 65535)
			sbi->master_port = (u16)port;
	}

	strscpy(sbi->master_host, tmp, sizeof(sbi->master_host));
	return 0;
}

#if MFS_USE_FS_CONTEXT
/* New-style per-parameter parsing for kernel 6.10+ */
static int mfs_parse_param(struct fs_context *fc, struct fs_parameter *param)
{
	struct mfs_sb_info *sbi = fc->fs_private;
	struct fs_parse_result result;
	int opt;

	pr_info("mfs: parse_param: key='%s' type=%d\n", param->key, param->type);
	if (param->type == fs_value_is_string && param->string)
		pr_info("mfs: parse_param:   string='%s'\n", param->string);

	opt = fs_parse(fc, mfs_fs_parameters, param, &result);
	if (opt < 0) {
		pr_info("mfs: parse_param: fs_parse returned %d\n", opt);
		return opt;
	}

	switch (opt) {
	case Opt_master:
		pr_info("mfs: parse_param: Opt_master val='%s'\n", param->string);
		if (strcmp(param->key, "source") == 0) {
			kfree(fc->source);
			fc->source = kstrdup(param->string, GFP_KERNEL);
			if (!fc->source)
				return -ENOMEM;
			return mfs_parse_source_value(sbi, param->string);
		}
		return mfs_parse_master_value(sbi, param->string);
	case Opt_port:
		if (result.uint_32 > 65535)
			return -EINVAL;
		sbi->master_port = (u16)result.uint_32;
		break;
	case Opt_subdir:
		strscpy(sbi->subdir, param->string, sizeof(sbi->subdir));
		break;
	case Opt_uid:
		sbi->mount_uid = make_kuid(current_user_ns(), result.uint_32);
		if (!uid_valid(sbi->mount_uid))
			return -EINVAL;
		break;
	case Opt_gid:
		sbi->mount_gid = make_kgid(current_user_ns(), result.uint_32);
		if (!gid_valid(sbi->mount_gid))
			return -EINVAL;
		break;
	case Opt_password:
		strscpy(sbi->password, param->string, sizeof(sbi->password));
		break;
	default:
		return -EINVAL;
	}
	return 0;
}
#else
/* Legacy string-based option parsing for pre-6.10 kernels */
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
			if (!host)
				return -ENOMEM;
			mfs_parse_master_value(sbi, host);
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

	mfs_apply_defaults(sbi);
	return 0;
}
#endif

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
	if (ret) {
		pr_err("mfs: register_session: mfs_call_checked failed ret=%d\n", ret);
		return ret;
	}
	pr_info("mfs: register_session: raw_len=%u expected>=%zu\n",
		raw_len, sizeof(*rsp));
	if (raw_len < sizeof(*rsp)) {
		pr_err("mfs: register_session: short response %u < %zu\n",
		       raw_len, sizeof(*rsp));
		kfree(raw);
		return -EIO;
	}

	rsp = raw;
	sbi->session_id = rsp->session_id;
	*root_ino = rsp->root_inode ? rsp->root_inode : 1;
	memcpy(root_attr, &rsp->root_attr, sizeof(*root_attr));
	pr_info("mfs: register_session: session=%u root_ino=%u type=%u mode=0%o uid=%u gid=%u nlink=%u\n",
		sbi->session_id, *root_ino,
		root_attr->type, root_attr->mode,
		root_attr->uid, root_attr->gid, root_attr->nlink);
	kfree(raw);
	return 0;
}

static int mfs_fill_super_common(struct super_block *sb, struct mfs_sb_info *sbi)
{
	struct inode *root_inode;
	struct mfs_wire_attr root_attr;
	u32 root_ino;
	int ret;

	if (!try_module_get(THIS_MODULE))
		return -ENODEV;

	mfs_cache_init(&sbi->chunk_cache);
	mfs_apply_defaults(sbi);

	sb->s_magic = MFS_SUPER_MAGIC;
	sb->s_fs_info = sbi;
	sb->s_time_gran = 1;
	sb->s_op = &mfs_super_ops;
	sb->s_maxbytes = MAX_LFS_FILESIZE;
	sb->s_xattr = mfs_xattr_handlers;

	/*
	 * Allocate a writeback-capable backing_dev_info.  Without this
	 * the superblock inherits noop_backing_dev_info which has
	 * BDI_CAP_NO_WRITEBACK, so the kernel writeback path will
	 * never call our .writepage.
	 */
	ret = super_setup_bdi_name(sb, "mfs-%u", sbi->master_port);
	if (ret) {
		pr_err("mfs: fill_super: super_setup_bdi_name failed ret=%d\n", ret);
		goto err;
	}
	/*
	 * Let the kernel issue sequential readahead instead of forcing every
	 * buffered read through single-page faulting.  256 pages is 1 MiB on
	 * 4 KiB-page kernels and is a reasonable first network-filesystem
	 * window without getting too aggressive.
	 */
	sb->s_bdi->ra_pages = 256;

	ret = mfs_register_session(sb, &root_ino, &root_attr);
	if (ret) {
		pr_err("mfs: fill_super: register_session failed ret=%d\n", ret);
		goto err;
	}
	pr_info("mfs: fill_super: register OK, root_ino=%u\n", root_ino);

	root_inode = mfs_iget_with_attr(sb, root_ino, &root_attr);
	if (IS_ERR(root_inode)) {
		ret = PTR_ERR(root_inode);
		pr_err("mfs: fill_super: mfs_iget_with_attr failed ret=%d\n", ret);
		goto err;
	}
	pr_info("mfs: fill_super: root inode mode=0%o uid=%u gid=%u nlink=%u size=%lld\n",
		root_inode->i_mode, i_uid_read(root_inode),
		i_gid_read(root_inode), root_inode->i_nlink,
		(long long)root_inode->i_size);

	sb->s_root = d_make_root(root_inode);
	if (!sb->s_root) {
		ret = -ENOMEM;
		pr_err("mfs: fill_super: d_make_root failed\n");
		goto err;
	}

	pr_info("mfs: fill_super: SUCCESS\n");
	return 0;

err:
	module_put(THIS_MODULE);
	mfs_cache_destroy(&sbi->chunk_cache);
	kfree(sbi);
	sb->s_fs_info = NULL;
	return ret;
}

#if !MFS_USE_FS_CONTEXT
/* Legacy path: options come as raw string in data */
static int mfs_fill_super(struct super_block *sb, void *data, int silent)
{
	struct mfs_sb_info *sbi;
	int ret;

	(void)silent;

	sbi = kzalloc(sizeof(*sbi), GFP_KERNEL);
	if (!sbi)
		return -ENOMEM;

	sbi->mount_uid = current_fsuid();
	sbi->mount_gid = current_fsgid();

	ret = mfs_parse_options(sbi, data);
	if (ret) {
		kfree(sbi);
		return ret;
	}

	return mfs_fill_super_common(sb, sbi);
}
#endif

/* ---- New mount API (kernel >= 6.10) ---- */
#if MFS_USE_FS_CONTEXT

static int mfs_fill_super_fc(struct super_block *sb, struct fs_context *fc)
{
	struct mfs_sb_info *sbi = fc->fs_private;

	/* Transfer ownership of sbi to the superblock */
	fc->fs_private = NULL;
	return mfs_fill_super_common(sb, sbi);
}

static int mfs_get_tree(struct fs_context *fc)
{
	return get_tree_nodev(fc, mfs_fill_super_fc);
}

static void mfs_fc_free(struct fs_context *fc)
{
	kfree(fc->fs_private);
}

static const struct fs_context_operations mfs_fc_ops = {
	.parse_param = mfs_parse_param,
	.get_tree = mfs_get_tree,
	.free = mfs_fc_free,
};

static int mfs_init_fs_context(struct fs_context *fc)
{
	struct mfs_sb_info *sbi;

	sbi = kzalloc(sizeof(*sbi), GFP_KERNEL);
	if (!sbi)
		return -ENOMEM;

	sbi->mount_uid = current_fsuid();
	sbi->mount_gid = current_fsgid();

	fc->fs_private = sbi;
	fc->ops = &mfs_fc_ops;
	return 0;
}

#else /* ---- Legacy mount API (kernel < 6.10, Proxmox 8/9) ---- */

static struct dentry *mfs_mount(struct file_system_type *fs_type,
				int flags, const char *dev_name,
				void *data)
{
	struct mfs_sb_info tmp = { 0 };

	if (dev_name && *dev_name) {
		tmp.mount_uid = current_fsuid();
		tmp.mount_gid = current_fsgid();
		mfs_parse_source_value(&tmp, dev_name);
	}
	return mount_nodev(fs_type, flags, data, mfs_fill_super);
}

#endif /* MFS_USE_FS_CONTEXT */

static void mfs_kill_sb(struct super_block *sb)
{
	struct mfs_sb_info *sbi = MFS_SB(sb);

	kill_anon_super(sb);
	if (sbi) {
		mfs_cache_destroy(&sbi->chunk_cache);
		kfree(sbi);
	}
	module_put(THIS_MODULE);
}

struct file_system_type mfs_fs_type = {
	.owner = THIS_MODULE,
	.name = "mfs",
#if MFS_USE_FS_CONTEXT
	.init_fs_context = mfs_init_fs_context,
#else
	.mount = mfs_mount,
#endif
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

	pr_info("mfs: kernel module loaded build=%s\n", MFS_BUILD_ID);
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
	pr_info("mfs: kernel module unloaded build=%s\n", MFS_BUILD_ID);
}

module_init(mfs_init);
module_exit(mfs_exit);

MODULE_DESCRIPTION("MooseFS kernel module with userspace helper transport");
MODULE_AUTHOR("Riff Labs");
MODULE_LICENSE("GPL");
MODULE_VERSION(MFS_BUILD_ID);
