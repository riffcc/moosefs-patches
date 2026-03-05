#include "mfs.h"

static inline dev_t mfs_make_rdev(u32 rdev)
{
	return MKDEV((rdev >> 16) & 0xffff, rdev & 0xffff);
}

int mfs_moosefs_error_to_errno(s32 status)
{
	if (status <= 0)
		return status;

	switch (status) {
	case MFS_STATUS_OK:
		return 0;
	case MFS_ERROR_EPERM:
		return -EPERM;
	case MFS_ERROR_ENOTDIR:
		return -ENOTDIR;
	case MFS_ERROR_ENOENT:
		return -ENOENT;
	case MFS_ERROR_EACCES:
		return -EACCES;
	case MFS_ERROR_EEXIST:
		return -EEXIST;
	case MFS_ERROR_EINVAL:
		return -EINVAL;
	case MFS_ERROR_ENOTEMPTY:
		return -ENOTEMPTY;
	case MFS_ERROR_CHUNKLOST:
		return -ENXIO;
	case MFS_ERROR_NOCHUNKSERVERS:
		return -ENOSPC;
	case MFS_ERROR_IO:
		return -EIO;
	case MFS_ERROR_EROFS:
		return -EROFS;
	case MFS_ERROR_QUOTA:
		return -EDQUOT;
	case MFS_ERROR_ENOATTR:
#ifdef ENODATA
		return -ENODATA;
#else
		return -ENOENT;
#endif
	case MFS_ERROR_ENOTSUP:
		return -EOPNOTSUPP;
	case MFS_ERROR_ERANGE:
		return -ERANGE;
	case MFS_ERROR_CSNOTPRESENT:
		return -ENXIO;
	case MFS_ERROR_EAGAIN:
		return -EAGAIN;
	case MFS_ERROR_EINTR:
		return -EINTR;
	case MFS_ERROR_ECANCELED:
		return -ECANCELED;
	case MFS_ERROR_ENAMETOOLONG:
		return -ENAMETOOLONG;
	case MFS_ERROR_EMLINK:
		return -EMLINK;
	case MFS_ERROR_ETIMEDOUT:
		return -ETIMEDOUT;
	case MFS_ERROR_EBADF:
		return -EBADF;
	case MFS_ERROR_EFBIG:
		return -EFBIG;
	case MFS_ERROR_EISDIR:
		return -EISDIR;
	default:
		return -EINVAL;
	}
}

int mfs_wire_type_to_dtype(u8 wire_type)
{
	switch (wire_type) {
	case MFS_WIRE_TYPE_FILE:
		return DT_REG;
	case MFS_WIRE_TYPE_DIR:
		return DT_DIR;
	case MFS_WIRE_TYPE_SYMLINK:
		return DT_LNK;
	case MFS_WIRE_TYPE_FIFO:
		return DT_FIFO;
	case MFS_WIRE_TYPE_BLOCK:
		return DT_BLK;
	case MFS_WIRE_TYPE_CHAR:
		return DT_CHR;
	case MFS_WIRE_TYPE_SOCK:
		return DT_SOCK;
	default:
		return DT_UNKNOWN;
	}
}

umode_t mfs_wire_type_to_mode(u8 wire_type)
{
	switch (wire_type) {
	case MFS_WIRE_TYPE_FILE:
		return S_IFREG;
	case MFS_WIRE_TYPE_DIR:
		return S_IFDIR;
	case MFS_WIRE_TYPE_SYMLINK:
		return S_IFLNK;
	case MFS_WIRE_TYPE_FIFO:
		return S_IFIFO;
	case MFS_WIRE_TYPE_BLOCK:
		return S_IFBLK;
	case MFS_WIRE_TYPE_CHAR:
		return S_IFCHR;
	case MFS_WIRE_TYPE_SOCK:
		return S_IFSOCK;
	default:
		return 0;
	}
}

int mfs_wire_attr_to_kstat(u32 ino, const struct mfs_wire_attr *wa, struct kstat *st)
{
	umode_t type;

	if (!wa || !st)
		return -EINVAL;

	type = mfs_wire_type_to_mode(wa->type);
	if (!type)
		return -EIO;

	memset(st, 0, sizeof(*st));
	st->ino = ino;
	st->mode = type | (wa->mode & 07777);
	st->nlink = wa->nlink;
	st->uid = make_kuid(current_user_ns(), wa->uid);
	st->gid = make_kgid(current_user_ns(), wa->gid);
	st->size = wa->size;
	st->atime = ns_to_timespec64((u64)wa->atime * NSEC_PER_SEC);
	st->mtime = ns_to_timespec64((u64)wa->mtime * NSEC_PER_SEC);
	st->ctime = ns_to_timespec64((u64)wa->ctime * NSEC_PER_SEC);
	st->blksize = MFS_BLOCK_SIZE;
	st->blocks = DIV_ROUND_UP_ULL(wa->size, 512);
	if (type == S_IFBLK || type == S_IFCHR)
		st->rdev = mfs_make_rdev(wa->rdev);

	return 0;
}

void mfs_wire_attr_to_inode(struct inode *inode, const struct mfs_wire_attr *wa)
{
	umode_t type;

	if (!inode || !wa)
		return;

	type = mfs_wire_type_to_mode(wa->type);
	if (!type)
		type = S_IFREG;

	inode->i_mode = type | (wa->mode & 07777);
	i_uid_write(inode, wa->uid);
	i_gid_write(inode, wa->gid);
	set_nlink(inode, wa->nlink ? wa->nlink : 1);
	inode->i_size = wa->size;
	mfs_set_inode_atime(inode, wa->atime);
	mfs_set_inode_mtime(inode, wa->mtime);
	mfs_set_inode_ctime(inode, wa->ctime);
	inode->i_blocks = DIV_ROUND_UP_ULL(wa->size, 512);
	/* blksize_bits() may not be visible in all configs; ilog2 is equivalent
	 * for power-of-two block sizes and is always available. */
	inode->i_blkbits = ilog2(MFS_BLOCK_SIZE);

	if (type == S_IFBLK || type == S_IFCHR)
		inode->i_rdev = mfs_make_rdev(wa->rdev);

	MFS_I(inode)->known_size = wa->size;
	MFS_I(inode)->type = wa->type;
	MFS_I(inode)->attr_valid = 1;
}

void mfs_inode_to_wire_attr(const struct inode *inode, struct mfs_wire_attr *wa)
{
	if (!inode || !wa)
		return;

	memset(wa, 0, sizeof(*wa));
	switch (inode->i_mode & S_IFMT) {
	case S_IFREG:
		wa->type = MFS_WIRE_TYPE_FILE;
		break;
	case S_IFDIR:
		wa->type = MFS_WIRE_TYPE_DIR;
		break;
	case S_IFLNK:
		wa->type = MFS_WIRE_TYPE_SYMLINK;
		break;
	case S_IFIFO:
		wa->type = MFS_WIRE_TYPE_FIFO;
		break;
	case S_IFBLK:
		wa->type = MFS_WIRE_TYPE_BLOCK;
		break;
	case S_IFCHR:
		wa->type = MFS_WIRE_TYPE_CHAR;
		break;
	case S_IFSOCK:
		wa->type = MFS_WIRE_TYPE_SOCK;
		break;
	default:
		wa->type = MFS_WIRE_TYPE_UNKNOWN;
		break;
	}
	wa->mode = inode->i_mode & 07777;
	wa->uid = from_kuid(current_user_ns(), inode->i_uid);
	wa->gid = from_kgid(current_user_ns(), inode->i_gid);
	wa->size = i_size_read(inode);
	wa->atime = inode_get_atime_sec(inode);
	wa->mtime = inode_get_mtime_sec(inode);
	wa->ctime = inode_get_ctime_sec(inode);
	wa->nlink = inode->i_nlink;
	wa->rdev = new_encode_dev(inode->i_rdev);
}
