/*
 * mfs_wire_defs.h — Userspace-safe MooseFS wire protocol constants.
 *
 * This header extracts the pure #define constants from the MooseFS protocol
 * so that the userspace helper daemon (mfskmod-helper) can be compiled
 * without pulling in kernel-only headers from mfs_proto.h.
 *
 * All values match MFSCommunication.h from MooseFS CE 3.x / 4.x.
 */
#ifndef _MFS_WIRE_DEFS_H_
#define _MFS_WIRE_DEFS_H_

/* ── Chunk / Block geometry ──────────────────────────────────────── */
#define MFSBLOCKSINCHUNK 0x400U
#define MFSCHUNKSIZE     0x04000000U   /* 64 MiB */
#define MFSBLOCKSIZE     0x10000U      /* 64 KiB */
#define MFSCRCEMPTY      0xD7978EEBU

/* ── ANY <-> ANY control packets ────────────────────────────────── */
#define ANTOAN_NOP                0
#define ANTOAN_UNKNOWN_COMMAND    1
#define ANTOAN_BAD_COMMAND_SIZE   2
#define ANTOAN_FORCE_TIMEOUT      5

#define MFS_ROOT_ID     1U
#define MFS_NAME_MAX    255U
#define MFS_SYMLINK_MAX 4096U
#define MFS_PATH_MAX    1024U
#define MFS_MAX_PACKETSIZE 50000000U

/* ── CLTOCS / CSTOCL – chunkserver data protocol ─────────────── */
#define CLTOCS_READ           200
#define CSTOCL_READ_STATUS    201
#define CSTOCL_READ_DATA      202
#define CLTOCS_WRITE          210
#define CSTOCL_WRITE_STATUS   211
#define CLTOCS_WRITE_DATA     212
#define CLTOCS_WRITE_FINISH   213

/* ── CLTOMA_FUSE_* – metadata opcodes (client → master) ────────── */
#define CLTOMA_FUSE_REGISTER              400
#define CLTOMA_FUSE_STATFS                402
#define CLTOMA_FUSE_ACCESS                404
#define CLTOMA_FUSE_LOOKUP                406
#define CLTOMA_FUSE_GETATTR               408
#define CLTOMA_FUSE_SETATTR               410
#define CLTOMA_FUSE_READLINK              412
#define CLTOMA_FUSE_SYMLINK               414
#define CLTOMA_FUSE_MKNOD                 416
#define CLTOMA_FUSE_MKDIR                 418
#define CLTOMA_FUSE_UNLINK                420
#define CLTOMA_FUSE_RMDIR                 422
#define CLTOMA_FUSE_RENAME                424
#define CLTOMA_FUSE_LINK                  426
#define CLTOMA_FUSE_READDIR               428
#define CLTOMA_FUSE_OPEN                  430
#define CLTOMA_FUSE_READ_CHUNK            432
#define CLTOMA_FUSE_WRITE_CHUNK           434
#define CLTOMA_FUSE_WRITE_CHUNK_END       436
#define CLTOMA_FUSE_APPEND_SLICE          438
#define CLTOMA_FUSE_CHECK                 440
#define CLTOMA_FUSE_GETTRASHRETENTION     442
#define CLTOMA_FUSE_SETTRASHRETENTION     444
#define CLTOMA_FUSE_GETSCLASS             446
#define CLTOMA_FUSE_SETSCLASS             448
#define CLTOMA_FUSE_GETTRASH              450
#define CLTOMA_FUSE_GETDETACHEDATTR       452
#define CLTOMA_FUSE_GETTRASHPATH          454
#define CLTOMA_FUSE_SETTRASHPATH          456
#define CLTOMA_FUSE_UNDEL                 458
#define CLTOMA_FUSE_PURGE                 460
#define CLTOMA_FUSE_GETDIRSTATS           462
#define CLTOMA_FUSE_TRUNCATE              464
#define CLTOMA_FUSE_REPAIR                466
#define CLTOMA_FUSE_SNAPSHOT              468
#define CLTOMA_FUSE_GETSUSTAINED          470
#define CLTOMA_FUSE_GETEATTR              472
#define CLTOMA_FUSE_SETEATTR              474
#define CLTOMA_FUSE_QUOTACONTROL          476
#define CLTOMA_FUSE_GETXATTR              478
#define CLTOMA_FUSE_SETXATTR              480
#define CLTOMA_FUSE_CREATE                482
#define CLTOMA_FUSE_PARENTS               484
#define CLTOMA_FUSE_PATHS                 486
#define CLTOMA_FUSE_GETFACL               488
#define CLTOMA_FUSE_SETFACL               490
#define CLTOMA_FUSE_FLOCK                 492
#define CLTOMA_FUSE_POSIX_LOCK            494
#define CLTOMA_FUSE_ARCHCTL               496
#define CLTOMA_FUSE_FSYNC                 498

/* ── MATOCL_FUSE_* – metadata responses (master → client) ──────── */
#define MATOCL_FUSE_REGISTER              401
#define MATOCL_FUSE_STATFS                403
#define MATOCL_FUSE_ACCESS                405
#define MATOCL_FUSE_LOOKUP                407
#define MATOCL_FUSE_GETATTR               409
#define MATOCL_FUSE_SETATTR               411
#define MATOCL_FUSE_READLINK              413
#define MATOCL_FUSE_SYMLINK               415
#define MATOCL_FUSE_MKNOD                 417
#define MATOCL_FUSE_MKDIR                 419
#define MATOCL_FUSE_UNLINK                421
#define MATOCL_FUSE_RMDIR                 423
#define MATOCL_FUSE_RENAME                425
#define MATOCL_FUSE_LINK                  427
#define MATOCL_FUSE_READDIR               429
#define MATOCL_FUSE_OPEN                  431
#define MATOCL_FUSE_READ_CHUNK            433
#define MATOCL_FUSE_WRITE_CHUNK           435
#define MATOCL_FUSE_WRITE_CHUNK_END       437
#define MATOCL_FUSE_APPEND_SLICE          439
#define MATOCL_FUSE_CHECK                 441
#define MATOCL_FUSE_GETTRASHRETENTION     443
#define MATOCL_FUSE_SETTRASHRETENTION     445
#define MATOCL_FUSE_GETSCLASS             447
#define MATOCL_FUSE_SETSCLASS             449
#define MATOCL_FUSE_GETTRASH              451
#define MATOCL_FUSE_GETDETACHEDATTR       453
#define MATOCL_FUSE_GETTRASHPATH          455
#define MATOCL_FUSE_SETTRASHPATH          457
#define MATOCL_FUSE_UNDEL                 459
#define MATOCL_FUSE_PURGE                 461
#define MATOCL_FUSE_GETDIRSTATS           463
#define MATOCL_FUSE_TRUNCATE              465
#define MATOCL_FUSE_REPAIR                467
#define MATOCL_FUSE_SNAPSHOT              469
#define MATOCL_FUSE_GETSUSTAINED          471
#define MATOCL_FUSE_GETEATTR              473
#define MATOCL_FUSE_SETEATTR              475
#define MATOCL_FUSE_QUOTACONTROL          477
#define MATOCL_FUSE_GETXATTR              479
#define MATOCL_FUSE_SETXATTR              481
#define MATOCL_FUSE_CREATE                483
#define MATOCL_FUSE_PARENTS               485
#define MATOCL_FUSE_PATHS                 487
#define MATOCL_FUSE_GETFACL               489
#define MATOCL_FUSE_SETFACL               491
#define MATOCL_FUSE_FLOCK                 493
#define MATOCL_FUSE_POSIX_LOCK            495
#define MATOCL_FUSE_ARCHCTL               497
#define MATOCL_FUSE_FSYNC                 499

/* ── Registration sub-commands ─────────────────────────────────── */
#define REGISTER_GETRANDOM     1
#define REGISTER_NEWSESSION    2
#define REGISTER_RECONNECT     3
#define REGISTER_TOOLS         4
#define REGISTER_NEWMETASESSION 5
#define REGISTER_CLOSESESSION  6

/* ── Readdir flags ─────────────────────────────────────────────── */
#define GETDIR_FLAG_WITHATTR   0x01
#define GETDIR_FLAG_ADDTOCACHE 0x02

/* ── Xattr modes ───────────────────────────────────────────────── */
#define MFS_XATTR_CREATE_OR_REPLACE 0
#define MFS_XATTR_CREATE_ONLY       1
#define MFS_XATTR_REPLACE_ONLY      2
#define MFS_XATTR_REMOVE            3
#define MFS_XATTR_GETA_DATA         0
#define MFS_XATTR_LENGTH_ONLY       1

/* ── Type codes (wire) ─────────────────────────────────────────── */
#define TYPE_FILE      1
#define TYPE_DIRECTORY 2
#define TYPE_SYMLINK   3
#define TYPE_FIFO      4
#define TYPE_BLOCKDEV  5
#define TYPE_CHARDEV   6
#define TYPE_SOCKET    7
#define TYPE_TRASH     8

/* Display-type aliases used by some MooseFS versions */
#define DISP_TYPE_FILE      '-'
#define DISP_TYPE_DIRECTORY 'd'
#define DISP_TYPE_SYMLINK   'l'
#define DISP_TYPE_FIFO      'f'
#define DISP_TYPE_BLOCKDEV  'b'
#define DISP_TYPE_CHARDEV   'c'
#define DISP_TYPE_SOCKET    's'

/* ── MooseFS error / status codes ──────────────────────────────── */
#define MFS_STATUS_OK               0
#define MFS_ERROR_EPERM             1
#define MFS_ERROR_ENOTDIR           2
#define MFS_ERROR_ENOENT            3
#define MFS_ERROR_EACCES            4
#define MFS_ERROR_EEXIST            5
#define MFS_ERROR_EINVAL            6
#define MFS_ERROR_ENOTEMPTY         7
#define MFS_ERROR_CHUNKLOST         8
#define MFS_ERROR_OUTOFMEMORY       9
#define MFS_ERROR_INDEXTOOBIG      10
#define MFS_ERROR_LOCKED           11
#define MFS_ERROR_NOCHUNKSERVERS   12
#define MFS_ERROR_NOCHUNK          13
#define MFS_ERROR_CHUNKBUSY        14
#define MFS_ERROR_REGISTER         15
#define MFS_ERROR_NOTDONE          16
#define MFS_ERROR_NOTOPENED        17
#define MFS_ERROR_NOTSTARTED       18
#define MFS_ERROR_WRONGVERSION     19
#define MFS_ERROR_CHUNKEXIST       20
#define MFS_ERROR_NOSPACE          21
#define MFS_ERROR_IO               22
#define MFS_ERROR_BNUMTOOBIG      23
#define MFS_ERROR_WRONGSIZE        24
#define MFS_ERROR_WRONGOFFSET      25
#define MFS_ERROR_CANTCONNECT      26
#define MFS_ERROR_WRONGCHUNKID     27
#define MFS_ERROR_DISCONNECTED     28
#define MFS_ERROR_CRC              29
#define MFS_ERROR_DELAYED          30
#define MFS_ERROR_CANTCREATEPATH   31
#define MFS_ERROR_MISMATCH         32
#define MFS_ERROR_EROFS            33
#define MFS_ERROR_QUOTA            34
#define MFS_ERROR_BADSESSIONID     35
#define MFS_ERROR_NOPASSWORD       36
#define MFS_ERROR_BADPASSWORD      37
#define MFS_ERROR_ENOATTR          38
#define MFS_ERROR_ENOTSUP          39
#define MFS_ERROR_ERANGE           40
#define MFS_ERROR_NOTFOUND         41
#define MFS_ERROR_ACTIVE           42
#define MFS_ERROR_CSNOTPRESENT     43
#define MFS_ERROR_WAITING          44
#define MFS_ERROR_EAGAIN           45
#define MFS_ERROR_EINTR            46
#define MFS_ERROR_ECANCELED        47
#define MFS_ERROR_ENOENT_NOCACHE   48
#define MFS_ERROR_ENAMETOOLONG     58
#define MFS_ERROR_EMLINK           59
#define MFS_ERROR_ETIMEDOUT        60
#define MFS_ERROR_EBADF            61
#define MFS_ERROR_EFBIG            62
#define MFS_ERROR_EISDIR           63
#define MFS_ERROR_MAX              64

/* ── SETATTR bitmask flags (setmask byte on the wire) ──────────── */
#define SET_WINATTR_FLAG      0x01
#define SET_MODE_FLAG         0x02
#define SET_UID_FLAG          0x04
#define SET_GID_FLAG          0x08
#define SET_MTIME_NOW_FLAG    0x10
#define SET_MTIME_FLAG        0x20
#define SET_ATIME_FLAG        0x40
#define SET_ATIME_NOW_FLAG    0x80

/* ── Rename mode flags ─────────────────────────────────────────── */
#define MFS_RENAME_STD        0
#define MFS_RENAME_NOREPLACE  1
#define MFS_RENAME_EXCHANGE   2

/* ── CRC polynomial (Castagnoli / CRC-32C) ─────────────────────── */
#define CRC_POLY 0xEDB88320U

/* ── Version constant for "any version" registration ───────────── */
#define VERSION_ANY 0U

/* ── Default ports ────────────────────────────────────────────── */
#define MFS_DEFAULT_MASTER_PORT 9421

/* ── FUSE register blobs ────────────────────────────────────────── */
/* ACL blob - required for modern MooseFS (CE and Pro) */
static const unsigned char FUSE_REGISTER_BLOB_ACL[64] =
	"DjI1GAQDULI5d2YjA26ypc3ovkhjvhciTQVx3CS4nYgtBoUcsljiVpsErJENHaw0";

/* NOACL blob - for compatibility with very old masters */
static const unsigned char FUSE_REGISTER_BLOB_NOACL[64] =
	"kFh9mdZsR84l5e675v8bi54VfXaXSYozaU3DSz9AsLLtOtKipzb9aQNkxeOISx64";

/* ── Version encoding for registration ──────────────────────────── */
/* MooseFS 4.58.3 */
#define MFS_VERSMAJ 4
#define MFS_VERSMID 58
#define MFS_VERSMIN 3

#endif /* _MFS_WIRE_DEFS_H_ */
