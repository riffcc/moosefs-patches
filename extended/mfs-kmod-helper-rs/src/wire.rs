//! MooseFS wire protocol constants and encoding/decoding.
//!
//! All values match MFSCommunication.h from MooseFS CE 4.x.

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Cursor, Read, Write};
use nix::libc;

// ── Chunk / Block geometry ──────────────────────────────────────────
pub const MFSBLOCKSINCHUNK: u32 = 0x400;
pub const MFSCHUNKSIZE: u64 = 0x04000000; // 64 MiB
pub const MFSBLOCKSIZE: u32 = 0x10000; // 64 KiB
pub const MFSCRCEMPTY: u32 = 0xD7978EEB;

pub const MFS_ROOT_ID: u32 = 1;
pub const MFS_NAME_MAX: u32 = 255;
pub const MFS_PATH_MAX: u32 = 1024;
pub const MFS_MAX_PACKETSIZE: u32 = 50_000_000;

// ── CLTOMA_FUSE_* opcodes (client → master) ─────────────────────────
pub const CLTOMA_FUSE_REGISTER: u32 = 400;
pub const CLTOMA_FUSE_STATFS: u32 = 402;
pub const CLTOMA_FUSE_LOOKUP: u32 = 406;
pub const CLTOMA_FUSE_GETATTR: u32 = 408;
pub const CLTOMA_FUSE_SETATTR: u32 = 410;
pub const CLTOMA_FUSE_READLINK: u32 = 412;
pub const CLTOMA_FUSE_SYMLINK: u32 = 414;
pub const CLTOMA_FUSE_MKDIR: u32 = 418;
pub const CLTOMA_FUSE_UNLINK: u32 = 420;
pub const CLTOMA_FUSE_RMDIR: u32 = 422;
pub const CLTOMA_FUSE_RENAME: u32 = 424;
pub const CLTOMA_FUSE_LINK: u32 = 426;
pub const CLTOMA_FUSE_READDIR: u32 = 428;
pub const CLTOMA_FUSE_READ_CHUNK: u32 = 432;
pub const CLTOMA_FUSE_WRITE_CHUNK: u32 = 434;
pub const CLTOMA_FUSE_WRITE_CHUNK_END: u32 = 436;
pub const CLTOMA_FUSE_TRUNCATE: u32 = 464;
pub const CLTOMA_FUSE_GETXATTR: u32 = 478;
pub const CLTOMA_FUSE_SETXATTR: u32 = 480;
pub const CLTOMA_FUSE_CREATE: u32 = 482;
pub const CLTOMA_FUSE_FSYNC: u32 = 498;

// ── MATOCL_FUSE_* responses (master → client) ──────────────────────
pub const MATOCL_FUSE_REGISTER: u32 = 401;
pub const MATOCL_FUSE_STATFS: u32 = 403;
pub const MATOCL_FUSE_LOOKUP: u32 = 407;
pub const MATOCL_FUSE_GETATTR: u32 = 409;
pub const MATOCL_FUSE_SETATTR: u32 = 411;
pub const MATOCL_FUSE_READLINK: u32 = 413;
pub const MATOCL_FUSE_SYMLINK: u32 = 415;
pub const MATOCL_FUSE_MKDIR: u32 = 419;
pub const MATOCL_FUSE_UNLINK: u32 = 421;
pub const MATOCL_FUSE_RMDIR: u32 = 423;
pub const MATOCL_FUSE_RENAME: u32 = 425;
pub const MATOCL_FUSE_LINK: u32 = 427;
pub const MATOCL_FUSE_READDIR: u32 = 429;
pub const MATOCL_FUSE_READ_CHUNK: u32 = 433;
pub const MATOCL_FUSE_WRITE_CHUNK: u32 = 435;
pub const MATOCL_FUSE_WRITE_CHUNK_END: u32 = 437;
pub const MATOCL_FUSE_TRUNCATE: u32 = 465;
pub const MATOCL_FUSE_GETXATTR: u32 = 479;
pub const MATOCL_FUSE_SETXATTR: u32 = 481;
pub const MATOCL_FUSE_CREATE: u32 = 483;
pub const MATOCL_FUSE_FSYNC: u32 = 499;

// ── CLTOCS / CSTOCL – chunkserver data protocol ─────────────────────
pub const CLTOCS_READ: u32 = 200;
pub const CSTOCL_READ_STATUS: u32 = 201;
pub const CSTOCL_READ_DATA: u32 = 202;
pub const CLTOCS_WRITE: u32 = 210;
pub const CSTOCL_WRITE_STATUS: u32 = 211;
pub const CLTOCS_WRITE_DATA: u32 = 212;
pub const CLTOCS_WRITE_FINISH: u32 = 213;

// ── Registration sub-commands ───────────────────────────────────────
pub const REGISTER_GETRANDOM: u8 = 1;
pub const REGISTER_NEWSESSION: u8 = 2;
pub const REGISTER_RECONNECT: u8 = 3;
pub const REGISTER_CLOSESESSION: u8 = 6;

// ── Readdir flags ───────────────────────────────────────────────────
pub const GETDIR_FLAG_WITHATTR: u32 = 0x01;
pub const GETDIR_FLAG_ADDTOCACHE: u32 = 0x02;

// ── Xattr modes ─────────────────────────────────────────────────────
pub const MFS_XATTR_CREATE_OR_REPLACE: u8 = 0;
pub const MFS_XATTR_CREATE_ONLY: u8 = 1;
pub const MFS_XATTR_REPLACE_ONLY: u8 = 2;
pub const MFS_XATTR_REMOVE: u8 = 3;
pub const MFS_XATTR_GETA_DATA: u8 = 0;
pub const MFS_XATTR_LENGTH_ONLY: u8 = 1;

// ── Rename mode flags ───────────────────────────────────────────────
pub const MFS_RENAME_STD: u32 = 0;
pub const MFS_RENAME_NOREPLACE: u32 = 1;
pub const MFS_RENAME_EXCHANGE: u32 = 2;

// ── SETATTR bitmask flags ───────────────────────────────────────────
pub const SET_MODE_FLAG: u8 = 0x01;
pub const SET_UID_FLAG: u8 = 0x02;
pub const SET_GID_FLAG: u8 = 0x04;
pub const SET_MTIME_FLAG: u8 = 0x08;
pub const SET_ATIME_FLAG: u8 = 0x10;
pub const SET_MTIME_NOW_FLAG: u8 = 0x40;
pub const SET_ATIME_NOW_FLAG: u8 = 0x80;

// ── MooseFS error codes ─────────────────────────────────────────────
pub const MFS_STATUS_OK: u8 = 0;
pub const MFS_ERROR_EPERM: u8 = 1;
pub const MFS_ERROR_ENOTDIR: u8 = 2;
pub const MFS_ERROR_ENOENT: u8 = 3;
pub const MFS_ERROR_EACCES: u8 = 4;
pub const MFS_ERROR_EEXIST: u8 = 5;
pub const MFS_ERROR_EINVAL: u8 = 6;
pub const MFS_ERROR_ENOTEMPTY: u8 = 7;
pub const MFS_ERROR_IO: u8 = 22;
pub const MFS_ERROR_NOSPACE: u8 = 21;
pub const MFS_ERROR_BADSESSIONID: u8 = 35;
pub const MFS_ERROR_ENOATTR: u8 = 38;
pub const MFS_ERROR_ENOTSUP: u8 = 39;
pub const MFS_ERROR_ERANGE: u8 = 40;
pub const MFS_ERROR_ENAMETOOLONG: u8 = 58;
pub const MFS_ERROR_EMLINK: u8 = 59;
pub const MFS_ERROR_EISDIR: u8 = 63;
pub const MFS_ERROR_MAX: u8 = 64;

// ── Version constant for registration ───────────────────────────────
pub const MFS_VERSMAJ: u16 = 4;
pub const MFS_VERSMID: u8 = 58;
pub const MFS_VERSMIN: u8 = 3;
pub const MFS_DEFAULT_MASTER_PORT: u16 = 9421;

// ── FUSE register ACL blob ──────────────────────────────────────────
pub const FUSE_REGISTER_BLOB_ACL: &[u8; 64] =
    b"DjI1GAQDULI5d2YjA26ypc3ovkhjvhciTQVx3CS4nYgtBoUcsljiVpsErJENHaw0";

/// Map MooseFS status code to libc errno.
pub fn mfs_status_to_errno(status: u8) -> i32 {
    match status {
        0 => 0, // MFS_STATUS_OK
        1 => libc::EPERM,
        2 => libc::ENOTDIR,
        3 => libc::ENOENT,
        4 => libc::EACCES,
        5 => libc::EEXIST,
        6 => libc::EINVAL,
        7 => libc::ENOTEMPTY,
        8 => libc::ENXIO,  // CHUNKLOST
        9 => libc::ENOMEM,
        10 => libc::EINVAL, // INDEXTOOBIG
        11 => libc::EAGAIN, // LOCKED
        12 => libc::ENOSPC, // NOCHUNKSERVERS
        13 => libc::ENOENT, // NOCHUNK
        14 => libc::EAGAIN, // CHUNKBUSY
        15 => libc::EACCES, // REGISTER
        21 => libc::ENOSPC,
        22 => libc::EIO,
        33 => libc::EROFS,
        34 => libc::EDQUOT,
        35 => libc::EACCES, // BADSESSIONID
        38 => libc::ENODATA, // ENOATTR
        39 => libc::ENOTSUP,
        40 => libc::ERANGE,
        58 => libc::ENAMETOOLONG,
        59 => libc::EMLINK,
        63 => libc::EISDIR,
        _ => libc::EIO,
    }
}

// ── Packet I/O ──────────────────────────────────────────────────────

/// Read exactly `n` bytes from a reader.
pub fn read_exact<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<()> {
    reader.read_exact(buf)
}

/// Write a MooseFS wire packet: 4-byte type + 4-byte length + data.
pub fn send_packet<W: Write>(writer: &mut W, pkt_type: u32, data: &[u8]) -> io::Result<()> {
    writer.write_u32::<BigEndian>(pkt_type)?;
    writer.write_u32::<BigEndian>(data.len() as u32)?;
    writer.write_all(data)?;
    writer.flush()
}

/// Receive a MooseFS wire packet. Returns (type, payload).
pub fn recv_packet<R: Read>(reader: &mut R) -> io::Result<(u32, Vec<u8>)> {
    let pkt_type = reader.read_u32::<BigEndian>()?;
    let pkt_len = reader.read_u32::<BigEndian>()?;
    if pkt_len > MFS_MAX_PACKETSIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("packet too large: {pkt_len}"),
        ));
    }
    let mut data = vec![0u8; pkt_len as usize];
    reader.read_exact(&mut data)?;
    Ok((pkt_type, data))
}

/// Parsed MooseFS attribute record.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MfsAttr {
    pub ftype: u8,
    pub mattr: u8,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub atime: u32,
    pub mtime: u32,
    pub ctime: u32,
    pub nlink: u32,
    pub size: u64,
    pub rdev: u32,
    pub winattr: u8,
}

/// Wire type constants matching `MFS_WIRE_TYPE_*`.
pub mod wire_type {
    pub const UNKNOWN: u8 = 0;
    pub const FILE: u8 = 1;
    pub const DIR: u8 = 2;
    pub const SYMLINK: u8 = 3;
    pub const FIFO: u8 = 4;
    pub const BLOCK: u8 = 5;
    pub const CHAR: u8 = 6;
    pub const SOCK: u8 = 7;
}

/// Display-type aliases used by some MooseFS versions.
fn disp_type_to_wire(dt: u8) -> u8 {
    match dt {
        b'-' => wire_type::FILE,
        b'd' => wire_type::DIR,
        b'l' => wire_type::SYMLINK,
        b'f' => wire_type::FIFO,
        b'b' => wire_type::BLOCK,
        b'c' => wire_type::CHAR,
        b's' => wire_type::SOCK,
        t if t <= 7 => t, // already a wire type
        _ => wire_type::UNKNOWN,
    }
}

/// Parse a MooseFS attribute record from a byte slice.
/// Returns the attr and the number of bytes consumed.
pub fn parse_attr(data: &[u8]) -> io::Result<(MfsAttr, usize)> {
    if data.len() < 35 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "attr too short",
        ));
    }

    let mut cur = Cursor::new(data);
    let first_byte = cur.read_u8()?;

    let (ftype, mode, mattr) = if first_byte < 64 {
        // New-style: flags(1) + mode(2)
        let flags = first_byte;
        let raw_mode = cur.read_u16::<BigEndian>()?;
        let t = ((raw_mode >> 12) & 0x0f) as u8;
        let m = raw_mode & 0x0fff;
        (t, m, flags)
    } else {
        // Old-style: display_type(1) + mode_with_flags(2)
        let t = disp_type_to_wire(first_byte & 0x7f);
        let raw = cur.read_u16::<BigEndian>()?;
        let m = raw & 0x0fff;
        let flags = ((raw >> 12) & 0x0f) as u8;
        (t, m, flags)
    };

    let uid = cur.read_u32::<BigEndian>()?;
    let gid = cur.read_u32::<BigEndian>()?;
    let atime = cur.read_u32::<BigEndian>()?;
    let mtime = cur.read_u32::<BigEndian>()?;
    let ctime = cur.read_u32::<BigEndian>()?;
    let nlink = cur.read_u32::<BigEndian>()?;

    let (size, rdev) = if ftype == wire_type::BLOCK || ftype == wire_type::CHAR {
        let pos = cur.position() as usize;
        if pos + 8 > data.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "attr truncated"));
        }
        let major = cur.read_u16::<BigEndian>()?;
        let minor = cur.read_u16::<BigEndian>()?;
        // Skip 4 padding bytes
        cur.read_u32::<BigEndian>()?;
        (0u64, ((major as u32) << 16) | minor as u32)
    } else {
        let pos = cur.position() as usize;
        if pos + 8 > data.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "attr truncated"));
        }
        let s = cur.read_u64::<BigEndian>()?;
        (s, 0u32)
    };

    let consumed = cur.position() as usize;

    Ok((
        MfsAttr {
            ftype,
            mattr,
            mode,
            uid,
            gid,
            atime,
            mtime,
            ctime,
            nlink,
            size,
            rdev,
            winattr: 0,
        },
        consumed,
    ))
}

/// Serialize an MfsAttr into the ctrl protocol wire format (for responses to kernel).
pub fn attr_to_ctrl_bytes(attr: &MfsAttr) -> [u8; 44] {
    let mut buf = [0u8; 44];
    buf[0] = attr.ftype;
    buf[1] = attr.mattr;
    buf[2] = (attr.mode >> 8) as u8;
    buf[3] = attr.mode as u8;
    let mut c = Cursor::new(&mut buf[4..]);
    let _ = c.write_u32::<BigEndian>(attr.uid);
    let _ = c.write_u32::<BigEndian>(attr.gid);
    let _ = c.write_u64::<BigEndian>(attr.size);
    let _ = c.write_u32::<BigEndian>(attr.atime);
    let _ = c.write_u32::<BigEndian>(attr.mtime);
    let _ = c.write_u32::<BigEndian>(attr.ctime);
    let _ = c.write_u32::<BigEndian>(attr.nlink);
    let _ = c.write_u32::<BigEndian>(attr.rdev);
    let _ = c.write_u8(attr.winattr);
    // 3 reserved bytes already zero
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mfs_status_to_errno() {
        assert_eq!(mfs_status_to_errno(MFS_STATUS_OK), 0);
        assert_eq!(mfs_status_to_errno(MFS_ERROR_EPERM), libc::EPERM);
        assert_eq!(mfs_status_to_errno(MFS_ERROR_ENOENT), libc::ENOENT);
        assert_eq!(mfs_status_to_errno(MFS_ERROR_EACCES), libc::EACCES);
        assert_eq!(mfs_status_to_errno(MFS_ERROR_EISDIR), libc::EISDIR);
        // Unknown error maps to EIO
        assert_eq!(mfs_status_to_errno(255), libc::EIO);
    }

    #[test]
    fn test_send_recv_packet() {
        let mut buf = Vec::new();
        send_packet(&mut buf, 408, &[1, 2, 3, 4]).unwrap();
        assert_eq!(buf.len(), 4 + 4 + 4); // type + len + data

        let mut cursor = Cursor::new(&buf);
        let (ptype, data) = recv_packet(&mut cursor).unwrap();
        assert_eq!(ptype, 408);
        assert_eq!(data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_parse_attr_new_style_dir() {
        // Construct a new-style attr for a directory:
        // flags=0, mode = (DIR << 12) | 0o755 = 0x21ED
        let mut data = Vec::new();
        data.push(0u8); // flags
        data.write_u16::<BigEndian>(0x21ED).unwrap(); // mode with type
        data.write_u32::<BigEndian>(0).unwrap(); // uid
        data.write_u32::<BigEndian>(0).unwrap(); // gid
        data.write_u32::<BigEndian>(1000).unwrap(); // atime
        data.write_u32::<BigEndian>(2000).unwrap(); // mtime
        data.write_u32::<BigEndian>(3000).unwrap(); // ctime
        data.write_u32::<BigEndian>(2).unwrap(); // nlink
        data.write_u64::<BigEndian>(4096).unwrap(); // size

        let (attr, consumed) = parse_attr(&data).unwrap();
        assert_eq!(attr.ftype, wire_type::DIR);
        assert_eq!(attr.mode, 0o755);
        assert_eq!(attr.nlink, 2);
        assert_eq!(attr.size, 4096);
        assert_eq!(consumed, 35);
    }

    #[test]
    fn test_parse_attr_old_style_dir() {
        // Old-style: display type 'd' for directory
        let mut data = Vec::new();
        data.push(b'd');
        // mode with flags: flags=0 in top 4 bits, mode=0o755 = 0x01ED
        data.write_u16::<BigEndian>(0x01ED).unwrap();
        data.write_u32::<BigEndian>(1000).unwrap(); // uid
        data.write_u32::<BigEndian>(1000).unwrap(); // gid
        data.write_u32::<BigEndian>(100).unwrap(); // atime
        data.write_u32::<BigEndian>(200).unwrap(); // mtime
        data.write_u32::<BigEndian>(300).unwrap(); // ctime
        data.write_u32::<BigEndian>(2).unwrap(); // nlink
        data.write_u64::<BigEndian>(4096).unwrap(); // size

        let (attr, consumed) = parse_attr(&data).unwrap();
        assert_eq!(attr.ftype, wire_type::DIR);
        assert_eq!(attr.mode, 0o755);
        assert_eq!(attr.uid, 1000);
        assert_eq!(attr.gid, 1000);
        assert_eq!(attr.size, 4096);
        assert_eq!(consumed, 35);
    }

    #[test]
    fn test_attr_to_ctrl_bytes_roundtrip() {
        let attr = MfsAttr {
            ftype: wire_type::DIR,
            mattr: 0,
            mode: 0o755,
            uid: 0,
            gid: 0,
            atime: 1000,
            mtime: 2000,
            ctime: 3000,
            nlink: 2,
            size: 4096,
            rdev: 0,
            winattr: 0,
        };
        let bytes = attr_to_ctrl_bytes(&attr);
        assert_eq!(bytes[0], wire_type::DIR);
        assert_eq!(bytes[1], 0); // mattr
        assert_eq!(u16::from_be_bytes([bytes[2], bytes[3]]), 0o755);
    }
}
