//! Kernel ↔ helper control protocol types.
//!
//! These mirror the C structures in `mfs_ctrl_proto.h` and define the
//! binary protocol spoken over `/dev/mfs_ctrl`.

use std::io::{self, Cursor};

use byteorder::{NativeEndian, ReadBytesExt};

/// Magic value for ctrl protocol frames.
pub const MFS_CTRL_MAGIC: u32 = 0x4d465343; // "MFSC"
pub const MFS_CTRL_VERSION: u32 = 1;
pub const MFS_CTRL_FLAG_REQUEST: u16 = 0x0001;
pub const MFS_CTRL_FLAG_RESPONSE: u16 = 0x0002;
pub const MFS_CTRL_MAX_PAYLOAD: u32 = 2 * 1024 * 1024;

/// Control protocol opcodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum CtrlOp {
    Hello = 1,
    Register = 2,
    Lookup = 3,
    Getattr = 4,
    Setattr = 5,
    Truncate = 6,
    Readdir = 7,
    Create = 8,
    Unlink = 9,
    Link = 10,
    Rename = 11,
    Mkdir = 12,
    Rmdir = 13,
    Readlink = 14,
    Symlink = 15,
    Getxattr = 16,
    Setxattr = 17,
    Listxattr = 18,
    Removexattr = 19,
    Read = 20,
    Write = 21,
    Fsync = 22,
    Statfs = 23,
}

impl CtrlOp {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            1 => Some(Self::Hello),
            2 => Some(Self::Register),
            3 => Some(Self::Lookup),
            4 => Some(Self::Getattr),
            5 => Some(Self::Setattr),
            6 => Some(Self::Truncate),
            7 => Some(Self::Readdir),
            8 => Some(Self::Create),
            9 => Some(Self::Unlink),
            10 => Some(Self::Link),
            11 => Some(Self::Rename),
            12 => Some(Self::Mkdir),
            13 => Some(Self::Rmdir),
            14 => Some(Self::Readlink),
            15 => Some(Self::Symlink),
            16 => Some(Self::Getxattr),
            17 => Some(Self::Setxattr),
            18 => Some(Self::Listxattr),
            19 => Some(Self::Removexattr),
            20 => Some(Self::Read),
            21 => Some(Self::Write),
            22 => Some(Self::Fsync),
            23 => Some(Self::Statfs),
            _ => None,
        }
    }
}

/// Ctrl protocol frame header (24 bytes, packed, native endian).
#[derive(Debug, Clone, Copy)]
#[repr(C, packed)]
pub struct CtrlHdr {
    pub magic: u32,
    pub version: u32,
    pub req_id: u32,
    pub op: u16,
    pub flags: u16,
    pub status: i32,
    pub payload_len: u32,
}

impl CtrlHdr {
    pub const SIZE: usize = 24;

    pub fn from_bytes(data: &[u8]) -> io::Result<Self> {
        if data.len() < Self::SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ctrl header too short",
            ));
        }
        // These are native-endian packed structs (kernel writes them directly)
        let mut c = Cursor::new(data);
        Ok(Self {
            magic: c.read_u32::<NativeEndian>()?,
            version: c.read_u32::<NativeEndian>()?,
            req_id: c.read_u32::<NativeEndian>()?,
            op: c.read_u16::<NativeEndian>()?,
            flags: c.read_u16::<NativeEndian>()?,
            status: c.read_i32::<NativeEndian>()?,
            payload_len: c.read_u32::<NativeEndian>()?,
        })
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        // Safe: packed repr(C) struct, all fields are plain integers
        unsafe { std::mem::transmute_copy(self) }
    }

    pub fn response(req: &Self, status: i32, payload_len: u32) -> Self {
        Self {
            magic: MFS_CTRL_MAGIC,
            version: MFS_CTRL_VERSION,
            req_id: req.req_id,
            op: req.op,
            flags: MFS_CTRL_FLAG_RESPONSE,
            status,
            payload_len,
        }
    }
}

/// Parsed register request.
#[derive(Debug)]
pub struct RegisterReq {
    pub master_host: String,
    pub master_port: u16,
    pub subdir: String,
    pub password: String,
    pub mount_uid: u32,
    pub mount_gid: u32,
    pub flags: u16,
}

impl RegisterReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "register req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        let master_len = c.read_u16::<NativeEndian>()? as usize;
        let subdir_len = c.read_u16::<NativeEndian>()? as usize;
        let password_len = c.read_u16::<NativeEndian>()? as usize;
        let _reserved = c.read_u16::<NativeEndian>()?;
        let master_port = c.read_u16::<NativeEndian>()?;
        let flags = c.read_u16::<NativeEndian>()?;
        let mount_uid = c.read_u32::<NativeEndian>()?;
        let mount_gid = c.read_u32::<NativeEndian>()?;

        let pos = c.position() as usize;
        let tail = &payload[pos..];
        if tail.len() < master_len + subdir_len + password_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "register req truncated",
            ));
        }

        let master_host =
            String::from_utf8_lossy(&tail[..master_len]).to_string();
        let subdir =
            String::from_utf8_lossy(&tail[master_len..master_len + subdir_len]).to_string();
        let password = String::from_utf8_lossy(
            &tail[master_len + subdir_len..master_len + subdir_len + password_len],
        )
        .to_string();

        Ok(Self {
            master_host,
            master_port,
            subdir,
            password,
            mount_uid,
            mount_gid,
            flags,
        })
    }
}

/// Inode request (getattr, unlink, rmdir share this shape).
#[derive(Debug, Clone, Copy)]
pub struct InodeReq {
    pub session_id: u32,
    pub inode: u32,
    pub uid: u32,
    pub gid: u32,
}

impl InodeReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inode req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        Ok(Self {
            session_id: c.read_u32::<NativeEndian>()?,
            inode: c.read_u32::<NativeEndian>()?,
            uid: c.read_u32::<NativeEndian>()?,
            gid: c.read_u32::<NativeEndian>()?,
        })
    }
}

/// Lookup request.
#[derive(Debug)]
pub struct LookupReq {
    pub session_id: u32,
    pub parent_inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub name: Vec<u8>,
}

impl LookupReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 20 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "lookup req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        let session_id = c.read_u32::<NativeEndian>()?;
        let parent_inode = c.read_u32::<NativeEndian>()?;
        let uid = c.read_u32::<NativeEndian>()?;
        let gid = c.read_u32::<NativeEndian>()?;
        let name_len = c.read_u16::<NativeEndian>()? as usize;
        let _reserved = c.read_u16::<NativeEndian>()?;

        let pos = c.position() as usize;
        if pos + name_len > payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "lookup name truncated",
            ));
        }
        let name = payload[pos..pos + name_len].to_vec();

        Ok(Self {
            session_id,
            parent_inode,
            uid,
            gid,
            name,
        })
    }
}

/// Create request.
#[derive(Debug)]
pub struct CreateReq {
    pub session_id: u32,
    pub parent_inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub name: Vec<u8>,
}

impl CreateReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 24 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "create req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        let session_id = c.read_u32::<NativeEndian>()?;
        let parent_inode = c.read_u32::<NativeEndian>()?;
        let uid = c.read_u32::<NativeEndian>()?;
        let gid = c.read_u32::<NativeEndian>()?;
        let mode = c.read_u32::<NativeEndian>()?;
        let name_len = c.read_u16::<NativeEndian>()? as usize;
        let _reserved = c.read_u16::<NativeEndian>()?;

        let pos = c.position() as usize;
        if pos + name_len > payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "create name truncated",
            ));
        }
        let name = payload[pos..pos + name_len].to_vec();

        Ok(Self {
            session_id,
            parent_inode,
            uid,
            gid,
            mode,
            name,
        })
    }
}

/// Mkdir request (same shape as create).
pub type MkdirReq = CreateReq;

/// Readdir request.
#[derive(Debug, Clone, Copy)]
pub struct ReaddirReq {
    pub session_id: u32,
    pub inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub offset: u64,
    pub max_entries: u32,
    pub flags: u32,
}

impl ReaddirReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "readdir req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        Ok(Self {
            session_id: c.read_u32::<NativeEndian>()?,
            inode: c.read_u32::<NativeEndian>()?,
            uid: c.read_u32::<NativeEndian>()?,
            gid: c.read_u32::<NativeEndian>()?,
            offset: c.read_u64::<NativeEndian>()?,
            max_entries: c.read_u32::<NativeEndian>()?,
            flags: c.read_u32::<NativeEndian>()?,
        })
    }
}

/// Read data request.
#[derive(Debug, Clone, Copy)]
pub struct ReadReq {
    pub session_id: u32,
    pub inode: u32,
    pub offset: u64,
    pub size: u32,
    pub flags: u32,
}

impl ReadReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 24 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "read req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        Ok(Self {
            session_id: c.read_u32::<NativeEndian>()?,
            inode: c.read_u32::<NativeEndian>()?,
            offset: c.read_u64::<NativeEndian>()?,
            size: c.read_u32::<NativeEndian>()?,
            flags: c.read_u32::<NativeEndian>()?,
        })
    }
}

/// Write data request (header only — data follows in payload).
#[derive(Debug, Clone, Copy)]
pub struct WriteReq {
    pub session_id: u32,
    pub inode: u32,
    pub offset: u64,
    pub size: u32,
    pub flags: u32,
}

impl WriteReq {
    pub const HEADER_SIZE: usize = 24;

    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < Self::HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "write req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        Ok(Self {
            session_id: c.read_u32::<NativeEndian>()?,
            inode: c.read_u32::<NativeEndian>()?,
            offset: c.read_u64::<NativeEndian>()?,
            size: c.read_u32::<NativeEndian>()?,
            flags: c.read_u32::<NativeEndian>()?,
        })
    }
}

/// Setattr request.
#[derive(Debug, Clone, Copy)]
pub struct SetattrReq {
    pub session_id: u32,
    pub inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub valid: u32,
    pub mode: u32,
    pub attr_uid: u32,
    pub attr_gid: u32,
    pub atime_ns: u64,
    pub mtime_ns: u64,
    pub ctime_ns: u64,
}

impl SetattrReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 56 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "setattr req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        Ok(Self {
            session_id: c.read_u32::<NativeEndian>()?,
            inode: c.read_u32::<NativeEndian>()?,
            uid: c.read_u32::<NativeEndian>()?,
            gid: c.read_u32::<NativeEndian>()?,
            valid: c.read_u32::<NativeEndian>()?,
            mode: c.read_u32::<NativeEndian>()?,
            attr_uid: c.read_u32::<NativeEndian>()?,
            attr_gid: c.read_u32::<NativeEndian>()?,
            atime_ns: c.read_u64::<NativeEndian>()?,
            mtime_ns: c.read_u64::<NativeEndian>()?,
            ctime_ns: c.read_u64::<NativeEndian>()?,
        })
    }
}

/// Truncate request.
#[derive(Debug, Clone, Copy)]
pub struct TruncateReq {
    pub session_id: u32,
    pub inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
}

impl TruncateReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 24 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncate req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        Ok(Self {
            session_id: c.read_u32::<NativeEndian>()?,
            inode: c.read_u32::<NativeEndian>()?,
            uid: c.read_u32::<NativeEndian>()?,
            gid: c.read_u32::<NativeEndian>()?,
            size: c.read_u64::<NativeEndian>()?,
        })
    }
}

/// Unlink/rmdir request (has name in tail).
#[derive(Debug)]
pub struct UnlinkReq {
    pub session_id: u32,
    pub parent_inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub name: Vec<u8>,
}

impl UnlinkReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 20 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unlink req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        let session_id = c.read_u32::<NativeEndian>()?;
        let parent_inode = c.read_u32::<NativeEndian>()?;
        let uid = c.read_u32::<NativeEndian>()?;
        let gid = c.read_u32::<NativeEndian>()?;
        let name_len = c.read_u16::<NativeEndian>()? as usize;
        let _reserved = c.read_u16::<NativeEndian>()?;
        let pos = c.position() as usize;
        if pos + name_len > payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unlink name truncated",
            ));
        }
        let name = payload[pos..pos + name_len].to_vec();
        Ok(Self {
            session_id,
            parent_inode,
            uid,
            gid,
            name,
        })
    }
}

/// Rename request.
#[derive(Debug)]
pub struct RenameReq {
    pub session_id: u32,
    pub old_parent: u32,
    pub new_parent: u32,
    pub uid: u32,
    pub gid: u32,
    pub flags: u32,
    pub old_name: Vec<u8>,
    pub new_name: Vec<u8>,
}

impl RenameReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 28 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "rename req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        let session_id = c.read_u32::<NativeEndian>()?;
        let old_parent = c.read_u32::<NativeEndian>()?;
        let new_parent = c.read_u32::<NativeEndian>()?;
        let uid = c.read_u32::<NativeEndian>()?;
        let gid = c.read_u32::<NativeEndian>()?;
        let flags = c.read_u32::<NativeEndian>()?;
        let old_name_len = c.read_u16::<NativeEndian>()? as usize;
        let new_name_len = c.read_u16::<NativeEndian>()? as usize;
        let pos = c.position() as usize;
        if pos + old_name_len + new_name_len > payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "rename names truncated",
            ));
        }
        let old_name = payload[pos..pos + old_name_len].to_vec();
        let new_name = payload[pos + old_name_len..pos + old_name_len + new_name_len].to_vec();
        Ok(Self {
            session_id,
            old_parent,
            new_parent,
            uid,
            gid,
            flags,
            old_name,
            new_name,
        })
    }
}

/// Link request.
#[derive(Debug)]
pub struct LinkReq {
    pub session_id: u32,
    pub inode: u32,
    pub new_parent: u32,
    pub uid: u32,
    pub gid: u32,
    pub name: Vec<u8>,
}

impl LinkReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 24 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "link req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        let session_id = c.read_u32::<NativeEndian>()?;
        let inode = c.read_u32::<NativeEndian>()?;
        let new_parent = c.read_u32::<NativeEndian>()?;
        let uid = c.read_u32::<NativeEndian>()?;
        let gid = c.read_u32::<NativeEndian>()?;
        let name_len = c.read_u16::<NativeEndian>()? as usize;
        let _reserved = c.read_u16::<NativeEndian>()?;
        let pos = c.position() as usize;
        if pos + name_len > payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "link name truncated",
            ));
        }
        let name = payload[pos..pos + name_len].to_vec();
        Ok(Self {
            session_id,
            inode,
            new_parent,
            uid,
            gid,
            name,
        })
    }
}

/// Symlink request.
#[derive(Debug)]
pub struct SymlinkReq {
    pub session_id: u32,
    pub parent_inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub name: Vec<u8>,
    pub target: Vec<u8>,
}

impl SymlinkReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 20 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "symlink req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        let session_id = c.read_u32::<NativeEndian>()?;
        let parent_inode = c.read_u32::<NativeEndian>()?;
        let uid = c.read_u32::<NativeEndian>()?;
        let gid = c.read_u32::<NativeEndian>()?;
        let name_len = c.read_u16::<NativeEndian>()? as usize;
        let target_len = c.read_u16::<NativeEndian>()? as usize;
        let pos = c.position() as usize;
        if pos + name_len + target_len > payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "symlink data truncated",
            ));
        }
        let name = payload[pos..pos + name_len].to_vec();
        let target = payload[pos + name_len..pos + name_len + target_len].to_vec();
        Ok(Self {
            session_id,
            parent_inode,
            uid,
            gid,
            name,
            target,
        })
    }
}

/// Xattr request.
#[derive(Debug)]
pub struct XattrReq {
    pub session_id: u32,
    pub inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub flags: u32,
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

impl XattrReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 24 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "xattr req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        let session_id = c.read_u32::<NativeEndian>()?;
        let inode = c.read_u32::<NativeEndian>()?;
        let uid = c.read_u32::<NativeEndian>()?;
        let gid = c.read_u32::<NativeEndian>()?;
        let flags = c.read_u32::<NativeEndian>()?;
        let name_len = c.read_u16::<NativeEndian>()? as usize;
        let _reserved = c.read_u16::<NativeEndian>()?;
        let value_len = c.read_u32::<NativeEndian>()? as usize;
        let pos = c.position() as usize;
        if pos + name_len + value_len > payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "xattr data truncated",
            ));
        }
        let name = payload[pos..pos + name_len].to_vec();
        let value = payload[pos + name_len..pos + name_len + value_len].to_vec();
        Ok(Self {
            session_id,
            inode,
            uid,
            gid,
            flags,
            name,
            value,
        })
    }
}

/// Fsync request.
#[derive(Debug, Clone, Copy)]
pub struct FsyncReq {
    pub session_id: u32,
    pub inode: u32,
    pub datasync: u32,
}

impl FsyncReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 12 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "fsync req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        Ok(Self {
            session_id: c.read_u32::<NativeEndian>()?,
            inode: c.read_u32::<NativeEndian>()?,
            datasync: c.read_u32::<NativeEndian>()?,
        })
    }
}

/// Statfs request.
#[derive(Debug, Clone, Copy)]
pub struct StatfsReq {
    pub session_id: u32,
}

impl StatfsReq {
    pub fn parse(payload: &[u8]) -> io::Result<Self> {
        if payload.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "statfs req too short",
            ));
        }
        let mut c = Cursor::new(payload);
        Ok(Self {
            session_id: c.read_u32::<NativeEndian>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ctrl_hdr_roundtrip() {
        let hdr = CtrlHdr {
            magic: MFS_CTRL_MAGIC,
            version: MFS_CTRL_VERSION,
            req_id: 42,
            op: CtrlOp::Getattr as u16,
            flags: MFS_CTRL_FLAG_REQUEST,
            status: 0,
            payload_len: 16,
        };
        let bytes = hdr.to_bytes();
        let parsed = CtrlHdr::from_bytes(&bytes).unwrap();
        let magic = parsed.magic;
        let req_id = parsed.req_id;
        let op = parsed.op;
        let payload_len = parsed.payload_len;
        assert_eq!(magic, MFS_CTRL_MAGIC);
        assert_eq!(req_id, 42);
        assert_eq!(op, CtrlOp::Getattr as u16);
        assert_eq!(payload_len, 16);
    }

    #[test]
    fn test_ctrl_op_from_u16() {
        assert_eq!(CtrlOp::from_u16(1), Some(CtrlOp::Hello));
        assert_eq!(CtrlOp::from_u16(4), Some(CtrlOp::Getattr));
        assert_eq!(CtrlOp::from_u16(23), Some(CtrlOp::Statfs));
        assert_eq!(CtrlOp::from_u16(99), None);
    }
}
