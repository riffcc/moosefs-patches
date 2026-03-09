use core::mem::size_of;

pub const MFS_CTRL_MAGIC: u32 = 0x4d46_5343;
pub const MFS_CTRL_VERSION: u32 = 1;

pub const MFS_CTRL_FLAG_REQUEST: u16 = 0x0001;
pub const MFS_CTRL_FLAG_RESPONSE: u16 = 0x0002;

pub const MFS_CTRL_MAX_PAYLOAD: u32 = 2 * 1024 * 1024;
pub const MFS_NAME_MAX: u32 = 255;

pub const MFS_CTRL_OP_HELLO: u16 = 1;
pub const MFS_CTRL_OP_REGISTER: u16 = 2;
pub const MFS_CTRL_OP_LOOKUP: u16 = 3;
pub const MFS_CTRL_OP_GETATTR: u16 = 4;
pub const MFS_CTRL_OP_SETATTR: u16 = 5;
pub const MFS_CTRL_OP_TRUNCATE: u16 = 6;
pub const MFS_CTRL_OP_READDIR: u16 = 7;
pub const MFS_CTRL_OP_CREATE: u16 = 8;
pub const MFS_CTRL_OP_UNLINK: u16 = 9;
pub const MFS_CTRL_OP_LINK: u16 = 10;
pub const MFS_CTRL_OP_RENAME: u16 = 11;
pub const MFS_CTRL_OP_MKDIR: u16 = 12;
pub const MFS_CTRL_OP_RMDIR: u16 = 13;
pub const MFS_CTRL_OP_READLINK: u16 = 14;
pub const MFS_CTRL_OP_SYMLINK: u16 = 15;
pub const MFS_CTRL_OP_GETXATTR: u16 = 16;
pub const MFS_CTRL_OP_SETXATTR: u16 = 17;
pub const MFS_CTRL_OP_LISTXATTR: u16 = 18;
pub const MFS_CTRL_OP_REMOVEXATTR: u16 = 19;
pub const MFS_CTRL_OP_READ: u16 = 20;
pub const MFS_CTRL_OP_WRITE: u16 = 21;
pub const MFS_CTRL_OP_FSYNC: u16 = 22;
pub const MFS_CTRL_OP_STATFS: u16 = 23;

pub const MFS_SETATTR_MODE: u32 = 1 << 0;
pub const MFS_SETATTR_UID: u32 = 1 << 1;
pub const MFS_SETATTR_GID: u32 = 1 << 2;
pub const MFS_SETATTR_ATIME: u32 = 1 << 3;
pub const MFS_SETATTR_MTIME: u32 = 1 << 4;
pub const MFS_SETATTR_CTIME: u32 = 1 << 5;
pub const MFS_SETATTR_ATIME_NOW: u32 = 1 << 6;
pub const MFS_SETATTR_MTIME_NOW: u32 = 1 << 7;

pub const MFS_WIRE_TYPE_UNKNOWN: u8 = 0;
pub const MFS_WIRE_TYPE_FILE: u8 = 1;
pub const MFS_WIRE_TYPE_DIR: u8 = 2;
pub const MFS_WIRE_TYPE_SYMLINK: u8 = 3;
pub const MFS_WIRE_TYPE_FIFO: u8 = 4;
pub const MFS_WIRE_TYPE_BLOCK: u8 = 5;
pub const MFS_WIRE_TYPE_CHAR: u8 = 6;
pub const MFS_WIRE_TYPE_SOCK: u8 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum CtrlOp {
    Hello = MFS_CTRL_OP_HELLO,
    Register = MFS_CTRL_OP_REGISTER,
    Lookup = MFS_CTRL_OP_LOOKUP,
    Getattr = MFS_CTRL_OP_GETATTR,
    Setattr = MFS_CTRL_OP_SETATTR,
    Truncate = MFS_CTRL_OP_TRUNCATE,
    Readdir = MFS_CTRL_OP_READDIR,
    Create = MFS_CTRL_OP_CREATE,
    Unlink = MFS_CTRL_OP_UNLINK,
    Link = MFS_CTRL_OP_LINK,
    Rename = MFS_CTRL_OP_RENAME,
    Mkdir = MFS_CTRL_OP_MKDIR,
    Rmdir = MFS_CTRL_OP_RMDIR,
    Readlink = MFS_CTRL_OP_READLINK,
    Symlink = MFS_CTRL_OP_SYMLINK,
    Getxattr = MFS_CTRL_OP_GETXATTR,
    Setxattr = MFS_CTRL_OP_SETXATTR,
    Listxattr = MFS_CTRL_OP_LISTXATTR,
    Removexattr = MFS_CTRL_OP_REMOVEXATTR,
    Read = MFS_CTRL_OP_READ,
    Write = MFS_CTRL_OP_WRITE,
    Fsync = MFS_CTRL_OP_FSYNC,
    Statfs = MFS_CTRL_OP_STATFS,
}

impl TryFrom<u16> for CtrlOp {
    type Error = ParseError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            MFS_CTRL_OP_HELLO => Ok(Self::Hello),
            MFS_CTRL_OP_REGISTER => Ok(Self::Register),
            MFS_CTRL_OP_LOOKUP => Ok(Self::Lookup),
            MFS_CTRL_OP_GETATTR => Ok(Self::Getattr),
            MFS_CTRL_OP_SETATTR => Ok(Self::Setattr),
            MFS_CTRL_OP_TRUNCATE => Ok(Self::Truncate),
            MFS_CTRL_OP_READDIR => Ok(Self::Readdir),
            MFS_CTRL_OP_CREATE => Ok(Self::Create),
            MFS_CTRL_OP_UNLINK => Ok(Self::Unlink),
            MFS_CTRL_OP_LINK => Ok(Self::Link),
            MFS_CTRL_OP_RENAME => Ok(Self::Rename),
            MFS_CTRL_OP_MKDIR => Ok(Self::Mkdir),
            MFS_CTRL_OP_RMDIR => Ok(Self::Rmdir),
            MFS_CTRL_OP_READLINK => Ok(Self::Readlink),
            MFS_CTRL_OP_SYMLINK => Ok(Self::Symlink),
            MFS_CTRL_OP_GETXATTR => Ok(Self::Getxattr),
            MFS_CTRL_OP_SETXATTR => Ok(Self::Setxattr),
            MFS_CTRL_OP_LISTXATTR => Ok(Self::Listxattr),
            MFS_CTRL_OP_REMOVEXATTR => Ok(Self::Removexattr),
            MFS_CTRL_OP_READ => Ok(Self::Read),
            MFS_CTRL_OP_WRITE => Ok(Self::Write),
            MFS_CTRL_OP_FSYNC => Ok(Self::Fsync),
            MFS_CTRL_OP_STATFS => Ok(Self::Statfs),
            _ => Err(ParseError::InvalidOp),
        }
    }
}

pub const CTRL_HDR_LEN: usize = 24;
pub const MFS_WIRE_ATTR_LEN: usize = 44;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsWireAttr {
    pub type_: u8,
    pub mattr: u8,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub atime: u32,
    pub mtime: u32,
    pub ctime: u32,
    pub nlink: u32,
    pub rdev: u32,
    pub winattr: u8,
    pub reserved: [u8; 3],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlRegisterReq {
    pub master_len: u16,
    pub subdir_len: u16,
    pub password_len: u16,
    pub reserved: u16,
    pub master_port: u16,
    pub flags: u16,
    pub mount_uid: u32,
    pub mount_gid: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlRegisterRsp {
    pub session_id: u32,
    pub root_inode: u32,
    pub root_attr: MfsWireAttr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlInodeReq {
    pub session_id: u32,
    pub inode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlLookupReq {
    pub session_id: u32,
    pub parent_inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub name_len: u16,
    pub reserved: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlLookupRsp {
    pub inode: u32,
    pub attr: MfsWireAttr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlSetattrReq {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlTruncateReq {
    pub session_id: u32,
    pub inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlReaddirReq {
    pub session_id: u32,
    pub inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub offset: u64,
    pub max_entries: u32,
    pub flags: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlReaddirRsp {
    pub next_offset: u64,
    pub count: u32,
    pub eof: u8,
    pub reserved: [u8; 3],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlDirentWire {
    pub next_offset: u64,
    pub inode: u32,
    pub type_: u8,
    pub reserved: u8,
    pub name_len: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlCreateReq {
    pub session_id: u32,
    pub parent_inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub name_len: u16,
    pub reserved: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlCreateRsp {
    pub inode: u32,
    pub attr: MfsWireAttr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlUnlinkReq {
    pub session_id: u32,
    pub parent_inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub name_len: u16,
    pub reserved: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlLinkReq {
    pub session_id: u32,
    pub inode: u32,
    pub new_parent_inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub name_len: u16,
    pub reserved: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlRenameReq {
    pub session_id: u32,
    pub old_parent_inode: u32,
    pub new_parent_inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub flags: u32,
    pub old_name_len: u16,
    pub new_name_len: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlMkdirReq {
    pub session_id: u32,
    pub parent_inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub name_len: u16,
    pub reserved: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlRmdirReq {
    pub session_id: u32,
    pub parent_inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub name_len: u16,
    pub reserved: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlSymlinkReq {
    pub session_id: u32,
    pub parent_inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub name_len: u16,
    pub target_len: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlReadlinkRsp {
    pub size: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlXattrReq {
    pub session_id: u32,
    pub inode: u32,
    pub uid: u32,
    pub gid: u32,
    pub flags: u32,
    pub name_len: u16,
    pub reserved: u16,
    pub value_len: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlXattrRsp {
    pub size: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlReadReq {
    pub session_id: u32,
    pub inode: u32,
    pub offset: u64,
    pub size: u32,
    pub flags: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlReadRsp {
    pub size: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlWriteReq {
    pub session_id: u32,
    pub inode: u32,
    pub offset: u64,
    pub size: u32,
    pub flags: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlWriteRsp {
    pub written: u32,
    pub attr_valid: u8,
    pub reserved: [u8; 3],
    pub attr: MfsWireAttr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlFsyncReq {
    pub session_id: u32,
    pub inode: u32,
    pub datasync: u32,
    pub reserved: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlStatfsReq {
    pub session_id: u32,
    pub reserved: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct MfsCtrlStatfsRsp {
    pub total_space: u64,
    pub avail_space: u64,
    pub free_space: u64,
    pub trash_space: u64,
    pub sustained_space: u64,
    pub inodes: u32,
    pub reserved: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    BufferTooShort,
    InvalidMagic,
    InvalidVersion,
    InvalidLength,
    PayloadTooLarge,
    InvalidOp,
}

pub fn encode_ctrl_hdr(hdr: &CtrlHdr) -> [u8; CTRL_HDR_LEN] {
    let mut out = [0u8; CTRL_HDR_LEN];
    put_u32(&mut out, 0, hdr.magic);
    put_u32(&mut out, 4, hdr.version);
    put_u32(&mut out, 8, hdr.req_id);
    put_u16(&mut out, 12, hdr.op);
    put_u16(&mut out, 14, hdr.flags);
    put_i32(&mut out, 16, hdr.status);
    put_u32(&mut out, 20, hdr.payload_len);
    out
}

pub fn decode_ctrl_hdr(buf: &[u8]) -> Result<CtrlHdr, ParseError> {
    if buf.len() < CTRL_HDR_LEN {
        return Err(ParseError::BufferTooShort);
    }

    let payload_len = read_u32(buf, 20)?;
    if payload_len > MFS_CTRL_MAX_PAYLOAD {
        return Err(ParseError::PayloadTooLarge);
    }

    Ok(CtrlHdr {
        magic: read_u32(buf, 0)?,
        version: read_u32(buf, 4)?,
        req_id: read_u32(buf, 8)?,
        op: read_u16(buf, 12)?,
        flags: read_u16(buf, 14)?,
        status: read_i32(buf, 16)?,
        payload_len,
    })
}

pub fn decode_ctrl_hdr_checked(buf: &[u8]) -> Result<CtrlHdr, ParseError> {
    let hdr = decode_ctrl_hdr(buf)?;
    if hdr.magic != MFS_CTRL_MAGIC {
        return Err(ParseError::InvalidMagic);
    }
    if hdr.version != MFS_CTRL_VERSION {
        return Err(ParseError::InvalidVersion);
    }
    Ok(hdr)
}

pub fn encode_wire_attr(attr: &MfsWireAttr) -> [u8; MFS_WIRE_ATTR_LEN] {
    let mut out = [0u8; MFS_WIRE_ATTR_LEN];
    out[0] = attr.type_;
    out[1] = attr.mattr;
    put_u16(&mut out, 2, attr.mode);
    put_u32(&mut out, 4, attr.uid);
    put_u32(&mut out, 8, attr.gid);
    put_u64(&mut out, 12, attr.size);
    put_u32(&mut out, 20, attr.atime);
    put_u32(&mut out, 24, attr.mtime);
    put_u32(&mut out, 28, attr.ctime);
    put_u32(&mut out, 32, attr.nlink);
    put_u32(&mut out, 36, attr.rdev);
    out[40] = attr.winattr;
    out[41..44].copy_from_slice(&attr.reserved);
    out
}

pub fn decode_wire_attr(buf: &[u8]) -> Result<MfsWireAttr, ParseError> {
    if buf.len() < MFS_WIRE_ATTR_LEN {
        return Err(ParseError::BufferTooShort);
    }

    Ok(MfsWireAttr {
        type_: buf[0],
        mattr: buf[1],
        mode: read_u16(buf, 2)?,
        uid: read_u32(buf, 4)?,
        gid: read_u32(buf, 8)?,
        size: read_u64(buf, 12)?,
        atime: read_u32(buf, 20)?,
        mtime: read_u32(buf, 24)?,
        ctime: read_u32(buf, 28)?,
        nlink: read_u32(buf, 32)?,
        rdev: read_u32(buf, 36)?,
        winattr: buf[40],
        reserved: [buf[41], buf[42], buf[43]],
    })
}

fn read_u16(buf: &[u8], off: usize) -> Result<u16, ParseError> {
    let end = off.checked_add(2).ok_or(ParseError::InvalidLength)?;
    let raw = buf.get(off..end).ok_or(ParseError::BufferTooShort)?;
    Ok(u16::from_ne_bytes([raw[0], raw[1]]))
}

fn read_u32(buf: &[u8], off: usize) -> Result<u32, ParseError> {
    let end = off.checked_add(4).ok_or(ParseError::InvalidLength)?;
    let raw = buf.get(off..end).ok_or(ParseError::BufferTooShort)?;
    Ok(u32::from_ne_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

fn read_i32(buf: &[u8], off: usize) -> Result<i32, ParseError> {
    Ok(read_u32(buf, off)? as i32)
}

fn read_u64(buf: &[u8], off: usize) -> Result<u64, ParseError> {
    let end = off.checked_add(8).ok_or(ParseError::InvalidLength)?;
    let raw = buf.get(off..end).ok_or(ParseError::BufferTooShort)?;
    Ok(u64::from_ne_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]))
}

fn put_u16(buf: &mut [u8], off: usize, value: u16) {
    let bytes = value.to_ne_bytes();
    buf[off..off + 2].copy_from_slice(&bytes);
}

fn put_u32(buf: &mut [u8], off: usize, value: u32) {
    let bytes = value.to_ne_bytes();
    buf[off..off + 4].copy_from_slice(&bytes);
}

fn put_i32(buf: &mut [u8], off: usize, value: i32) {
    put_u32(buf, off, value as u32);
}

fn put_u64(buf: &mut [u8], off: usize, value: u64) {
    let bytes = value.to_ne_bytes();
    buf[off..off + 8].copy_from_slice(&bytes);
}

const _: [(); CTRL_HDR_LEN] = [(); size_of::<CtrlHdr>()];
const _: [(); MFS_WIRE_ATTR_LEN] = [(); size_of::<MfsWireAttr>()];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ctrl_header_roundtrip_native_endian() {
        let hdr = CtrlHdr {
            magic: MFS_CTRL_MAGIC,
            version: MFS_CTRL_VERSION,
            req_id: 7,
            op: MFS_CTRL_OP_LOOKUP,
            flags: MFS_CTRL_FLAG_REQUEST,
            status: -5,
            payload_len: 123,
        };

        let encoded = encode_ctrl_hdr(&hdr);
        let decoded = decode_ctrl_hdr_checked(&encoded).expect("decode should succeed");
        assert_eq!(decoded, hdr);
    }

    #[test]
    fn ctrl_header_rejects_bad_magic() {
        let mut raw = [0u8; CTRL_HDR_LEN];
        put_u32(&mut raw, 0, 0x1234_5678);
        put_u32(&mut raw, 4, MFS_CTRL_VERSION);
        put_u32(&mut raw, 20, 0);

        let err = decode_ctrl_hdr_checked(&raw).expect_err("must reject bad magic");
        assert_eq!(err, ParseError::InvalidMagic);
    }

    #[test]
    fn wire_attr_roundtrip() {
        let attr = MfsWireAttr {
            type_: MFS_WIRE_TYPE_FILE,
            mattr: 0,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            size: 1_048_576,
            atime: 10,
            mtime: 11,
            ctime: 12,
            nlink: 2,
            rdev: 0,
            winattr: 0,
            reserved: [0; 3],
        };

        let encoded = encode_wire_attr(&attr);
        let decoded = decode_wire_attr(&encoded).expect("decode attr");
        assert_eq!(decoded, attr);
    }

    #[test]
    fn struct_sizes_match_c_abi() {
        assert_eq!(size_of::<CtrlHdr>(), 24);
        assert_eq!(size_of::<MfsWireAttr>(), 44);
        assert_eq!(size_of::<MfsCtrlRegisterReq>(), 20);
        assert_eq!(size_of::<MfsCtrlLookupReq>(), 20);
        assert_eq!(size_of::<MfsCtrlCreateRsp>(), 48);
        assert_eq!(size_of::<MfsCtrlWriteRsp>(), 52);
    }
}
