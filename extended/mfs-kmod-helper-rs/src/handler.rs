use std::io::{self, Cursor};

use byteorder::{BigEndian, NativeEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;

use crate::ctrl::{
    CreateReq, CtrlHdr, CtrlOp, FsyncReq, InodeReq, LinkReq, LookupReq,
    ReadReq, ReaddirReq, RegisterReq, RenameReq, SetattrReq, StatfsReq,
    SymlinkReq, TruncateReq, UnlinkReq, WriteReq, XattrReq,
};
use crate::session::{MasterSession, SessionError};
use crate::wire::{
    attr_to_ctrl_bytes, parse_attr, CLTOMA_FUSE_CREATE, CLTOMA_FUSE_FSYNC,
    CLTOMA_FUSE_GETATTR, CLTOMA_FUSE_GETXATTR, CLTOMA_FUSE_LINK,
    CLTOMA_FUSE_LOOKUP, CLTOMA_FUSE_MKDIR, CLTOMA_FUSE_READDIR,
    CLTOMA_FUSE_READ_CHUNK, CLTOMA_FUSE_RENAME,
    CLTOMA_FUSE_RMDIR, CLTOMA_FUSE_SETATTR, CLTOMA_FUSE_SETXATTR,
    CLTOMA_FUSE_STATFS, CLTOMA_FUSE_SYMLINK, CLTOMA_FUSE_TRUNCATE,
    CLTOMA_FUSE_UNLINK, CLTOMA_FUSE_WRITE_CHUNK, GETDIR_FLAG_WITHATTR,
    MFS_ROOT_ID, MFSCHUNKSIZE, SET_ATIME_FLAG, SET_ATIME_NOW_FLAG,
    SET_GID_FLAG, SET_MODE_FLAG, SET_MTIME_FLAG, SET_MTIME_NOW_FLAG,
    SET_UID_FLAG,
};

const MFS_SETATTR_MODE: u32 = 1 << 0;
const MFS_SETATTR_UID: u32 = 1 << 1;
const MFS_SETATTR_GID: u32 = 1 << 2;
const MFS_SETATTR_ATIME: u32 = 1 << 3;
const MFS_SETATTR_MTIME: u32 = 1 << 4;
const MFS_SETATTR_ATIME_NOW: u32 = 1 << 6;
const MFS_SETATTR_MTIME_NOW: u32 = 1 << 7;

#[derive(Debug, Error)]
pub enum HandlerError {
    #[error("session error: {0}")]
    Session(#[from] SessionError),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid request: {0}")]
    InvalidRequest(&'static str),
    #[error("protocol error: {0}")]
    Protocol(&'static str),
    #[error("master status {0}")]
    Master(u8),
    #[error("unsupported op {0}")]
    UnsupportedOp(u16),
    #[error("msgid mismatch: expected {expected}, got {got}")]
    MsgIdMismatch { expected: u32, got: u32 },
}

impl HandlerError {
    pub fn status_code(&self) -> i32 {
        match self {
            Self::Master(status) => i32::from(*status),
            Self::InvalidRequest(_) => -nix::libc::EINVAL,
            Self::UnsupportedOp(_) => -nix::libc::EOPNOTSUPP,
            Self::MsgIdMismatch { .. } | Self::Protocol(_) => -nix::libc::EPROTO,
            Self::Session(SessionError::NotConnected) => -nix::libc::ENOTCONN,
            Self::Session(SessionError::MasterStatus(status)) => i32::from(*status),
            Self::Session(SessionError::ConnectionClosed) => -nix::libc::EPIPE,
            Self::Session(SessionError::TypeMismatch { .. }) => -nix::libc::EPROTO,
            Self::Session(SessionError::Protocol(_)) => -nix::libc::EPROTO,
            Self::Session(SessionError::Io(_)) | Self::Io(_) => -nix::libc::EIO,
        }
    }
}

pub trait SessionIo {
    fn connect_to(
        &mut self,
        host: &str,
        port: u16,
        subdir: &str,
    ) -> Result<u32, SessionError>;
    fn rpc(
        &mut self,
        req_type: u32,
        req_data: &[u8],
    ) -> Result<(u32, Vec<u8>), SessionError>;
    fn next_msgid(&mut self) -> u32;
}

impl SessionIo for MasterSession {
    fn connect_to(
        &mut self,
        host: &str,
        port: u16,
        subdir: &str,
    ) -> Result<u32, SessionError> {
        MasterSession::connect_to(self, host, port, subdir)
    }

    fn rpc(
        &mut self,
        req_type: u32,
        req_data: &[u8],
    ) -> Result<(u32, Vec<u8>), SessionError> {
        MasterSession::rpc(self, req_type, req_data)
    }

    fn next_msgid(&mut self) -> u32 {
        MasterSession::next_msgid(self)
    }
}

pub fn handle_request(
    ctrl_hdr: &CtrlHdr,
    payload: &[u8],
    session: &mut MasterSession,
) -> Result<Vec<u8>, HandlerError> {
    handle_request_with(ctrl_hdr, payload, session)
}

fn handle_request_with<S: SessionIo>(
    ctrl_hdr: &CtrlHdr,
    payload: &[u8],
    session: &mut S,
) -> Result<Vec<u8>, HandlerError> {
    let op = CtrlOp::from_u16(ctrl_hdr.op).ok_or(HandlerError::UnsupportedOp(ctrl_hdr.op))?;

    match op {
        CtrlOp::Hello => Ok(Vec::new()),
        CtrlOp::Register => handle_register(payload, session),
        CtrlOp::Getattr => handle_getattr(payload, session),
        CtrlOp::Lookup => handle_lookup(payload, session),
        CtrlOp::Readdir => handle_readdir(payload, session),
        CtrlOp::Create => handle_create(payload, session),
        CtrlOp::Mkdir => handle_mkdir(payload, session),
        CtrlOp::Unlink => handle_unlink(payload, session, CLTOMA_FUSE_UNLINK),
        CtrlOp::Rmdir => handle_unlink(payload, session, CLTOMA_FUSE_RMDIR),
        CtrlOp::Rename => handle_rename(payload, session),
        CtrlOp::Symlink => handle_symlink(payload, session),
        CtrlOp::Link => handle_link(payload, session),
        CtrlOp::Read => handle_read(payload, session),
        CtrlOp::Write => handle_write(payload, session),
        CtrlOp::Setattr => handle_setattr(payload, session),
        CtrlOp::Truncate => handle_truncate(payload, session),
        CtrlOp::Fsync => handle_fsync(payload, session),
        CtrlOp::Statfs => handle_statfs(payload, session),
        CtrlOp::Getxattr | CtrlOp::Listxattr | CtrlOp::Setxattr | CtrlOp::Removexattr => {
            handle_xattr(op, payload, session)
        }
        CtrlOp::Readlink => Err(HandlerError::UnsupportedOp(ctrl_hdr.op)),
    }
}

fn handle_register<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = RegisterReq::parse(payload)?;
    let session_id = session.connect_to(&req.master_host, req.master_port, &req.subdir)?;

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(17);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(MFS_ROOT_ID)?;
    wire_req.write_u8(0)?;
    wire_req.write_u32::<BigEndian>(req.mount_uid)?;
    wire_req.write_u32::<BigEndian>(req.mount_gid)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_GETATTR, &wire_req)?;
    let attr = parse_attr_response(msgid, &wire_rsp)?;

    let mut out = Vec::with_capacity(4 + 4 + 44);
    out.write_u32::<NativeEndian>(session_id)?;
    out.write_u32::<NativeEndian>(MFS_ROOT_ID)?;
    out.extend_from_slice(&attr_to_ctrl_bytes(&attr));
    Ok(out)
}

fn handle_getattr<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = InodeReq::parse(payload)?;
    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(17);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.inode)?;
    wire_req.write_u8(0)?;
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;
    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_GETATTR, &wire_req)?;
    let attr = parse_attr_response(msgid, &wire_rsp)?;
    Ok(attr_to_ctrl_bytes(&attr).to_vec())
}

fn handle_lookup<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = LookupReq::parse(payload)?;
    if req.name.is_empty() || req.name.len() > 255 {
        return Err(HandlerError::InvalidRequest("lookup name length"));
    }

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(4 + 4 + 1 + req.name.len() + 12);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.parent_inode)?;
    wire_req.write_u8(req.name.len() as u8)?;
    wire_req.extend_from_slice(&req.name);
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(1)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_LOOKUP, &wire_req)?;
    let body = parse_response_body(msgid, &wire_rsp)?;
    if body.len() < 4 + 35 {
        return Err(HandlerError::Protocol("lookup response too short"));
    }

    let mut c = Cursor::new(body);
    let inode = c.read_u32::<BigEndian>()?;
    let off = c.position() as usize;
    let (attr, _) = parse_attr(&body[off..])?;

    let mut out = Vec::with_capacity(4 + 44);
    out.write_u32::<NativeEndian>(inode)?;
    out.extend_from_slice(&attr_to_ctrl_bytes(&attr));
    Ok(out)
}

fn handle_readdir<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = ReaddirReq::parse(payload)?;
    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(33);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.inode)?;
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(1)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;
    wire_req.write_u8(GETDIR_FLAG_WITHATTR as u8)?;
    wire_req.write_u32::<BigEndian>(100_000)?;
    wire_req.write_u64::<BigEndian>(0)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_READDIR, &wire_req)?;
    let body = parse_response_body(msgid, &wire_rsp)?;
    Ok(body.to_vec())
}

fn parse_create_like(msgid: u32, wire_rsp: &[u8]) -> Result<Vec<u8>, HandlerError> {
    let body = parse_response_body(msgid, wire_rsp)?;

    let (inode, attr_data) = if body.len() >= 1 + 4 + 35 {
        let mut c = Cursor::new(&body[1..]);
        (c.read_u32::<BigEndian>()?, &body[5..])
    } else if body.len() >= 4 + 35 {
        let mut c = Cursor::new(body);
        (c.read_u32::<BigEndian>()?, &body[4..])
    } else {
        return Err(HandlerError::Protocol("create-like response too short"));
    };

    let (attr, _) = parse_attr(attr_data)?;
    let mut out = Vec::with_capacity(4 + 44);
    out.write_u32::<NativeEndian>(inode)?;
    out.extend_from_slice(&attr_to_ctrl_bytes(&attr));
    Ok(out)
}

fn handle_create<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = CreateReq::parse(payload)?;
    if req.name.is_empty() || req.name.len() > 255 {
        return Err(HandlerError::InvalidRequest("create name length"));
    }

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(4 + 4 + 1 + req.name.len() + 14);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.parent_inode)?;
    wire_req.write_u8(req.name.len() as u8)?;
    wire_req.extend_from_slice(&req.name);
    wire_req.write_u16::<BigEndian>((req.mode & 0o7777) as u16)?;
    wire_req.write_u16::<BigEndian>(0)?;
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(1)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_CREATE, &wire_req)?;
    parse_create_like(msgid, &wire_rsp)
}

fn handle_mkdir<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = CreateReq::parse(payload)?;
    if req.name.is_empty() || req.name.len() > 255 {
        return Err(HandlerError::InvalidRequest("mkdir name length"));
    }

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(4 + 4 + 1 + req.name.len() + 15);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.parent_inode)?;
    wire_req.write_u8(req.name.len() as u8)?;
    wire_req.extend_from_slice(&req.name);
    wire_req.write_u16::<BigEndian>((req.mode & 0o7777) as u16)?;
    wire_req.write_u16::<BigEndian>(0)?;
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(1)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;
    wire_req.write_u8(0)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_MKDIR, &wire_req)?;
    parse_create_like(msgid, &wire_rsp)
}

fn handle_unlink<S: SessionIo>(
    payload: &[u8],
    session: &mut S,
    req_type: u32,
) -> Result<Vec<u8>, HandlerError> {
    let req = UnlinkReq::parse(payload)?;
    if req.name.is_empty() || req.name.len() > 255 {
        return Err(HandlerError::InvalidRequest("unlink/rmdir name length"));
    }

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(4 + 4 + 1 + req.name.len() + 12);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.parent_inode)?;
    wire_req.write_u8(req.name.len() as u8)?;
    wire_req.extend_from_slice(&req.name);
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(1)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;

    let (_typ, wire_rsp) = session.rpc(req_type, &wire_req)?;
    parse_simple_status(msgid, &wire_rsp)?;
    Ok(Vec::new())
}

fn handle_rename<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = RenameReq::parse(payload)?;
    if req.old_name.is_empty() || req.old_name.len() > 255 {
        return Err(HandlerError::InvalidRequest("rename old_name length"));
    }
    if req.new_name.is_empty() || req.new_name.len() > 255 {
        return Err(HandlerError::InvalidRequest("rename new_name length"));
    }

    let mode = if req.flags & 2 != 0 {
        2u8
    } else if req.flags & 1 != 0 {
        1u8
    } else {
        0u8
    };

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(4 + 4 + 1 + req.old_name.len() + 4 + 1 + req.new_name.len() + 13);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.old_parent)?;
    wire_req.write_u8(req.old_name.len() as u8)?;
    wire_req.extend_from_slice(&req.old_name);
    wire_req.write_u32::<BigEndian>(req.new_parent)?;
    wire_req.write_u8(req.new_name.len() as u8)?;
    wire_req.extend_from_slice(&req.new_name);
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(1)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;
    wire_req.write_u8(mode)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_RENAME, &wire_req)?;
    parse_simple_status(msgid, &wire_rsp)?;
    Ok(Vec::new())
}

fn handle_symlink<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = SymlinkReq::parse(payload)?;
    if req.name.is_empty() || req.name.len() > 255 {
        return Err(HandlerError::InvalidRequest("symlink name length"));
    }

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(4 + 4 + 1 + req.name.len() + 4 + req.target.len() + 12);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.parent_inode)?;
    wire_req.write_u8(req.name.len() as u8)?;
    wire_req.extend_from_slice(&req.name);
    wire_req.write_u32::<BigEndian>(req.target.len() as u32)?;
    wire_req.extend_from_slice(&req.target);
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(1)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_SYMLINK, &wire_req)?;
    parse_create_like(msgid, &wire_rsp)
}

fn handle_link<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = LinkReq::parse(payload)?;
    if req.name.is_empty() || req.name.len() > 255 {
        return Err(HandlerError::InvalidRequest("link name length"));
    }

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(4 + 4 + 4 + 1 + req.name.len() + 12);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.inode)?;
    wire_req.write_u32::<BigEndian>(req.new_parent)?;
    wire_req.write_u8(req.name.len() as u8)?;
    wire_req.extend_from_slice(&req.name);
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(1)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_LINK, &wire_req)?;
    if wire_rsp.len() == 5 {
        parse_simple_status(msgid, &wire_rsp)?;
        return Ok(Vec::new());
    }
    let _body = parse_response_body(msgid, &wire_rsp)?;
    Ok(Vec::new())
}

fn handle_read<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = ReadReq::parse(payload)?;
    let chunk_idx = (req.offset / MFSCHUNKSIZE) as u32;

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(13);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.inode)?;
    wire_req.write_u32::<BigEndian>(chunk_idx)?;
    wire_req.write_u8(0)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_READ_CHUNK, &wire_req)?;
    let body = parse_response_body(msgid, &wire_rsp)?;

    let read_size = std::cmp::min(req.size as usize, body.len());

    let mut out = Vec::with_capacity(4 + read_size);
    out.write_u32::<NativeEndian>(read_size as u32)?;
    out.extend_from_slice(&body[..read_size]);
    Ok(out)
}

fn handle_write<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = WriteReq::parse(payload)?;
    if payload.len() < WriteReq::HEADER_SIZE + req.size as usize {
        return Err(HandlerError::InvalidRequest("write payload truncated"));
    }

    let chunk_idx = (req.offset / MFSCHUNKSIZE) as u32;
    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(13);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.inode)?;
    wire_req.write_u32::<BigEndian>(chunk_idx)?;
    wire_req.write_u8(0)?;
    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_WRITE_CHUNK, &wire_req)?;
    let _ = parse_response_body(msgid, &wire_rsp)?;

    let mut out = Vec::with_capacity(52);
    out.write_u32::<NativeEndian>(req.size)?;
    out.write_u8(0)?;
    out.extend_from_slice(&[0u8; 3]);
    out.extend_from_slice(&[0u8; 44]);
    Ok(out)
}

fn handle_setattr<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = SetattrReq::parse(payload)?;

    let mut setmask = 0u8;
    if req.valid & MFS_SETATTR_MODE != 0 {
        setmask |= SET_MODE_FLAG;
    }
    if req.valid & MFS_SETATTR_UID != 0 {
        setmask |= SET_UID_FLAG;
    }
    if req.valid & MFS_SETATTR_GID != 0 {
        setmask |= SET_GID_FLAG;
    }
    if req.valid & MFS_SETATTR_ATIME_NOW != 0 {
        setmask |= SET_ATIME_NOW_FLAG;
    } else if req.valid & MFS_SETATTR_ATIME != 0 {
        setmask |= SET_ATIME_FLAG;
    }
    if req.valid & MFS_SETATTR_MTIME_NOW != 0 {
        setmask |= SET_MTIME_NOW_FLAG;
    } else if req.valid & MFS_SETATTR_MTIME != 0 {
        setmask |= SET_MTIME_FLAG;
    }

    let atime = (req.atime_ns / 1_000_000_000) as u32;
    let mtime = (req.mtime_ns / 1_000_000_000) as u32;

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(42);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.inode)?;
    wire_req.write_u8(0)?;
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(1)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;
    wire_req.write_u8(setmask)?;
    wire_req.write_u16::<BigEndian>((req.mode & 0o7777) as u16)?;
    wire_req.write_u32::<BigEndian>(req.attr_uid)?;
    wire_req.write_u32::<BigEndian>(req.attr_gid)?;
    wire_req.write_u32::<BigEndian>(atime)?;
    wire_req.write_u32::<BigEndian>(mtime)?;
    wire_req.write_u8(0)?;
    wire_req.write_u8(0)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_SETATTR, &wire_req)?;
    let attr = parse_attr_response(msgid, &wire_rsp)?;
    Ok(attr_to_ctrl_bytes(&attr).to_vec())
}

fn handle_truncate<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = TruncateReq::parse(payload)?;

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(29);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.inode)?;
    wire_req.write_u8(0)?;
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(1)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;
    wire_req.write_u64::<BigEndian>(req.size)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_TRUNCATE, &wire_req)?;
    let body = parse_response_body(msgid, &wire_rsp)?;
    let attr_data = if body.len() >= 8 + 35 { &body[8..] } else { body };
    let (attr, _) = parse_attr(attr_data)?;

    Ok(attr_to_ctrl_bytes(&attr).to_vec())
}

fn handle_fsync<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let req = FsyncReq::parse(payload)?;
    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(8);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.inode)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_FSYNC, &wire_req)?;
    parse_simple_status(msgid, &wire_rsp)?;
    Ok(Vec::new())
}

fn handle_statfs<S: SessionIo>(payload: &[u8], session: &mut S) -> Result<Vec<u8>, HandlerError> {
    let _ = StatfsReq::parse(payload)?;

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(4);
    wire_req.write_u32::<BigEndian>(msgid)?;
    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_STATFS, &wire_req)?;
    let body = parse_response_body(msgid, &wire_rsp)?;

    if body.len() < 8 * 3 + 4 {
        return Err(HandlerError::Protocol("statfs response too short"));
    }

    let mut c = Cursor::new(body);
    let total_space = c.read_u64::<BigEndian>()?;
    let avail_space = c.read_u64::<BigEndian>()?;
    let trash_space = c.read_u64::<BigEndian>()?;
    let (sustained_space, inodes) = if body.len() >= 8 * 4 + 4 {
        (c.read_u64::<BigEndian>()?, c.read_u32::<BigEndian>()?)
    } else {
        (0u64, c.read_u32::<BigEndian>()?)
    };

    let mut out = Vec::with_capacity(48);
    out.write_u64::<NativeEndian>(total_space)?;
    out.write_u64::<NativeEndian>(avail_space)?;
    out.write_u64::<NativeEndian>(avail_space)?;
    out.write_u64::<NativeEndian>(trash_space)?;
    out.write_u64::<NativeEndian>(sustained_space)?;
    out.write_u32::<NativeEndian>(inodes)?;
    out.write_u32::<NativeEndian>(0)?;
    Ok(out)
}

fn handle_xattr<S: SessionIo>(
    op: CtrlOp,
    payload: &[u8],
    session: &mut S,
) -> Result<Vec<u8>, HandlerError> {
    match op {
        CtrlOp::Getxattr => {
            let req = XattrReq::parse(payload)?;
            getxattr_like(&req, &req.name, session)
        }
        CtrlOp::Listxattr => {
            let inode = InodeReq::parse(payload)?;
            let req = XattrReq {
                session_id: inode.session_id,
                inode: inode.inode,
                uid: inode.uid,
                gid: inode.gid,
                flags: 0,
                name: Vec::new(),
                value: Vec::new(),
            };
            getxattr_like(&req, &[], session)
        }
        CtrlOp::Setxattr => {
            let req = XattrReq::parse(payload)?;
            setxattr_like(&req, &req.name, &req.value, req.flags as u8, session)?;
            Ok(Vec::new())
        }
        CtrlOp::Removexattr => {
            let req = XattrReq::parse(payload)?;
            setxattr_like(&req, &req.name, &[], 3, session)?;
            Ok(Vec::new())
        }
        _ => Err(HandlerError::UnsupportedOp(op as u16)),
    }
}

fn getxattr_like<S: SessionIo>(
    req: &XattrReq,
    name: &[u8],
    session: &mut S,
) -> Result<Vec<u8>, HandlerError> {
    if name.len() > 255 {
        return Err(HandlerError::InvalidRequest("xattr name too long"));
    }

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(4 + 4 + 1 + name.len() + 2 + 12);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.inode)?;
    wire_req.write_u8(name.len() as u8)?;
    wire_req.extend_from_slice(name);
    wire_req.write_u8((req.flags & 1) as u8)?;
    wire_req.write_u8(0)?;
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(1)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_GETXATTR, &wire_req)?;
    let body = parse_response_body(msgid, &wire_rsp)?;
    if body.len() < 4 {
        return Err(HandlerError::Protocol("xattr response too short"));
    }

    let mut c = Cursor::new(body);
    let size = c.read_u32::<BigEndian>()? as usize;
    if body.len() < 4 + size {
        return Err(HandlerError::Protocol("xattr value truncated"));
    }

    let mut out = Vec::with_capacity(4 + size);
    out.write_u32::<NativeEndian>(size as u32)?;
    out.extend_from_slice(&body[4..4 + size]);
    Ok(out)
}

fn setxattr_like<S: SessionIo>(
    req: &XattrReq,
    name: &[u8],
    value: &[u8],
    flags: u8,
    session: &mut S,
) -> Result<(), HandlerError> {
    if name.len() > 255 {
        return Err(HandlerError::InvalidRequest("xattr name too long"));
    }

    let msgid = session.next_msgid();
    let mut wire_req = Vec::with_capacity(4 + 4 + 1 + name.len() + 4 + value.len() + 2 + 12);
    wire_req.write_u32::<BigEndian>(msgid)?;
    wire_req.write_u32::<BigEndian>(req.inode)?;
    wire_req.write_u8(name.len() as u8)?;
    wire_req.extend_from_slice(name);
    wire_req.write_u32::<BigEndian>(value.len() as u32)?;
    wire_req.extend_from_slice(value);
    wire_req.write_u8(flags & 3)?;
    wire_req.write_u8(0)?;
    wire_req.write_u32::<BigEndian>(req.uid)?;
    wire_req.write_u32::<BigEndian>(1)?;
    wire_req.write_u32::<BigEndian>(req.gid)?;

    let (_typ, wire_rsp) = session.rpc(CLTOMA_FUSE_SETXATTR, &wire_req)?;
    parse_simple_status(msgid, &wire_rsp)?;
    Ok(())
}

fn parse_simple_status(msgid: u32, wire_rsp: &[u8]) -> Result<(), HandlerError> {
    if wire_rsp.len() != 5 {
        return Err(HandlerError::Protocol("simple status response len"));
    }
    let got = u32::from_be_bytes([wire_rsp[0], wire_rsp[1], wire_rsp[2], wire_rsp[3]]);
    if got != msgid {
        return Err(HandlerError::MsgIdMismatch {
            expected: msgid,
            got,
        });
    }
    let status = wire_rsp[4];
    if status != 0 {
        return Err(HandlerError::Master(status));
    }
    Ok(())
}

fn parse_response_body(msgid: u32, wire_rsp: &[u8]) -> Result<&[u8], HandlerError> {
    if wire_rsp.len() == 5 {
        let got = u32::from_be_bytes([wire_rsp[0], wire_rsp[1], wire_rsp[2], wire_rsp[3]]);
        if got != msgid {
            return Err(HandlerError::MsgIdMismatch {
                expected: msgid,
                got,
            });
        }
        return Err(HandlerError::Master(wire_rsp[4]));
    }
    if wire_rsp.len() < 4 {
        return Err(HandlerError::Protocol("response too short"));
    }

    let got = u32::from_be_bytes([wire_rsp[0], wire_rsp[1], wire_rsp[2], wire_rsp[3]]);
    if got != msgid {
        return Err(HandlerError::MsgIdMismatch {
            expected: msgid,
            got,
        });
    }
    Ok(&wire_rsp[4..])
}

fn parse_attr_response(msgid: u32, wire_rsp: &[u8]) -> Result<crate::wire::MfsAttr, HandlerError> {
    let body = parse_response_body(msgid, wire_rsp)?;
    let (attr, _) = parse_attr(body)?;
    Ok(attr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctrl::{CtrlHdr, MFS_CTRL_MAGIC, MFS_CTRL_VERSION};

    #[derive(Default)]
    struct MockSession {
        next_msgid: u32,
        connect_expect: Option<(String, u16, String, u32)>,
        rpc_expect: Vec<(u32, Vec<u8>, Vec<u8>)>,
    }

    impl MockSession {
        fn with_msgid(seed: u32) -> Self {
            Self {
                next_msgid: seed,
                ..Self::default()
            }
        }
    }

    impl SessionIo for MockSession {
        fn connect_to(
            &mut self,
            host: &str,
            port: u16,
            subdir: &str,
        ) -> Result<u32, SessionError> {
            let (eh, ep, es, sid) = self.connect_expect.take().expect("unexpected connect_to");
            assert_eq!(host, eh);
            assert_eq!(port, ep);
            assert_eq!(subdir, es);
            Ok(sid)
        }

        fn rpc(
            &mut self,
            req_type: u32,
            req_data: &[u8],
        ) -> Result<(u32, Vec<u8>), SessionError> {
            let (exp_type, exp_data, rsp) = self.rpc_expect.remove(0);
            assert_eq!(req_type, exp_type);
            assert_eq!(req_data, exp_data.as_slice());
            Ok((req_type + 1, rsp))
        }

        fn next_msgid(&mut self) -> u32 {
            self.next_msgid += 1;
            self.next_msgid
        }
    }

    fn mk_hdr(op: CtrlOp) -> CtrlHdr {
        CtrlHdr {
            magic: MFS_CTRL_MAGIC,
            version: MFS_CTRL_VERSION,
            req_id: 1,
            op: op as u16,
            flags: 0,
            status: 0,
            payload_len: 0,
        }
    }

    fn mk_attr_record() -> Vec<u8> {
        let mut v = Vec::new();
        v.push(0); // flags new-style
        v.write_u16::<BigEndian>(0x21ED).unwrap(); // dir + 0755
        v.write_u32::<BigEndian>(1000).unwrap();
        v.write_u32::<BigEndian>(1001).unwrap();
        v.write_u32::<BigEndian>(10).unwrap();
        v.write_u32::<BigEndian>(11).unwrap();
        v.write_u32::<BigEndian>(12).unwrap();
        v.write_u32::<BigEndian>(2).unwrap();
        v.write_u64::<BigEndian>(4096).unwrap();
        v
    }

    #[test]
    fn hello_returns_empty() {
        let hdr = mk_hdr(CtrlOp::Hello);
        let mut session = MockSession::default();
        let out = handle_request_with(&hdr, &[], &mut session).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn getattr_dispatches_and_formats_attr() {
        let hdr = mk_hdr(CtrlOp::Getattr);

        let mut payload = Vec::new();
        payload.write_u32::<NativeEndian>(55).unwrap();
        payload.write_u32::<NativeEndian>(7).unwrap();
        payload.write_u32::<NativeEndian>(1000).unwrap();
        payload.write_u32::<NativeEndian>(1001).unwrap();

        let msgid = 11u32;
        let mut exp_req = Vec::new();
        exp_req.write_u32::<BigEndian>(msgid).unwrap();
        exp_req.write_u32::<BigEndian>(7).unwrap();
        exp_req.push(0);
        exp_req.write_u32::<BigEndian>(1000).unwrap();
        exp_req.write_u32::<BigEndian>(1001).unwrap();

        let mut rsp = Vec::new();
        rsp.write_u32::<BigEndian>(msgid).unwrap();
        rsp.extend_from_slice(&mk_attr_record());

        let mut session = MockSession::with_msgid(10);
        session.rpc_expect.push((CLTOMA_FUSE_GETATTR, exp_req, rsp));

        let out = handle_request_with(&hdr, &payload, &mut session).unwrap();
        assert_eq!(out.len(), 44);
        assert_eq!(out[0], 2); // wire dir type
    }

    #[test]
    fn register_connects_and_returns_register_rsp() {
        let hdr = mk_hdr(CtrlOp::Register);

        let host = b"127.0.0.1";
        let subdir = b"/mnt";
        let pass = b"pw";
        let mut payload = Vec::new();
        payload
            .write_u16::<NativeEndian>(host.len() as u16)
            .unwrap();
        payload
            .write_u16::<NativeEndian>(subdir.len() as u16)
            .unwrap();
        payload
            .write_u16::<NativeEndian>(pass.len() as u16)
            .unwrap();
        payload.write_u16::<NativeEndian>(0).unwrap();
        payload.write_u16::<NativeEndian>(9421).unwrap();
        payload.write_u16::<NativeEndian>(0).unwrap();
        payload.write_u32::<NativeEndian>(500).unwrap();
        payload.write_u32::<NativeEndian>(600).unwrap();
        payload.extend_from_slice(host);
        payload.extend_from_slice(subdir);
        payload.extend_from_slice(pass);

        let msgid = 2u32;
        let mut getattr_req = Vec::new();
        getattr_req.write_u32::<BigEndian>(msgid).unwrap();
        getattr_req.write_u32::<BigEndian>(1).unwrap();
        getattr_req.push(0);
        getattr_req.write_u32::<BigEndian>(500).unwrap();
        getattr_req.write_u32::<BigEndian>(600).unwrap();

        let mut rsp = Vec::new();
        rsp.write_u32::<BigEndian>(msgid).unwrap();
        rsp.extend_from_slice(&mk_attr_record());

        let mut session = MockSession::with_msgid(1);
        session.connect_expect = Some((
            "127.0.0.1".to_string(),
            9421,
            "/mnt".to_string(),
            4242,
        ));
        session
            .rpc_expect
            .push((CLTOMA_FUSE_GETATTR, getattr_req, rsp));

        let out = handle_request_with(&hdr, &payload, &mut session).unwrap();
        assert_eq!(out.len(), 52);
        assert_eq!(u32::from_ne_bytes([out[0], out[1], out[2], out[3]]), 4242);
        assert_eq!(u32::from_ne_bytes([out[4], out[5], out[6], out[7]]), 1);
    }

    #[test]
    fn unlink_maps_master_status_error() {
        let hdr = mk_hdr(CtrlOp::Unlink);

        let mut payload = Vec::new();
        payload.write_u32::<NativeEndian>(1).unwrap();
        payload.write_u32::<NativeEndian>(2).unwrap();
        payload.write_u32::<NativeEndian>(3).unwrap();
        payload.write_u32::<NativeEndian>(4).unwrap();
        payload.write_u16::<NativeEndian>(3).unwrap();
        payload.write_u16::<NativeEndian>(0).unwrap();
        payload.extend_from_slice(b"abc");

        let msgid = 8u32;
        let mut exp_req = Vec::new();
        exp_req.write_u32::<BigEndian>(msgid).unwrap();
        exp_req.write_u32::<BigEndian>(2).unwrap();
        exp_req.push(3);
        exp_req.extend_from_slice(b"abc");
        exp_req.write_u32::<BigEndian>(3).unwrap();
        exp_req.write_u32::<BigEndian>(1).unwrap();
        exp_req.write_u32::<BigEndian>(4).unwrap();

        let mut rsp = Vec::new();
        rsp.write_u32::<BigEndian>(msgid).unwrap();
        rsp.push(3);

        let mut session = MockSession::with_msgid(7);
        session.rpc_expect.push((CLTOMA_FUSE_UNLINK, exp_req, rsp));

        let err = handle_request_with(&hdr, &payload, &mut session).unwrap_err();
        assert!(matches!(err, HandlerError::Master(3)));
    }

    #[test]
    fn statfs_formats_ctrl_payload() {
        let hdr = mk_hdr(CtrlOp::Statfs);

        let mut payload = Vec::new();
        payload.write_u32::<NativeEndian>(1).unwrap();

        let msgid = 4u32;
        let mut req = Vec::new();
        req.write_u32::<BigEndian>(msgid).unwrap();

        let mut rsp = Vec::new();
        rsp.write_u32::<BigEndian>(msgid).unwrap();
        rsp.write_u64::<BigEndian>(100).unwrap();
        rsp.write_u64::<BigEndian>(40).unwrap();
        rsp.write_u64::<BigEndian>(3).unwrap();
        rsp.write_u32::<BigEndian>(99).unwrap();

        let mut session = MockSession::with_msgid(3);
        session.rpc_expect.push((CLTOMA_FUSE_STATFS, req, rsp));

        let out = handle_request_with(&hdr, &payload, &mut session).unwrap();
        assert_eq!(out.len(), 48);
        assert_eq!(u64::from_ne_bytes(out[0..8].try_into().unwrap()), 100);
        assert_eq!(u64::from_ne_bytes(out[8..16].try_into().unwrap()), 40);
        assert_eq!(u64::from_ne_bytes(out[16..24].try_into().unwrap()), 40);
        assert_eq!(u32::from_ne_bytes(out[40..44].try_into().unwrap()), 99);
    }

    #[test]
    fn read_returns_size_prefixed_payload() {
        let hdr = mk_hdr(CtrlOp::Read);
        let mut payload = Vec::new();
        payload.write_u32::<NativeEndian>(1).unwrap();
        payload.write_u32::<NativeEndian>(2).unwrap();
        payload.write_u64::<NativeEndian>(0).unwrap();
        payload.write_u32::<NativeEndian>(3).unwrap();
        payload.write_u32::<NativeEndian>(0).unwrap();

        let msgid = 5u32;
        let mut req = Vec::new();
        req.write_u32::<BigEndian>(msgid).unwrap();
        req.write_u32::<BigEndian>(2).unwrap();
        req.write_u32::<BigEndian>(0).unwrap();
        req.push(0);

        let mut rsp = Vec::new();
        rsp.write_u32::<BigEndian>(msgid).unwrap();
        rsp.extend_from_slice(b"abcdef");

        let mut session = MockSession::with_msgid(4);
        session.rpc_expect.push((CLTOMA_FUSE_READ_CHUNK, req, rsp));

        let out = handle_request_with(&hdr, &payload, &mut session).unwrap();
        assert_eq!(u32::from_ne_bytes(out[0..4].try_into().unwrap()), 3);
        assert_eq!(&out[4..], b"abc");
    }
}
