use alloc::vec::Vec;

use crate::inode_logic::{
    build_create_request, build_getattr_request, build_lookup_request, parse_lookup_response,
    InodeBuildError, LookupResponse,
};

#[derive(Debug, Clone, Copy)]
pub struct InodeIdentity {
    pub ino: u32,
    pub uid: u32,
    pub gid: u32,
}

pub fn make_getattr_payload(session_id: u32, inode: InodeIdentity) -> [u8; 16] {
    build_getattr_request(session_id, inode.ino, inode.uid, inode.gid)
}

pub fn make_lookup_payload(
    session_id: u32,
    parent_inode: u32,
    uid: u32,
    gid: u32,
    name: &[u8],
) -> Result<Vec<u8>, InodeBuildError> {
    build_lookup_request(session_id, parent_inode, uid, gid, name)
}

pub fn make_create_payload(
    session_id: u32,
    parent_inode: u32,
    uid: u32,
    gid: u32,
    mode: u32,
    name: &[u8],
) -> Result<Vec<u8>, InodeBuildError> {
    build_create_request(session_id, parent_inode, uid, gid, mode, name)
}

pub fn parse_lookup_or_create_response(payload: &[u8]) -> Result<LookupResponse, InodeBuildError> {
    parse_lookup_response(payload)
}

// Stage-1 note:
// Full inode operation callbacks are intentionally deferred until Rust-for-Linux
// exposes stable VFS inode/dentry registration APIs equivalent to struct
// inode_operations and iget_locked lifecycle hooks.
