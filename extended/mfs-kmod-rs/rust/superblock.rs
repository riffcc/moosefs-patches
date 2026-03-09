use alloc::vec::Vec;

use crate::internal::{MountConfig, SessionInfo};
use crate::protocol::MfsCtrlRegisterReq;

pub fn build_register_req(cfg: &MountConfig) -> Vec<u8> {
    let master = cfg.master_host.as_bytes();
    let subdir = cfg.subdir.as_bytes();
    let password = cfg.password.as_bytes();

    let mut out = Vec::with_capacity(core::mem::size_of::<MfsCtrlRegisterReq>() + master.len() + subdir.len() + password.len());
    let hdr = MfsCtrlRegisterReq {
        master_len: master.len() as u16,
        subdir_len: subdir.len() as u16,
        password_len: password.len() as u16,
        reserved: 0,
        master_port: cfg.master_port,
        flags: 0,
        mount_uid: cfg.mount_uid,
        mount_gid: cfg.mount_gid,
    };

    out.extend_from_slice(&hdr.master_len.to_ne_bytes());
    out.extend_from_slice(&hdr.subdir_len.to_ne_bytes());
    out.extend_from_slice(&hdr.password_len.to_ne_bytes());
    out.extend_from_slice(&hdr.reserved.to_ne_bytes());
    out.extend_from_slice(&hdr.master_port.to_ne_bytes());
    out.extend_from_slice(&hdr.flags.to_ne_bytes());
    out.extend_from_slice(&hdr.mount_uid.to_ne_bytes());
    out.extend_from_slice(&hdr.mount_gid.to_ne_bytes());
    out.extend_from_slice(master);
    out.extend_from_slice(subdir);
    out.extend_from_slice(password);

    out
}

pub fn parse_register_rsp(payload: &[u8]) -> Option<SessionInfo> {
    if payload.len() < 8 {
        return None;
    }

    let session_id = u32::from_ne_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let mut root_inode = u32::from_ne_bytes([payload[4], payload[5], payload[6], payload[7]]);
    if root_inode == 0 {
        root_inode = 1;
    }

    Some(SessionInfo {
        session_id,
        root_inode,
    })
}

// Stage-1 note:
// register_filesystem/get_tree_nodev/init_fs_context plumbing is pending
// upstream Rust VFS mount API coverage.
