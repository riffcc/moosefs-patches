use alloc::vec::Vec;

use crate::logic::{ENAMETOOLONG, MFS_NAME_MAX, MFS_SYMLINK_MAX};
use crate::protocol::MfsCtrlSymlinkReq;

pub fn build_symlink_req(
    session_id: u32,
    parent_inode: u32,
    uid: u32,
    gid: u32,
    name: &[u8],
    target: &[u8],
) -> Result<Vec<u8>, i32> {
    if name.len() > MFS_NAME_MAX {
        return Err(-ENAMETOOLONG);
    }
    if target.len() > MFS_SYMLINK_MAX {
        return Err(-ENAMETOOLONG);
    }

    let mut out = Vec::with_capacity(core::mem::size_of::<MfsCtrlSymlinkReq>() + name.len() + target.len());
    let hdr = MfsCtrlSymlinkReq {
        session_id,
        parent_inode,
        uid,
        gid,
        name_len: name.len() as u16,
        target_len: target.len() as u16,
    };

    out.extend_from_slice(&hdr.session_id.to_ne_bytes());
    out.extend_from_slice(&hdr.parent_inode.to_ne_bytes());
    out.extend_from_slice(&hdr.uid.to_ne_bytes());
    out.extend_from_slice(&hdr.gid.to_ne_bytes());
    out.extend_from_slice(&hdr.name_len.to_ne_bytes());
    out.extend_from_slice(&hdr.target_len.to_ne_bytes());
    out.extend_from_slice(name);
    out.extend_from_slice(target);

    Ok(out)
}
