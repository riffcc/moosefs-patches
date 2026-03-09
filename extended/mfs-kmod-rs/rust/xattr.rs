use alloc::string::String;
use alloc::vec::Vec;

use crate::logic::{EINVAL, ENAMETOOLONG, EOPNOTSUPP};
use crate::protocol::MfsCtrlXattrReq;

pub fn compose_xattr_name(prefix: &str, name: &str) -> Result<String, i32> {
    if prefix.is_empty() {
        return Err(-EOPNOTSUPP);
    }
    if name.is_empty() {
        return Err(-EINVAL);
    }

    let mut full = String::with_capacity(prefix.len() + name.len());
    full.push_str(prefix);
    full.push_str(name);
    if full.len() > 255 {
        return Err(-ENAMETOOLONG);
    }
    Ok(full)
}

pub fn build_setxattr_req(
    session_id: u32,
    inode: u32,
    uid: u32,
    gid: u32,
    flags: u32,
    name: &[u8],
    value: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(core::mem::size_of::<MfsCtrlXattrReq>() + name.len() + value.len());
    let hdr = MfsCtrlXattrReq {
        session_id,
        inode,
        uid,
        gid,
        flags,
        name_len: name.len() as u16,
        reserved: 0,
        value_len: value.len() as u32,
    };

    out.extend_from_slice(&hdr.session_id.to_ne_bytes());
    out.extend_from_slice(&hdr.inode.to_ne_bytes());
    out.extend_from_slice(&hdr.uid.to_ne_bytes());
    out.extend_from_slice(&hdr.gid.to_ne_bytes());
    out.extend_from_slice(&hdr.flags.to_ne_bytes());
    out.extend_from_slice(&hdr.name_len.to_ne_bytes());
    out.extend_from_slice(&hdr.reserved.to_ne_bytes());
    out.extend_from_slice(&hdr.value_len.to_ne_bytes());
    out.extend_from_slice(name);
    out.extend_from_slice(value);

    out
}
