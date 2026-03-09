use alloc::vec::Vec;

use crate::logic::{EINVAL, ENAMETOOLONG};
use crate::protocol::{
    decode_wire_attr, MfsWireAttr, MFS_NAME_MAX, MFS_WIRE_ATTR_LEN,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InodeBuildError {
    NameTooLong,
    InvalidResponse,
}

impl InodeBuildError {
    pub fn errno(&self) -> i32 {
        match self {
            Self::NameTooLong => -ENAMETOOLONG,
            Self::InvalidResponse => -EINVAL,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupResponse {
    pub inode: u32,
    pub attr: MfsWireAttr,
}

pub fn build_getattr_request(session_id: u32, inode: u32, uid: u32, gid: u32) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&session_id.to_ne_bytes());
    out[4..8].copy_from_slice(&inode.to_ne_bytes());
    out[8..12].copy_from_slice(&uid.to_ne_bytes());
    out[12..16].copy_from_slice(&gid.to_ne_bytes());
    out
}

pub fn build_lookup_request(
    session_id: u32,
    parent_inode: u32,
    uid: u32,
    gid: u32,
    name: &[u8],
) -> Result<Vec<u8>, InodeBuildError> {
    if name.len() > MFS_NAME_MAX as usize {
        return Err(InodeBuildError::NameTooLong);
    }

    let mut out = Vec::with_capacity(20 + name.len());
    out.extend_from_slice(&session_id.to_ne_bytes());
    out.extend_from_slice(&parent_inode.to_ne_bytes());
    out.extend_from_slice(&uid.to_ne_bytes());
    out.extend_from_slice(&gid.to_ne_bytes());
    out.extend_from_slice(&(name.len() as u16).to_ne_bytes());
    out.extend_from_slice(&0u16.to_ne_bytes());
    out.extend_from_slice(name);
    Ok(out)
}

pub fn build_create_request(
    session_id: u32,
    parent_inode: u32,
    uid: u32,
    gid: u32,
    mode: u32,
    name: &[u8],
) -> Result<Vec<u8>, InodeBuildError> {
    if name.len() > MFS_NAME_MAX as usize {
        return Err(InodeBuildError::NameTooLong);
    }

    let mut out = Vec::with_capacity(24 + name.len());
    out.extend_from_slice(&session_id.to_ne_bytes());
    out.extend_from_slice(&parent_inode.to_ne_bytes());
    out.extend_from_slice(&uid.to_ne_bytes());
    out.extend_from_slice(&gid.to_ne_bytes());
    out.extend_from_slice(&mode.to_ne_bytes());
    out.extend_from_slice(&(name.len() as u16).to_ne_bytes());
    out.extend_from_slice(&0u16.to_ne_bytes());
    out.extend_from_slice(name);
    Ok(out)
}

pub fn parse_lookup_response(payload: &[u8]) -> Result<LookupResponse, InodeBuildError> {
    if payload.len() < 4 + MFS_WIRE_ATTR_LEN {
        return Err(InodeBuildError::InvalidResponse);
    }

    let inode = u32::from_ne_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let attr = decode_wire_attr(&payload[4..4 + MFS_WIRE_ATTR_LEN])
        .map_err(|_| InodeBuildError::InvalidResponse)?;

    Ok(LookupResponse { inode, attr })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{encode_wire_attr, MFS_WIRE_TYPE_FILE};

    #[test]
    fn build_lookup_request_shapes_payload() {
        let req = build_lookup_request(1, 2, 3, 4, b"foo").expect("lookup req");
        assert_eq!(req.len(), 23);
        assert_eq!(u32::from_ne_bytes(req[0..4].try_into().unwrap()), 1);
        assert_eq!(u16::from_ne_bytes(req[16..18].try_into().unwrap()), 3);
        assert_eq!(&req[20..], b"foo");
    }

    #[test]
    fn name_length_is_validated() {
        let long = [b'a'; 256];
        let err = build_create_request(1, 2, 3, 4, 0o644, &long).expect_err("name too long");
        assert_eq!(err, InodeBuildError::NameTooLong);
    }

    #[test]
    fn parse_lookup_response_extracts_inode_and_attr() {
        let attr = MfsWireAttr {
            type_: MFS_WIRE_TYPE_FILE,
            mattr: 0,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            size: 42,
            atime: 1,
            mtime: 2,
            ctime: 3,
            nlink: 1,
            rdev: 0,
            winattr: 0,
            reserved: [0; 3],
        };

        let mut payload = Vec::new();
        payload.extend_from_slice(&77u32.to_ne_bytes());
        payload.extend_from_slice(&encode_wire_attr(&attr));

        let parsed = parse_lookup_response(&payload).expect("valid lookup response");
        assert_eq!(parsed.inode, 77);
        assert_eq!(parsed.attr, attr);
    }
}
