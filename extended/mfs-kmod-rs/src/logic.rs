use alloc::collections::{BTreeMap, VecDeque};
use alloc::vec::Vec;

use crate::protocol::{
    decode_ctrl_hdr_checked, encode_ctrl_hdr, CtrlHdr, MFS_CTRL_FLAG_REQUEST, MFS_CTRL_FLAG_RESPONSE,
    MFS_CTRL_MAGIC, MFS_CTRL_MAX_PAYLOAD, MFS_CTRL_VERSION,
};

pub const MFS_NAME_MAX: usize = 255;
pub const MFS_SYMLINK_MAX: usize = 4096;

pub const EPERM: i32 = 1;
pub const ENOENT: i32 = 2;
pub const EIO: i32 = 5;
pub const ENXIO: i32 = 6;
pub const E2BIG: i32 = 7;
pub const EAGAIN: i32 = 11;
pub const ENOMEM: i32 = 12;
pub const EACCES: i32 = 13;
pub const EFAULT: i32 = 14;
pub const EBUSY: i32 = 16;
pub const EEXIST: i32 = 17;
pub const EXDEV: i32 = 18;
pub const ENODEV: i32 = 19;
pub const ENOTDIR: i32 = 20;
pub const EISDIR: i32 = 21;
pub const EINVAL: i32 = 22;
pub const EFBIG: i32 = 27;
pub const ENOSPC: i32 = 28;
pub const EROFS: i32 = 30;
pub const EMLINK: i32 = 31;
pub const EPIPE: i32 = 32;
pub const ERANGE: i32 = 34;
pub const ENAMETOOLONG: i32 = 36;
pub const ENOTEMPTY: i32 = 39;
pub const EINTR: i32 = 4;
pub const ENODATA: i32 = 61;
pub const ETIMEDOUT: i32 = 110;
pub const EOPNOTSUPP: i32 = 95;
pub const EDQUOT: i32 = 122;
pub const ECANCELED: i32 = 125;
pub const ENOTCONN: i32 = 107;
pub const EMSGSIZE: i32 = 90;
pub const EBADF: i32 = 9;

pub const DT_UNKNOWN: u8 = 0;
pub const DT_FIFO: u8 = 1;
pub const DT_CHR: u8 = 2;
pub const DT_DIR: u8 = 4;
pub const DT_BLK: u8 = 6;
pub const DT_REG: u8 = 8;
pub const DT_LNK: u8 = 10;
pub const DT_SOCK: u8 = 12;

pub const S_IFIFO: u16 = 0o010000;
pub const S_IFCHR: u16 = 0o020000;
pub const S_IFDIR: u16 = 0o040000;
pub const S_IFBLK: u16 = 0o060000;
pub const S_IFREG: u16 = 0o100000;
pub const S_IFLNK: u16 = 0o120000;
pub const S_IFSOCK: u16 = 0o140000;

pub const MFS_STATUS_OK: i32 = 0;
pub const MFS_ERROR_EPERM: i32 = 1;
pub const MFS_ERROR_ENOTDIR: i32 = 2;
pub const MFS_ERROR_ENOENT: i32 = 3;
pub const MFS_ERROR_EACCES: i32 = 4;
pub const MFS_ERROR_EEXIST: i32 = 5;
pub const MFS_ERROR_EINVAL: i32 = 6;
pub const MFS_ERROR_ENOTEMPTY: i32 = 7;
pub const MFS_ERROR_CHUNKLOST: i32 = 8;
pub const MFS_ERROR_OUTOFMEMORY: i32 = 9;
pub const MFS_ERROR_INDEXTOOBIG: i32 = 10;
pub const MFS_ERROR_NOCHUNKSERVERS: i32 = 12;
pub const MFS_ERROR_IO: i32 = 22;
pub const MFS_ERROR_EROFS: i32 = 33;
pub const MFS_ERROR_QUOTA: i32 = 34;
pub const MFS_ERROR_ENOATTR: i32 = 38;
pub const MFS_ERROR_ENOTSUP: i32 = 39;
pub const MFS_ERROR_ERANGE: i32 = 40;
pub const MFS_ERROR_CSNOTPRESENT: i32 = 43;
pub const MFS_ERROR_EAGAIN: i32 = 45;
pub const MFS_ERROR_EINTR: i32 = 46;
pub const MFS_ERROR_ECANCELED: i32 = 47;
pub const MFS_ERROR_ENAMETOOLONG: i32 = 58;
pub const MFS_ERROR_EMLINK: i32 = 59;
pub const MFS_ERROR_ETIMEDOUT: i32 = 60;
pub const MFS_ERROR_EBADF: i32 = 61;
pub const MFS_ERROR_EFBIG: i32 = 62;
pub const MFS_ERROR_EISDIR: i32 = 63;

pub fn mfs_moosefs_error_to_errno(status: i32) -> i32 {
    if status <= 0 {
        return status;
    }

    match status {
        MFS_STATUS_OK => 0,
        MFS_ERROR_EPERM => -EPERM,
        MFS_ERROR_ENOTDIR => -ENOTDIR,
        MFS_ERROR_ENOENT => -ENOENT,
        MFS_ERROR_EACCES => -EACCES,
        MFS_ERROR_EEXIST => -EEXIST,
        MFS_ERROR_EINVAL => -EINVAL,
        MFS_ERROR_ENOTEMPTY => -ENOTEMPTY,
        MFS_ERROR_CHUNKLOST => -ENXIO,
        MFS_ERROR_OUTOFMEMORY => -ENOMEM,
        MFS_ERROR_INDEXTOOBIG => -EINVAL,
        MFS_ERROR_NOCHUNKSERVERS => -ENOSPC,
        MFS_ERROR_IO => -EIO,
        MFS_ERROR_EROFS => -EROFS,
        MFS_ERROR_QUOTA => -EDQUOT,
        MFS_ERROR_ENOATTR => -ENODATA,
        MFS_ERROR_ENOTSUP => -EOPNOTSUPP,
        MFS_ERROR_ERANGE => -ERANGE,
        MFS_ERROR_CSNOTPRESENT => -ENXIO,
        MFS_ERROR_EAGAIN => -EAGAIN,
        MFS_ERROR_EINTR => -EINTR,
        MFS_ERROR_ECANCELED => -ECANCELED,
        MFS_ERROR_ENAMETOOLONG => -ENAMETOOLONG,
        MFS_ERROR_EMLINK => -EMLINK,
        MFS_ERROR_ETIMEDOUT => -ETIMEDOUT,
        MFS_ERROR_EBADF => -EBADF,
        MFS_ERROR_EFBIG => -EFBIG,
        MFS_ERROR_EISDIR => -EISDIR,
        _ => -EINVAL,
    }
}

pub fn mfs_wire_type_to_dtype(wire_type: u8) -> u8 {
    match wire_type {
        1 => DT_REG,
        2 => DT_DIR,
        3 => DT_LNK,
        4 => DT_FIFO,
        5 => DT_BLK,
        6 => DT_CHR,
        7 => DT_SOCK,
        _ => DT_UNKNOWN,
    }
}

pub fn mfs_wire_type_to_mode(wire_type: u8) -> u16 {
    match wire_type {
        1 => S_IFREG,
        2 => S_IFDIR,
        3 => S_IFLNK,
        4 => S_IFIFO,
        5 => S_IFBLK,
        6 => S_IFCHR,
        7 => S_IFSOCK,
        _ => 0,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CtrlQueueError {
    InvalidArg,
    NotConnected,
    MsgSize,
    Proto,
    NoEntry,
}

impl CtrlQueueError {
    pub fn errno(&self) -> i32 {
        match self {
            Self::InvalidArg => -EINVAL,
            Self::NotConnected => -ENOTCONN,
            Self::MsgSize => -EMSGSIZE,
            Self::Proto => -71,
            Self::NoEntry => -ENOENT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedResponse {
    pub status: i32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
struct PendingReq {
    op: u16,
    payload: Vec<u8>,
    response: Option<CompletedResponse>,
    done: bool,
}

#[derive(Debug, Default)]
pub struct ControlQueue {
    tx_queue: VecDeque<u32>,
    inflight: BTreeMap<u32, PendingReq>,
    next_req_id: u32,
    helper_openers: u32,
}

impl ControlQueue {
    pub fn new() -> Self {
        Self {
            tx_queue: VecDeque::new(),
            inflight: BTreeMap::new(),
            next_req_id: 1,
            helper_openers: 0,
        }
    }

    pub fn helper_is_online(&self) -> bool {
        self.helper_openers > 0
    }

    pub fn helper_open(&mut self) {
        self.helper_openers = self.helper_openers.saturating_add(1);
    }

    pub fn helper_release(&mut self) {
        if self.helper_openers == 0 {
            return;
        }
        self.helper_openers -= 1;
        if self.helper_openers == 0 {
            self.fail_all(-EPIPE);
        }
    }

    pub fn enqueue_call(&mut self, op: u16, req_data: &[u8]) -> Result<u32, CtrlQueueError> {
        if req_data.len() > MFS_CTRL_MAX_PAYLOAD as usize {
            return Err(CtrlQueueError::MsgSize);
        }
        if !self.helper_is_online() {
            return Err(CtrlQueueError::NotConnected);
        }

        let req_id = self.next_req_id;
        self.next_req_id = self.next_req_id.wrapping_add(1);
        if self.next_req_id == 0 {
            self.next_req_id = 1;
        }

        self.tx_queue.push_back(req_id);
        self.inflight.insert(
            req_id,
            PendingReq {
                op,
                payload: req_data.to_vec(),
                response: None,
                done: false,
            },
        );

        Ok(req_id)
    }

    pub fn pop_next_request_frame(&mut self, max_count: usize) -> Result<Option<Vec<u8>>, CtrlQueueError> {
        let Some(req_id) = self.tx_queue.front().copied() else {
            return Ok(None);
        };

        let Some(req) = self.inflight.get(&req_id) else {
            self.tx_queue.pop_front();
            return Ok(None);
        };

        let total = crate::protocol::CTRL_HDR_LEN + req.payload.len();
        if max_count < total {
            return Err(CtrlQueueError::MsgSize);
        }

        self.tx_queue.pop_front();

        let hdr = CtrlHdr {
            magic: MFS_CTRL_MAGIC,
            version: MFS_CTRL_VERSION,
            req_id,
            op: req.op,
            flags: MFS_CTRL_FLAG_REQUEST,
            status: 0,
            payload_len: req.payload.len() as u32,
        };

        let mut frame = Vec::with_capacity(total);
        frame.extend_from_slice(&encode_ctrl_hdr(&hdr));
        frame.extend_from_slice(&req.payload);
        Ok(Some(frame))
    }

    pub fn submit_response_frame(&mut self, frame: &[u8]) -> Result<(), CtrlQueueError> {
        if frame.len() < crate::protocol::CTRL_HDR_LEN {
            return Err(CtrlQueueError::InvalidArg);
        }

        let hdr = decode_ctrl_hdr_checked(frame).map_err(|_| CtrlQueueError::Proto)?;
        let flags = hdr.flags;
        if flags & MFS_CTRL_FLAG_RESPONSE == 0 {
            return Err(CtrlQueueError::Proto);
        }

        let payload_len = hdr.payload_len as usize;
        let expected = crate::protocol::CTRL_HDR_LEN + payload_len;
        if expected != frame.len() {
            return Err(CtrlQueueError::InvalidArg);
        }

        let req_id = hdr.req_id;
        let req = self
            .inflight
            .get_mut(&req_id)
            .ok_or(CtrlQueueError::NoEntry)?;

        self.tx_queue.retain(|id| *id != req_id);

        let payload = frame[crate::protocol::CTRL_HDR_LEN..].to_vec();
        req.response = Some(CompletedResponse {
            status: hdr.status,
            payload,
        });
        req.done = true;
        Ok(())
    }

    pub fn timeout_request(&mut self, req_id: u32) {
        if let Some(req) = self.inflight.get_mut(&req_id) {
            if req.done {
                return;
            }

            self.tx_queue.retain(|id| *id != req_id);
            req.response = Some(CompletedResponse {
                status: -ETIMEDOUT,
                payload: Vec::new(),
            });
            req.done = true;
        }
    }

    pub fn take_response(&mut self, req_id: u32) -> Option<CompletedResponse> {
        let done = self.inflight.get(&req_id).map(|r| r.done).unwrap_or(false);
        if !done {
            return None;
        }
        self.inflight.remove(&req_id).and_then(|r| r.response)
    }

    fn fail_all(&mut self, status: i32) {
        self.tx_queue.clear();
        for req in self.inflight.values_mut() {
            req.response = Some(CompletedResponse {
                status,
                payload: Vec::new(),
            });
            req.done = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_mapping_matches_expected_errors() {
        assert_eq!(mfs_moosefs_error_to_errno(0), 0);
        assert_eq!(mfs_moosefs_error_to_errno(MFS_ERROR_ENOENT), -ENOENT);
        assert_eq!(mfs_moosefs_error_to_errno(MFS_ERROR_EEXIST), -EEXIST);
        assert_eq!(mfs_moosefs_error_to_errno(MFS_ERROR_IO), -EIO);
        assert_eq!(mfs_moosefs_error_to_errno(9999), -EINVAL);
    }

    #[test]
    fn wire_type_mappings_are_stable() {
        assert_eq!(mfs_wire_type_to_dtype(1), DT_REG);
        assert_eq!(mfs_wire_type_to_dtype(2), DT_DIR);
        assert_eq!(mfs_wire_type_to_dtype(7), DT_SOCK);
        assert_eq!(mfs_wire_type_to_dtype(255), DT_UNKNOWN);

        assert_eq!(mfs_wire_type_to_mode(1), S_IFREG);
        assert_eq!(mfs_wire_type_to_mode(2), S_IFDIR);
        assert_eq!(mfs_wire_type_to_mode(3), S_IFLNK);
        assert_eq!(mfs_wire_type_to_mode(255), 0);
    }

    #[test]
    fn control_queue_request_response_flow() {
        let mut queue = ControlQueue::new();
        queue.helper_open();

        let req_id = queue.enqueue_call(3, b"abc").expect("enqueue");
        let tx = queue
            .pop_next_request_frame(128)
            .expect("frame result")
            .expect("frame");

        let mut rsp = tx;
        rsp[14..16].copy_from_slice(&MFS_CTRL_FLAG_RESPONSE.to_ne_bytes());
        rsp[16..20].copy_from_slice(0i32.to_ne_bytes().as_slice());

        queue.submit_response_frame(&rsp).expect("submit response");
        let done = queue.take_response(req_id).expect("completed response");
        assert_eq!(done.status, 0);
        assert_eq!(done.payload, b"abc".to_vec());
    }

    #[test]
    fn helper_disconnect_fails_all_pending() {
        let mut queue = ControlQueue::new();
        queue.helper_open();
        let req_id = queue.enqueue_call(4, &[]).expect("enqueue");

        queue.helper_release();

        let done = queue.take_response(req_id).expect("disconnect completion");
        assert_eq!(done.status, -EPIPE);
        assert!(done.payload.is_empty());
    }

    #[test]
    fn pop_frame_checks_buffer_size() {
        let mut queue = ControlQueue::new();
        queue.helper_open();
        queue.enqueue_call(1, b"hello").expect("enqueue");

        let err = queue
            .pop_next_request_frame(crate::protocol::CTRL_HDR_LEN)
            .expect_err("must reject short read buffer");
        assert_eq!(err, CtrlQueueError::MsgSize);
    }
}
