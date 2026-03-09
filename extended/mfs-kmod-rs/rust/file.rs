use crate::protocol::{MfsCtrlReadReq, MfsCtrlWriteReq};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadWindow {
    pub offset: u64,
    pub size: u32,
}

pub fn build_read_req(session_id: u32, inode: u32, window: ReadWindow) -> MfsCtrlReadReq {
    MfsCtrlReadReq {
        session_id,
        inode,
        offset: window.offset,
        size: window.size,
        flags: 0,
    }
}

pub fn build_write_req_header(session_id: u32, inode: u32, offset: u64, size: u32) -> MfsCtrlWriteReq {
    MfsCtrlWriteReq {
        session_id,
        inode,
        offset,
        size,
        flags: 0,
    }
}

// Stage-1 note:
// read_folio/write_begin/write_end/dirty_folio/writeback callbacks need VFS and
// page-cache Rust abstractions that are still evolving upstream.
