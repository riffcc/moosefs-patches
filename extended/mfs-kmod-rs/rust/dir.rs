use crate::protocol::MfsCtrlReaddirReq;

pub fn build_readdir_req(
    session_id: u32,
    inode: u32,
    uid: u32,
    gid: u32,
    offset: u64,
    max_entries: u32,
) -> MfsCtrlReaddirReq {
    MfsCtrlReaddirReq {
        session_id,
        inode,
        uid,
        gid,
        offset,
        max_entries,
        flags: 0,
    }
}

// Stage-1 note:
// iterate_shared glue to dir_context/dir_emit is not yet ported because stable
// Rust wrappers for filesystem directory iteration are incomplete.
