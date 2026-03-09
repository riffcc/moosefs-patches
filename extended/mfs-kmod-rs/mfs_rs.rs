#![no_std]

extern crate alloc;

use kernel::prelude::*;

#[path = "src/protocol.rs"]
mod protocol;
#[path = "src/logic.rs"]
mod logic;
#[path = "src/inode_logic.rs"]
mod inode_logic;

#[path = "rust/internal.rs"]
mod internal;
#[path = "rust/superblock.rs"]
mod superblock;
#[path = "rust/ctrl.rs"]
mod ctrl;
#[path = "rust/inode.rs"]
mod inode;
#[path = "rust/file.rs"]
mod file;
#[path = "rust/dir.rs"]
mod dir;
#[path = "rust/symlink.rs"]
mod symlink;
#[path = "rust/xattr.rs"]
mod xattr;

module! {
    type: MfsRustModule,
    name: "mfs_rs",
    author: "Riff Labs",
    description: "MooseFS Rust-for-Linux module (staged port)",
    license: "GPL",
}

struct MfsRustModule {
    _ctrl: ctrl::CtrlDevice,
}

impl kernel::Module for MfsRustModule {
    fn init(_module: &'static ThisModule) -> Result<Self> {
        pr_info!("mfs_rs: staged Rust module loaded\n");

        // Stage 1: control-protocol compatibility primitives and queueing logic.
        // VFS registration is intentionally deferred until Rust VFS APIs needed by
        // mfs_super/mfs_inode/mfs_file are available and validated.
        Ok(Self {
            _ctrl: ctrl::CtrlDevice::new(),
        })
    }
}

impl Drop for MfsRustModule {
    fn drop(&mut self) {
        pr_info!("mfs_rs: staged Rust module unloaded\n");
    }
}
