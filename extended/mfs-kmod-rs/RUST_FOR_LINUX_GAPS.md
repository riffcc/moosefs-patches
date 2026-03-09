# Rust-for-Linux API Gaps for Full `mfs.ko` Port

Target kernel: Linux 6.18+ (`CONFIG_RUST=y`).

## Required API Coverage

1. VFS filesystem registration wrappers
- Need Rust equivalents for `file_system_type`, `init_fs_context`, `get_tree_nodev`, and `kill_sb` wiring.
- Needed to port `mfs_super.c` mount/unmount lifecycle without C glue.

2. Inode lifecycle and cache hooks
- Need safe wrappers for `alloc_inode`, `free_inode`, `iget_locked`, `unlock_new_inode`, `iget_failed`.
- Needed for `mfs_inode.c` parity and inode cache behavior.

3. Full inode operation table support
- Need Rust registration for callbacks equivalent to `struct inode_operations`:
  `lookup`, `create`, `unlink`, `link`, `rename`, `mkdir`, `rmdir`, `symlink`, `getattr`, `setattr`, `listxattr`.

4. File operation table support for filesystem files and dirs
- Need robust Rust bindings for `read_iter`, `write_iter`, `iterate_shared`, `fsync`, `mmap`, `llseek`, `open`, `release`.
- Current Rust support is stronger for misc/char devices than complex FS file ops.

5. Address-space and folio writeback callbacks
- Need wrappers equivalent to `read_folio`, `write_begin`, `write_end`, `writepage`, `dirty_folio`.
- This is required to port `mfs_file.c` page-cache integration.

6. Dentry/dir_context helpers
- Need Rust APIs for `dir_emit`, `dir_emit_dots`, and `d_splice_alias` patterns.
- Required by `mfs_dir.c` and `mfs_inode.c` lookup/readdir behavior.

7. Xattr handler registration
- Need Rust-side `xattr_handler` registration and callbacks with namespace prefixes.
- Required to replace `mfs_xattr.c`.

8. Symlink delayed-call helpers
- Need wrappers for `get_link` + delayed cleanup (`set_delayed_call` equivalent).
- Required for `mfs_symlink.c` parity.

## Optional But Useful

1. Easier packed-struct decode helpers in `kernel` crate
- Reduces unsafe/manual parsing for native-endian wire protocols.

2. Kernel synchronization primitives for mixed waitqueue/completion workflows
- Direct wrapper coverage for completion + waitqueue patterns from `mfs_helper_comm.c`.

## Temporary Workaround Strategy

- Keep pure wire/protocol and queue logic in Rust now.
- Keep kernel VFS callback glue in C temporarily, or stage Rust integration as APIs land.
- Preserve exact ctrl protocol ABI so current helper daemons can interoperate with either module.
