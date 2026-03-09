# mfs-kmod-rs

Rust-for-Linux port workspace for `mfs.ko` (Linux 6.18+ with `CONFIG_RUST=y`).

## Goals

- Keep C module (`mfs-kmod/`) unchanged for Proxmox compatibility.
- Build a Rust module in parallel (`mfs-kmod-rs/`).
- Preserve `/dev/mfs_ctrl` wire protocol compatibility with existing helpers.

## Layout

- `mfs_rs.rs`: Rust kernel module entrypoint.
- `src/protocol.rs`: Ctrl protocol ABI types and native-endian encoders/decoders.
- `src/logic.rs`: Pure queue/error mapping logic for ctrl transport.
- `src/inode_logic.rs`: Pure request/response builders for inode operations.
- `rust/*.rs`: staged kernel-facing module files mirroring C split:
  - `superblock.rs`, `inode.rs`, `file.rs`, `dir.rs`, `ctrl.rs`, `symlink.rs`, `xattr.rs`.
- `RUST_FOR_LINUX_GAPS.md`: upstream API gaps required for full feature parity.

## Build

Kernel module (against Rust-enabled kernel tree):

```bash
make -C mfs-kmod-rs
```

Pure logic tests:

```bash
cargo test --manifest-path mfs-kmod-rs/Cargo.toml
```
