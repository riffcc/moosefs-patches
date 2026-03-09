# qemu-mfs

`qemu-mfs` is a MooseFS-backed QEMU block driver fragment intended to be
vendored into a QEMU tree.

## What It Does

- Registers a MooseFS session directly from QEMU.
- Resolves MooseFS paths to inodes using real `LOOKUP` / `GETATTR` RPCs.
- Creates virtual disk files with `CREATE` + `TRUNCATE`.
- Reads chunk metadata from the master and streams data directly from a
  chunkserver.
- Writes chunk data directly to a chunkserver and commits the new file length
  back to the master with `WRITE_CHUNK_END`.

## QEMU Integration

Add the subtree to a QEMU source tree and include it from `block/meson.build`:

```meson
subdir('../extended/qemu-mfs')
```

Then build QEMU as usual. The driver registers as both the format and protocol
name `mfs`.

Example create:

```bash
qemu-img create -f mfs \
  -o master=mfsmaster.example:9421,path=/vm-images/demo.qcow2,size=20G \
  dummy
```

Example run:

```bash
qemu-system-x86_64 \
  -drive file.driver=mfs,file.master=mfsmaster.example:9421,file.path=/vm-images/demo.raw,if=virtio,format=mfs
```

## Rust-First Prototype

This tree now also carries a Rust-side prototype shim in
`rust/qemu-mfs-rust/`. It depends on `../mfs-direct-rs` and exports a thin C
ABI that is intentionally closer to what QEMU needs than to the full MooseFS
protocol surface:

- connect with either plaintext `password` or QEMU-style `password-md5`
- open or create-sized files
- positioned reads and writes
- cached file size lookup

Build it locally with:

```bash
cargo build --release --manifest-path rust/qemu-mfs-rust/Cargo.toml
```

The current C block driver is still present, but the intended direction is to
turn `block/mfs.c` into a narrow QEMU glue layer and move MooseFS semantics into
Rust. The Rust static library is the bridge for that migration.

## Proxmox Direction

For Proxmox VE, the intended integration point is a storage plugin that maps a
 volume ID to:

- `file.driver=mfs`
- `file.master=<cluster-master>:9421`
- `file.path=/vm-images/<volume>`

That keeps the guest-visible disk on VirtIO while QEMU itself speaks MooseFS.
No Proxmox plugin is wired up in this directory yet; this tree currently
provides the QEMU-side driver and build fragment.

## Current Scope

- The backend now uses MooseFS master packet IDs and real metadata RPCs.
- Chunkserver writes currently target the selected replica directly without
  building a full MooseFS forwarding chain.
- Full validation still needs an actual QEMU tree build and an integration test
  against a live MooseFS cluster.
- The Rust prototype is not wired into the C QEMU block driver yet; it is the
  first concrete step toward a thin-shim architecture.
