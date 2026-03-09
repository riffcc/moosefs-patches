# MooseFS Direct

`moosefs-direct` is a native Rust MooseFS userspace client crate.

The point is simple:

- no FUSE
- no kernel driver
- no mount point
- direct MooseFS protocol access from Rust

This crate should let Rust programs write to MooseFS by speaking the real
MooseFS master and chunkserver protocols directly.

## Why

Today the usual path is:

- application
- filesystem-shaped blockstore or file writer
- POSIX/VFS
- maybe FUSE
- MooseFS protocol

MooseFS Direct collapses that to:

- application
- `moosefs-direct`
- MooseFS master RPCs
- MooseFS chunkserver data path

That removes a large amount of fake filesystem work:

- directory creation churn
- shard-path management
- mount lifecycle complexity
- VFS/FUSE overhead
- blockstore code pretending objects are ordinary files

## Repo sources of truth

This crate should be built from the real protocol material already in this repo:

- `mfscommon/MFSCommunication.h`
- `extended/mfs-proto/`
- `extended/qemu-mfs/block/mfs-proto.c`
- `mfsmaster/matoclserv.c`
- `mfschunkserver/*`

The `extended/qemu-mfs` code is especially important because it already contains
a real userspace MooseFS client path for:

- session registration
- path lookup
- create/open/truncate
- chunk-location decode
- direct chunkserver reads/writes

## First vertical slice

The first useful version should implement:

1. packet framing
2. master session registration
3. absolute-path lookup
4. leaf-file create under an existing parent directory
5. open validation
6. chunk-location lookup for writes
7. direct chunkserver writes
8. `WRITE_CHUNK_END` back to the master

That is enough to support:

- `write_all(path, bytes)`

for programs that want to push content directly into MooseFS.

## Non-goals for v1

- recursive directory creation
- full read API polish
- storage class APIs
- label-aware placement control
- full replica-chain forwarding
- retries/load balancing
- object-store adapter layer
- FFI/C ABI

Those can all come after the first native userspace write path is real.
