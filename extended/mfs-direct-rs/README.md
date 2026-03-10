# MooseFS Direct

`moosefs-direct` is a native Rust MooseFS userspace client crate.

The point is simple:

- no FUSE
- no kernel driver
- no mount point
- direct MooseFS protocol access from Rust

This crate lets Rust and C consumers talk to MooseFS directly by speaking the
real MooseFS master and chunkserver protocols.

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

## Roadmap Context

The longer-range backlog and preserved future ideas live in [`ROADMAP.md`](./ROADMAP.md).
The README describes current shape; the roadmap keeps the deferred ideas visible.

## Current Capabilities

Today this crate already supports:

- packet framing and status/error mapping
- master session registration and reconnect
- absolute-path lookup and attribute fetch
- recursive directory creation
- file create/open/truncate flows
- direct chunkserver reads and writes
- `WRITE_CHUNK_END` commit handling
- whole-file helpers:
  - `read_all(path)`
  - `write_all(path, bytes)`
- positioned file I/O:
  - `open_file(path)`
  - `ensure_file_len(path, size)`
  - `read_at(file, offset, out)`
  - `write_at(file, offset, bytes)`
- file namespace helpers:
  - `mkdir -p`
  - exists/stat
  - unlink/rmdir
  - rename
- an initial object namespace layer:
  - deterministic shard-path mapping for object IDs
  - object metadata probes
  - positioned object reads and writes
  - stream writes into MooseFS-backed objects
- C ABI exports for userspace integration

## Current C ABI Shape

The native ABI is aimed at downstream consumers that want MooseFS as a storage
transport rather than a mounted filesystem.

It currently exposes:

- client connect / destroy
- whole-file read and write
- open-or-ensure file handles
- positioned read and write on open file handles
- directory creation
- exists and file-size probes
- unlink / rmdir / rename

That is enough to start wiring block- or object-oriented consumers without
forcing every caller to fake local filesystem semantics themselves.

## Native Object Layer

The first real step beyond file-shaped helpers now exists as an object layer in
`src/object.rs`.

It still uses MooseFS files underneath, but it stops making downstream callers
invent their own shard trees and object layout ad hoc. Instead, it gives them:

- one deterministic object root
- deterministic sharding like `/objects/ba/fy/<cid>.obj`
- object-level `put/get/has/delete`
- positioned object I/O on top of open MooseFS file handles
- a place to evolve toward a more block-native substrate later

This is intentionally the bridge step:

- today: object API over positioned MooseFS file I/O
- later: richer native block/object semantics without every downstream consumer
  needing to rediscover the same layout and handle lifecycle rules

## Still Missing

- retries and replica failover strategy
- storage class / label-aware placement policy
- full block-native object layer above positioned I/O
- live integration coverage against larger real clusters
- complete QUIC data-path parity

## QUIC Status

There is now an experimental feature-gated QUIC path:

- default build: TCP-only, no extra QUIC dependencies required
- `--features quic`: enables native QUIC/TLS client experiments and packet-mode
  helpers

This is still experimental and should not be treated as the stable default
transport yet.

## Benchmarking Notes

For throughput numbers, prefer release builds and make sure you understand
whether the bench is measuring MooseFS I/O or payload generation overhead.

- Use `cargo run --release --example bench -- ...` for any serious result.
- For the cleanest write-path number, use `MFS_FORCE_BUFFERED=1`.
- The default large-file path may stream generated payload data, which can
  measure benchmark-side CPU work as much as MooseFS throughput.
- `MFS_INFLIGHT=16` is currently the best tested default for the direct TCP
  write path on this repo's local NVMe-backed cluster.
