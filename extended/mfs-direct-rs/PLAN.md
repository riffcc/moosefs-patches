# MooseFS Direct Plan

## Goal

Build `extended/mfs-direct-rs` into a native MooseFS transport library for
userspace consumers that want direct namespace, chunk, and positioned I/O
access without going through a mount.

## Architecture

### Layer 1: protocol transport

Provide:

- packet encode/decode
- big-endian field helpers
- request/response roundtrip helpers
- status/error mapping

### Layer 2: master client

Implemented:

- `register_session`
- `reconnect_session`
- `lookup_path`
- `getattr`
- `create_path`
- `mkdir -p`
- `open`
- `truncate`
- `unlink` / `rmdir` / `rename`
- `read_chunk` metadata lookup
- `write_chunk` metadata lookup
- `write_chunk_end`

### Layer 3: chunkserver client

Implemented:

- `read`
- `write_start`
- `write_data`
- `write_finish`
- status handling
- CRC32 for write fragments
- chunk metadata caching

### Layer 4: ergonomic client API

Expose:

- `connect(master_addr, options)`
- `lookup_path(path)`
- `stat_path(path)`
- `ensure_dir_all(path)`
- `ensure_file(path, size)`
- `open_file(path)`
- `ensure_file_len(path, size)`
- `read_all(path)`
- `write_all(path, bytes)`
- `read_at(file, offset, out)`
- `write_at(file, offset, bytes)`

## Current Milestone

The current success criterion is already met:

- connect to a live MooseFS master
- create or reuse a file
- stream bytes to a selected chunkserver
- commit the write back to the master
- reopen and read the written bytes back
- expose the path through a C ABI for downstream consumers

## Important protocol facts

- packet header is `type:32 length:32`, network byte order
- master file namespace ops use `CLTOMA_FUSE_*` / `MATOCL_FUSE_*`
- chunkserver data ops use `CLTOCS_*` / `CSTOCL_*`
- write flow is:
  - `CLTOMA_FUSE_WRITE_CHUNK`
  - decode chunk location reply
  - `CLTOCS_WRITE`
  - repeated `CLTOCS_WRITE_DATA`
  - `CLTOCS_WRITE_FINISH`
  - `CLTOMA_FUSE_WRITE_CHUNK_END`

## Concrete file layout

- `src/lib.rs`
- `src/error.rs`
- `src/protocol.rs`
- `src/client.rs`

Current files:

- `src/lib.rs`
- `src/error.rs`
- `src/protocol.rs`
- `src/client.rs`
- `src/ffi.rs`
- `src/quic.rs`

## Next High-Value Follow-Ups

1. block-oriented wrapper above positioned I/O
2. retry and replica failover policy
3. storage-class and label-aware policy selection
4. tighter integration path for downstream consumers like the flatfs
   multi-writer fork
5. QUIC stream-path validation against a live MooseFS endpoint
