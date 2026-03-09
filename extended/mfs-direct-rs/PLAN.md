# MooseFS Direct Plan

## Goal

Create `extended/mfs-direct-rs` as a Rust crate that can write directly to
MooseFS from userspace by speaking the protocol natively.

## Architecture

### Layer 1: protocol transport

Provide:

- packet encode/decode
- big-endian field helpers
- request/response roundtrip helpers
- status/error mapping

### Layer 2: master client

Implement:

- `register_session`
- `lookup_path`
- `create_path`
- `open`
- `truncate`
- `read_chunk` metadata lookup
- `write_chunk` metadata lookup
- `write_chunk_end`

### Layer 3: chunkserver client

Implement:

- `write_start`
- `write_data`
- `write_finish`
- status handling
- CRC32 for write fragments

### Layer 4: ergonomic client API

Expose:

- `connect(master_addr, options)`
- `lookup_path(path)`
- `ensure_file(path, size)`
- `write_all(path, bytes)`

## First implementation target

The first success criterion is:

- connect to a live MooseFS master
- create or reuse a file under an existing directory
- stream bytes to the selected chunkserver
- commit the write back to the master

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

Possible later additions:

- `src/master.rs`
- `src/chunkserver.rs`
- `src/object.rs`
- `src/ffi.rs`

## Immediate follow-ups after v1 works

1. recursive parent creation
2. direct read path
3. object/block oriented wrapper
4. storage-class and label-aware policy selection
5. C ABI for Go/cgo consumers like IPFS-HA
