# MooseFS Direct Architecture

## Design rule

Treat MooseFS as a storage protocol, not as a mounted filesystem.

Applications should talk to a native Rust client that:

- handles MooseFS packet framing
- manages master sessions
- resolves namespace and chunk placement
- streams bytes to chunkservers directly

## Conceptual stack

Old stack:

- app
- filesystem abstraction
- kernel / FUSE
- MooseFS

New stack:

- app
- `moosefs-direct`
- MooseFS

## Initial object model

### Session

- live TCP connection to master
- negotiated master version
- monotonic message IDs

### ChunkLocation

- protocol version
- file length
- chunk id
- chunk version
- chunk index
- replica list

### ChunkReplica

- host
- port
- chunkserver version
- optional label mask

## API direction

Low-level API:

- `register_session`
- `master_lookup_path`
- `master_create_path`
- `master_write_chunk`
- `chunkserver_write`
- `master_write_chunk_end`

High-level API:

- `write_all(path, data)`

Later:

- `put_object(key, data, policy)`
- `get_object(key)`
- `put_block(cid, data)`

## Why the qemu-mfs code matters

`extended/qemu-mfs/block/mfs-proto.c` already proves the core userspace pattern:

- real register flow
- real lookup/create/open/truncate
- real chunk-location decode
- real direct chunkserver write path

MooseFS Direct should lift that into a reusable Rust library rather than
re-discovering the protocol from scratch.
