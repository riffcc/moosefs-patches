# MooseFS Direct Roadmap

This file preserves the longer-range context for `mfs-direct-rs` so we do not
lose useful future directions as the README and PLAN get updated to reflect
current reality.

## Original Vertical-Slice Milestones

These were the first major unlocks for the crate:

1. packet framing
2. master session registration
3. absolute-path lookup
4. leaf-file create under an existing parent directory
5. open validation
6. chunk-location lookup for writes
7. direct chunkserver writes
8. `WRITE_CHUNK_END` back to the master

Those are now largely implemented.

## Original Non-Goals For V1

These were intentionally deferred from the very first slice, but they still
matter:

- recursive directory creation
- full read API polish
- storage class APIs
- label-aware placement control
- full replica-chain forwarding
- retries/load balancing
- object-store adapter layer
- FFI/C ABI

Some of these are now partially implemented:

- recursive directory creation
- direct read path
- FFI/C ABI

The rest remain important backlog, not discarded ideas.

## Still-Important Future Work

### Reliability

- retry policy for transient master and chunkserver failures
- replica failover and alternate replica selection
- reconnect behavior for longer-lived clients
- stronger live-cluster integration tests

### Placement And Policy

- storage class APIs
- label-aware placement control
- richer policy hints for downstream consumers

### Data Path

- full replica-chain forwarding semantics where desirable
- more block-native APIs above `pread` / `pwrite`
- object-store adapter layer for content-addressed or block-oriented systems

### Transport

- QUIC stream-path validation against live MooseFS servers
- decide what becomes stable default vs explicit experimental transport

### Downstream Integration

- env-gated integration with the flatfs multi-writer fork
- Kubo/IPFS-HA dogfooding
- other consumers that want MooseFS as a direct network storage substrate

## Current Direction

The current architectural bias is:

- keep whole-file and namespace operations available
- keep the C ABI friendly to downstream consumers
- move toward positioned and block-native storage APIs
- treat MooseFS as a protocol transport, not merely a mounted filesystem

That means the older file-emulation goals are still valid context, but the
center of gravity is shifting toward native storage semantics.
