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
- IPFS HA acting as a storage-class orchestrator for MooseFS-backed objects

## Current Direction

The current architectural bias is:

- keep whole-file and namespace operations available
- keep the C ABI friendly to downstream consumers
- move toward positioned and block-native storage APIs
- treat MooseFS as a protocol transport, not merely a mounted filesystem

That means the older file-emulation goals are still valid context, but the
center of gravity is shifting toward native storage semantics.

## Future Direction: IPFS HA As Storage-Class Orchestrator

Once the native object/block layer is real enough, IPFS HA becomes a natural
policy engine for moving data between MooseFS storage classes automatically.

The intended long-range shape is:

- keep MooseFS Direct as the underlying transport
- let object/block placement target explicit MooseFS classes or labels
- promote hot objects toward faster classes
- demote cold or archival objects toward cheaper classes
- let replication, popularity, pinning intent, or tenant policy drive class
  changes without changing visible CIDs

Examples of the policy layer we want later:

- fresh ingest lands on a fast write-oriented class first
- frequently requested CIDs get promoted toward NVMe-oriented classes
- archival or low-touch pins move toward cheaper durable classes
- rebalancing can migrate class placement without forcing application-visible
  renames or path changes

This is intentionally a later orchestration concern, not a blocker for the
current native Direct substrate work. But it is important enough to preserve
explicitly now, because it is one of the biggest strategic advantages of making
IPFS HA talk to MooseFS Direct as a real storage layer instead of a fake local
filesystem.
