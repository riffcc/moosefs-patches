# Experimental Out-of-Order Write Ack Mode

Patch file:
- [`patches/moosefs-experimental-ooo-write-acks.patch`](/home/wings/projects/moosefs-patches/extended/mfs-direct-rs/patches/moosefs-experimental-ooo-write-acks.patch)

What it changes:
- adds a config gate: `ENABLE_OOO_WRITE_ACKS = 0`
- treats bit `7` of `CLTOCS_WRITE protocolid` as an experimental flag
- when both the config and protocol flag are enabled, middle chunkservers stop requiring `CSTOCL_WRITE_STATUS` replies to be emitted in strict `writeid` queue order
- completed fragment statuses may be sent back as soon as that fragment is locally and downstream-complete

Why this matters:
- the current middle-hop path in `mfschunkserver/mainserv.c` enforces head-of-line completion through `wrdata.head` and `wrdata.nethead`
- that makes a pipelined client behave more like a FIFO stream than a fragment scoreboard
- enabling out-of-order fragment acknowledgements unlocks a Rust client that keeps many `CLTOCS_WRITE_DATA` fragments in flight and retires them by `writeid`

What it does not do yet:
- it does not change MooseFS default behavior
- it does not make TCP packets arrive out of order
- it does not parallelize `hdd_write` by itself
- it does not change last-hop chunkserver semantics much; the real unlock is removing middle-hop ordered-ack assumptions

Client-side follow-up needed:
- send `protocolid = 0x81` instead of `0x01` for the patched mode
- keep a per-chunk completion scoreboard keyed by `writeid`
- allow many `WRITE_DATA` fragments in flight before waiting
- only issue `WRITE_FINISH` / `WRITE_CHUNK_END` after all fragment writeids are acknowledged

Important caution:
- this is an extended protocol patch, not wire-compatible behavior for stock clients that assume FIFO write-status ordering
- only enable it when both ends are patched and the caller explicitly opts in
