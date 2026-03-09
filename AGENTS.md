# MooseFS Patches Agent Notes

## Read-path strategy

For the C kernel driver and helper, prefer thinking in stripe plans and scoreboards, not per-request metadata chatter.

The desired model is:

- Treat a sequential read stream as a stripe or band, not a pile of isolated chunk lookups.
- Resolve the metadata needed for the whole active stripe up front when practical.
- Represent progress as a scoreboard of covered and uncovered ranges.
- Let data workers consume from that stripe plan instead of rediscovering chunk metadata for each read.
- Prefer worker-local or stream-local planning over helper-global speculative chatter.
- Keep event-driven control flow, but let workers execute against precomputed stripe state.

## Citadel / SPORE lens

When choosing between designs, bias toward:

- deficit-driven planning over blind lookahead
- range coverage over FIFO thinking
- one proof or plan object per active stripe
- retiring work by coverage, not by request arrival order

## Kernel helper focus

When debugging sustained fanout regressions:

- first ask whether the failure is in metadata planning or data transfer
- look for repeated `GETATTR` / chunk-meta churn before changing the data plane
- avoid tiny one-entry caches when the real shape wants a stripe-local plan
- prefer safe instrumentation that identifies the exact failing stripe, chunk, replica, and packet type
