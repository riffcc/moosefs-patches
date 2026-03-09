# IPFS-HA FlatFS Experiment

This is the safest current shape for an experimental MooseFS Direct backend for
the Kubo blockstore.

## Scope

- Touch only the flatfs `/blocks` datastore path.
- Leave Kubo repo metadata storage alone.
- Preserve flatfs key-to-path layout exactly.
- Keep the feature fully disabled unless an explicit env var enables it.

## Proposed Env Vars

- `MFS_DIRECT_FLATFS_ENABLE=1`
  - Master kill switch. If unset, flatfs keeps using local filesystem I/O.
- `MFS_DIRECT_MASTER=host:port`
  - MooseFS master endpoint for the Direct client.
- `MFS_DIRECT_SUBDIR=/path`
  - MooseFS export/subdir root used by the Direct client.
- `MFS_DIRECT_PASSWORD=...`
  - Optional export password.
- `MFS_DIRECT_FLATFS_ROOT=/absolute/local/blocks/path`
  - The local flatfs root path that should be intercepted. This prevents
    accidental activation outside the intended repo.

## Direct RS Surface Needed By FlatFS

The Rust ABI now exposes the minimum file-like operations a flatfs experiment
would need:

- connect
- `mkdir -p`
- write full file
- read full file
- exists
- size lookup
- unlink
- rmdir
- rename

## Next Patch

When wiring the Go side, the narrow seam should stay inside
`go-ds-flatfs-mw`:

- write path: `doPut`, `renameAndUpdateDiskUsage`, `doDelete`, batch `Put`,
  batch `Commit`
- read path: `readFile`, `Has`, `GetSize`, `walk`, `walkTopLevel`

That keeps default Kubo behavior unchanged and makes the experiment easy to
disable.
