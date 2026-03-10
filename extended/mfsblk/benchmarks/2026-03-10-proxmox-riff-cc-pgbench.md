# Proxmox `mfsblk` `pgbench` Benchmark

- Date: `2026-03-10T01:15:34+00:00`
- Host: `proxmox.riff.cc`
- Kernel: `6.17.13-1-pve`
- Driver: `extended/mfsblk`
- Backing file class: `nvme`
- Backing file path: `/mnt/proxmox-mfs/mfsblk-pgbench-1773105254.raw`
- Mapping mode: normal path lookup
- Block device: `/dev/mfsblk0`
- Filesystem: `ext4`
- Database: `PostgreSQL 17.9`
- Benchmark: `pgbench`

## Workload

- Scale: `5`
- Clients: `8`
- Threads: `4`
- Duration: `20s`
- Protocol: builtin TPC-B-like workload
- Transport: TCP on `127.0.0.1:5432`

## Result

- Return code: `0`
- Transactions processed: `19400`
- Failed transactions: `0`
- TPS: `962.147100`
- Average latency: `8.304 ms`
- Latency stddev: `7.044 ms`
- Initial connection time: `11.387 ms`

## Statement Latencies

- `BEGIN`: `0.039 ms`
- `UPDATE pgbench_accounts`: `0.125 ms`
- `SELECT abalance`: `0.089 ms`
- `UPDATE pgbench_tellers`: `0.791 ms`
- `UPDATE pgbench_branches`: `3.552 ms`
- `INSERT pgbench_history`: `0.077 ms`
- `END`: `3.629 ms`

## Storage Observations

- `mfsblk0` stayed write-heavy during the run.
- Sampled write throughput in the tail ranged from roughly `19 MB/s` to `37 MB/s`.
- `%util` was repeatedly near or at saturation during the busy windows.

## Notes

- This run completed after fixing:
  - normal stock MooseFS path lookup in `mfsblk`
  - refusal to remove a mounted device
  - chunkserver null-socket reopen in the block read/write path
- PostgreSQL benchmarking was forced onto TCP because the container profile denied Unix socket creation.
