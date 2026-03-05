# Extended MooseFS — Native Setup Handover

**Date**: 2026-03-05
**Target host**: Blackberry (10.7.1.200)
**Goal**: Set up a native MooseFS cluster with kernel-native client, build everything from source, run the Rust benchmark suite.

## What's in the repo

The repo is a fork of `moosefs/moosefs` at version 4.58.3 with two additions:

### 1. Label patches (applied directly to source, 5 commits ahead of upstream)

These 5 commits add per-disk label support to MooseFS. They modify the C source directly:

```
6f6ea66 mfschunkserver: parse optional disk labels in mfshdd entries
4eca605 chunkserver: report per-disk labels to master
a312036 mfsmaster: use per-disk labels for placement matching
2e8fd55 chunkserver: add LABEL_PRECEDENCE configuration
23441fa config: document per-disk labels and precedence examples
```

The patches are also available as standalone patch files in `extended/mfs-labels/` if you need to re-apply them to a clean checkout.

Modified files: `mfschunkserver/hddspacemgr.c`, `mfschunkserver/hddspacemgr.h`, `mfschunkserver/masterconn.c`, `mfscommon/MFSCommunication.h`, `mfsmaster/chunks.c`, `mfsmaster/csdb.c`, `mfsmaster/matocsserv.c`, `mfsmaster/matocsserv.h`, `mfsdata/mfschunkserver.cfg.in`, `mfsdata/mfshdd.cfg`, `mfsmanpages/mfschunkserver.cfg.5`

### 2. `extended/` directory (NOT YET COMMITTED — see "Git situation" below)

#### Kernel modules (C)

| Module | Location | Description |
|--------|----------|-------------|
| mfs-proto | `extended/mfs-proto/` | Wire protocol encode/decode for kernel<->userspace |
| mfs-kmod | `extended/mfs-kmod/` | VFS kernel client — this is the `mount -t mfs` driver |
| mfsblk | `extended/mfsblk/` | Block device driver (expose MooseFS files as /dev/mfsblk*) |
| qemu-mfs | `extended/qemu-mfs/` | QEMU block driver plugin |

Build: each has a `Makefile` that uses standard kernel module build (`make -C /lib/modules/$(uname -r)/build M=$(pwd) modules`). Requires matching kernel headers.

mfs-kmod also builds `mfskmod-helper` (userspace helper binary) and installs `mount.mfs` (mount helper script).

**Build order matters**: mfs-proto first (provides `mfs_proto.ko`), then mfs-kmod (depends on proto symbols).

#### Rust benchmark suite

Location: `extended/test-cluster/benches/`

A cargo binary project called `mfs-bench`. 12 benchmark categories:

| Category | What it does |
|----------|--------------|
| sequential | fio sequential read/write (1M blocksize) |
| random | fio 4K random read/write |
| mixed | fio 70/30 read/write mix |
| metadata | file create/stat/unlink latency |
| small_files | create/read/delete N small files |
| large_file | single large file write/read throughput |
| fsync | fsync latency (sync ioengine) |
| dir_scale | readdir at 100/1K/10K entries |
| concurrent | multi-job fio scaling |
| postgres | pgbench on local vs MooseFS datadir |
| sqlite | insert/select/transaction on both targets |
| etcd | put/get/txn against etcd with data on both targets |

**Dependencies**: `fio`, `rustup` (needs Rust 1.87+, ideally latest stable), `protobuf-compiler` (for etcd-client crate).

**Usage**:
```bash
cargo build --release
./target/release/mfs-bench --quick                          # all benchmarks, quick mode
./target/release/mfs-bench --quick sequential               # single category
./target/release/mfs-bench --local-dir /tmp/bench-local \
                           --mfs-dir /mnt/mfs/bench-mfs \
                           --csv /tmp/results.csv
```

Quick mode: 128M sequential, 10s fio runtime, 1K small files.
Full mode: 512M sequential, 30s fio runtime, 10K small files.

fio benchmarks use `--ioengine=posixaio --direct=0` (NOT libaio, which hangs on FUSE/non-native mounts).

#### Config files

`extended/test-cluster/configs/` has example configs for master, metalogger, chunkserver, exports, and mfsmount. These were written for the Docker setup but the values are sensible for native too.

#### Docker files (DEPRECATED)

`docker-compose.yml` and `Dockerfile.moosefs` — these were the Docker-based approach. Abandoned because Docker Desktop (macOS linuxkit VM) can't load kernel modules, making the whole kernel-native client untestable.

## Git situation

The repo currently has:
- **Remote**: `origin` → `https://github.com/moosefs/moosefs` (official upstream, can't push)
- **Branch**: `master`, 5 commits ahead of upstream (the label patches)
- **Unstaged**: The entire `extended/` directory is staged but NOT committed due to a stale `.git/index.lock` that can't be removed (MooseFS filesystem permission issue in Cowork VM)

A tarball of all source files is at: **`extended-moosefs.tar.gz`** in the repo root (120K, 99 files, no build artifacts).

### What to do on Blackberry

**Option A — if you can push to a fork:**
```bash
# On Blackberry, in the moosefs repo checkout:
rm -f .git/index.lock    # clear stale lock
git add extended/
git commit -m "extended: add kernel modules, benchmark suite, and test infrastructure"
git remote add fork git@github.com:YOUR_USER/moosefs.git
git push fork master
```

**Option B — just use the tarball:**
```bash
# Copy the tarball to Blackberry and extract
scp extended-moosefs.tar.gz blackberry:~/
ssh blackberry
cd ~/moosefs   # or wherever the checkout is
tar xzf ~/extended-moosefs.tar.gz
```

## Native setup on Blackberry (the actual task)

Here's what the next Cowork session should do:

### Step 1: Build MooseFS from source (with label patches)

```bash
# The label patches are ALREADY applied to the source tree.
# Just build from the repo as-is.
sudo apt-get install -y build-essential autoconf automake libtool \
    pkg-config zlib1g-dev libfuse3-dev python3

cd /path/to/moosefs
autoreconf -fi
./configure --prefix=/usr --sysconfdir=/etc/mfs --localstatedir=/var/lib \
    --with-default-user=mfs --with-default-group=mfs --disable-mfscgiserv
make -j$(nproc)
sudo make install

# Create mfs user if it doesn't exist
sudo useradd -r -s /bin/false mfs 2>/dev/null || true
sudo mkdir -p /var/lib/mfs
sudo chown mfs:mfs /var/lib/mfs
```

### Step 2: Build kernel modules

```bash
# Install kernel headers
sudo apt-get install -y linux-headers-$(uname -r)

# Build mfs-proto first (dependency)
cd extended/mfs-proto
make clean && make
sudo insmod mfs_proto.ko

# Build mfs-kmod
cd ../mfs-kmod
make clean && make
sudo make install   # installs mfs.ko, mfskmod-helper, mount.mfs
sudo insmod mfs.ko

# Build mfsblk (optional)
cd ../mfsblk
make clean && make
sudo insmod mfsblk.ko
```

### Step 3: Set up native MooseFS cluster

Single-node cluster for benchmarking:

```bash
# Initialize master metadata
sudo -u mfs mfsmaster -i    # first-time init

# Configure exports (allow everything from localhost)
echo "* / rw,alldirs,maproot=0:0" | sudo tee /etc/mfs/mfsexports.cfg

# Start master
sudo mfsmaster start

# Start metalogger
sudo mfsmetalogger start

# Set up chunkserver storage directories
sudo mkdir -p /mnt/mfsdata/cs1 /mnt/mfsdata/cs2 /mnt/mfsdata/cs3
sudo chown mfs:mfs /mnt/mfsdata/cs1 /mnt/mfsdata/cs2 /mnt/mfsdata/cs3

# Configure chunkserver HDD paths (with labels for testing)
# In /etc/mfs/mfshdd.cfg:
#   /mnt/mfsdata/cs1 [label=ssd]
#   /mnt/mfsdata/cs2 [label=hdd]
#   /mnt/mfsdata/cs3 [label=archive]

# Start chunkserver
sudo mfschunkserver start
```

### Step 4: Mount with kernel-native client

```bash
# Create mount point
sudo mkdir -p /mnt/mfs

# Load modules if not already loaded
sudo insmod extended/mfs-proto/mfs_proto.ko 2>/dev/null || true
sudo insmod extended/mfs-kmod/mfs.ko 2>/dev/null || true

# Mount using kernel client
sudo mount -t mfs mfsmaster:9421 /mnt/mfs

# Verify it's kernel-native (NOT fuse)
mount | grep mfs
# Should show: mfsmaster:9421 on /mnt/mfs type mfs (rw,...)
# NOT: mfsmaster:9421 on /mnt/mfs type fuse.mfs (rw,...)

# Create benchmark directory
mkdir -p /mnt/mfs/bench-mfs
```

### Step 5: Build and run benchmarks

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env

# Install benchmark dependencies
sudo apt-get install -y fio protobuf-compiler

# Build benchmark suite
cd extended/test-cluster/benches
cargo build --release

# Run quick benchmark
./target/release/mfs-bench --quick \
    --local-dir /tmp/bench-local \
    --mfs-dir /mnt/mfs/bench-mfs \
    --csv /tmp/mfs-bench-results.csv

# Run full benchmark (slower, more thorough)
./target/release/mfs-bench \
    --local-dir /tmp/bench-local \
    --mfs-dir /mnt/mfs/bench-mfs \
    --csv /tmp/mfs-bench-results-full.csv
```

### Step 6: Test label system

With chunkservers running and labels configured in mfshdd.cfg:

```bash
# Check that master sees disk labels
mfsmaster info   # or check master logs

# Create a file with specific label requirements (if the CLI supports it)
# The label patches modify placement logic in master — files created after
# labels are configured should respect label-based placement.

# Quick validation:
cd extended/test-cluster/scripts
bash test_labels_patches.sh
```

## Important notes

- **ALL fio benchmarks use `posixaio` engine, NOT `libaio`** — libaio hangs on non-native mounts
- **ALL fio benchmarks use `direct=0`** — O_DIRECT can hang or fail on FUSE/kernel-mfs
- The benchmark compares local filesystem vs MooseFS side by side for every test
- Postgres and etcd benchmarks need those services running (install separately if needed)
- SQLite benchmark is self-contained (uses bundled rusqlite)
- The `--quick` flag is recommended for initial validation

## File counts

- Kernel modules: 42 C source files across 4 modules
- Benchmark suite: 16 Rust source files
- Label patches: 5 patch files (already applied to source tree)
- Config files: 8 example configs
- Total: ~77 source files, ~12,983 lines of C, ~2,500 lines of Rust

## Plane project tracking

- Project ID: `fd1b9173-81a8-4f93-9912-44d762a3daaf`
- All 5 work items currently in Done state (`876df6d1-b884-43d1-a014-92cfbefe1d02`)
- May want to create new work items for the native setup + benchmark run
