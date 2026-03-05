#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# MooseFS Extended — Comprehensive Benchmark Suite
# ═══════════════════════════════════════════════════════════════════════════════
#
# Runs identical benchmarks on LOCAL filesystem vs MooseFS FUSE mount to
# produce before/after comparisons across:
#
#   1. Sequential I/O        — dd + fio seq read/write (1M blocks)
#   2. Random I/O            — fio rand read/write/mixed (4K, 64K)
#   3. Metadata Operations   — create/stat/chmod/rename/delete (files + dirs)
#   4. Small File Workload   — 10K x 4K files create/read/delete
#   5. Large File Workload   — single 1GB file write/read
#   6. Mixed Workload        — fio mixed randrw (70/30 read/write)
#   7. Fsync / Durability    — fsync-heavy sequential writes
#   8. Directory Scaling     — readdir performance at 100/1K/10K entries
#   9. Concurrent I/O        — fio with 4/8/16 parallel jobs
#  10. PostgreSQL (pgbench)  — TPC-B-like OLTP workload
#  11. SQLite                — journal + WAL mode OLTP
#  12. etcd                  — key-value put/get/range/txn
#
# Usage:
#   ./benchmark.sh              — run all benchmarks
#   ./benchmark.sh quick        — quick mode (smaller datasets)
#   ./benchmark.sh <category>   — run specific category
#
# ═══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ─── Configuration ────────────────────────────────────────────────────────────

LOCAL_DIR="/tmp/bench-local"
MFS_DIR="/mnt/mfs/bench-mfs"
RESULTS_DIR="/tmp/bench-results"
RESULTS_CSV="$RESULTS_DIR/results.csv"

# Sizing (overridden in quick mode)
SEQ_SIZE="512M"
LARGE_FILE_SIZE="1G"
SMALL_FILE_COUNT=10000
SMALL_FILE_SIZE="4K"
DIR_SCALE_SIZES="100 1000 10000"
FIO_RUNTIME=30
PGBENCH_SCALE=10
PGBENCH_TIME=60
SQLITE_OPS=10000
ETCD_OPS=5000
CONCURRENT_JOBS="4 8 16"

MODE="${1:-all}"

if [ "$MODE" = "quick" ]; then
    SEQ_SIZE="128M"
    LARGE_FILE_SIZE="256M"
    SMALL_FILE_COUNT=1000
    DIR_SCALE_SIZES="100 1000"
    FIO_RUNTIME=10
    PGBENCH_SCALE=5
    PGBENCH_TIME=20
    SQLITE_OPS=2000
    ETCD_OPS=1000
    CONCURRENT_JOBS="4 8"
    MODE="all"
fi

# ─── Colors & Formatting ─────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

banner() {
    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║  $1${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
}

section() {
    echo ""
    echo -e "${YELLOW}▶ $1${NC}"
}

result_line() {
    local test_name="$1"
    local local_val="$2"
    local mfs_val="$3"
    local unit="$4"

    # Calculate ratio
    local ratio=""
    if command -v python3 &>/dev/null && [ -n "$local_val" ] && [ -n "$mfs_val" ]; then
        ratio=$(python3 -c "
l=float('$local_val'); m=float('$mfs_val')
if l > 0:
    r = m/l
    if r >= 1: print(f'{r:.2f}x')
    else: print(f'{r:.2f}x ({(1-r)*100:.0f}% slower)')
else: print('N/A')
" 2>/dev/null || echo "N/A")
    fi

    printf "  %-40s %12s %12s %8s  %s\n" "$test_name" "${local_val} ${unit}" "${mfs_val} ${unit}" "$unit" "$ratio"
    echo "$test_name,$local_val,$mfs_val,$unit,$ratio" >> "$RESULTS_CSV"
}

header_line() {
    printf "  ${BOLD}%-40s %12s %12s %8s  %s${NC}\n" "Test" "Local" "MooseFS" "Unit" "Ratio"
    printf "  %-40s %12s %12s %8s  %s\n" "$(printf '%.0s─' {1..40})" "$(printf '%.0s─' {1..12})" "$(printf '%.0s─' {1..12})" "$(printf '%.0s─' {1..8})" "$(printf '%.0s─' {1..12})"
}

# ─── Setup ────────────────────────────────────────────────────────────────────

setup() {
    banner "MooseFS Benchmark Suite"
    echo "  Mode:      $MODE"
    echo "  Local:     $LOCAL_DIR"
    echo "  MooseFS:   $MFS_DIR"
    echo "  FIO time:  ${FIO_RUNTIME}s"
    echo "  Date:      $(date -Iseconds)"
    echo ""

    mkdir -p "$LOCAL_DIR" "$MFS_DIR" "$RESULTS_DIR"
    echo "test,local,moosefs,unit,ratio" > "$RESULTS_CSV"

    # Install dependencies
    section "Installing benchmark tools..."
    apt-get update -qq > /dev/null 2>&1 || true
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
        fio sysbench postgresql postgresql-client sqlite3 \
        python3 bc > /dev/null 2>&1 || true

    # Install etcd if not present
    if ! command -v etcd &>/dev/null; then
        echo "  Installing etcd..."
        local arch="arm64"
        if [ "$(uname -m)" = "x86_64" ]; then arch="amd64"; fi
        local etcd_ver="v3.5.17"
        curl -fsSL "https://github.com/etcd-io/etcd/releases/download/${etcd_ver}/etcd-${etcd_ver}-linux-${arch}.tar.gz" \
            | tar xzf - -C /usr/local/bin --strip-components=1 \
                "etcd-${etcd_ver}-linux-${arch}/etcd" \
                "etcd-${etcd_ver}-linux-${arch}/etcdctl" 2>/dev/null || echo "  (etcd install failed, will skip etcd benchmarks)"
    fi

    # Drop caches before benchmarks (best effort)
    sync
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true

    echo -e "  ${GREEN}Setup complete${NC}"
}

cleanup_dir() {
    rm -rf "${1:?}"/* 2>/dev/null || true
}

drop_caches() {
    sync
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
}

# ─── Benchmark: extract metric from fio JSON ─────────────────────────────────

fio_extract() {
    local json_file="$1"
    local metric="$2"  # e.g., "read.bw_bytes" or "write.iops" or "read.lat_ns.mean"
    python3 -c "
import json, sys
with open('$json_file') as f: d = json.load(f)
j = d['jobs'][0]
keys = '$metric'.split('.')
v = j
for k in keys:
    v = v[k]
val = float(v)
# Convert bytes/s to MB/s for bandwidth
if 'bw_bytes' in '$metric':
    val = val / (1024*1024)
    print(f'{val:.1f}')
# Convert ns to us for latency
elif 'lat_ns' in '$metric':
    val = val / 1000
    print(f'{val:.1f}')
else:
    print(f'{val:.1f}')
" 2>/dev/null || echo "0"
}

# ═══════════════════════════════════════════════════════════════════════════════
# 1. Sequential I/O
# ═══════════════════════════════════════════════════════════════════════════════

bench_sequential() {
    section "1. Sequential I/O (block size=1M, size=$SEQ_SIZE)"
    header_line

    for target_name in local mfs; do
        if [ "$target_name" = "local" ]; then dir="$LOCAL_DIR"; else dir="$MFS_DIR"; fi
        cleanup_dir "$dir"
        drop_caches

        # Sequential write
        fio --name=seq-write --directory="$dir" --rw=write --bs=1M \
            --size="$SEQ_SIZE" --numjobs=1 --ioengine=libaio --direct=1 \
            --runtime="$FIO_RUNTIME" --time_based --group_reporting \
            --output-format=json --output="$RESULTS_DIR/seq_write_${target_name}.json" \
            > /dev/null 2>&1

        drop_caches

        # Sequential read
        fio --name=seq-read --directory="$dir" --rw=read --bs=1M \
            --size="$SEQ_SIZE" --numjobs=1 --ioengine=libaio --direct=1 \
            --runtime="$FIO_RUNTIME" --time_based --group_reporting \
            --output-format=json --output="$RESULTS_DIR/seq_read_${target_name}.json" \
            > /dev/null 2>&1

        cleanup_dir "$dir"
    done

    local lw=$(fio_extract "$RESULTS_DIR/seq_write_local.json" "write.bw_bytes")
    local mw=$(fio_extract "$RESULTS_DIR/seq_write_mfs.json" "write.bw_bytes")
    local lr=$(fio_extract "$RESULTS_DIR/seq_read_local.json" "read.bw_bytes")
    local mr=$(fio_extract "$RESULTS_DIR/seq_read_mfs.json" "read.bw_bytes")

    local lw_lat=$(fio_extract "$RESULTS_DIR/seq_write_local.json" "write.lat_ns.mean")
    local mw_lat=$(fio_extract "$RESULTS_DIR/seq_write_mfs.json" "write.lat_ns.mean")
    local lr_lat=$(fio_extract "$RESULTS_DIR/seq_read_local.json" "read.lat_ns.mean")
    local mr_lat=$(fio_extract "$RESULTS_DIR/seq_read_mfs.json" "read.lat_ns.mean")

    result_line "Seq Write Throughput" "$lw" "$mw" "MB/s"
    result_line "Seq Read Throughput" "$lr" "$mr" "MB/s"
    result_line "Seq Write Avg Latency" "$lw_lat" "$mw_lat" "µs"
    result_line "Seq Read Avg Latency" "$lr_lat" "$mr_lat" "µs"
}

# ═══════════════════════════════════════════════════════════════════════════════
# 2. Random I/O
# ═══════════════════════════════════════════════════════════════════════════════

bench_random() {
    section "2. Random I/O"
    header_line

    for bs in 4k 64k; do
        for target_name in local mfs; do
            if [ "$target_name" = "local" ]; then dir="$LOCAL_DIR"; else dir="$MFS_DIR"; fi
            cleanup_dir "$dir"
            drop_caches

            # Random write
            fio --name=rand-write-${bs} --directory="$dir" --rw=randwrite --bs="$bs" \
                --size="$SEQ_SIZE" --numjobs=1 --ioengine=libaio --direct=1 --iodepth=32 \
                --runtime="$FIO_RUNTIME" --time_based --group_reporting \
                --output-format=json --output="$RESULTS_DIR/rand_write_${bs}_${target_name}.json" \
                > /dev/null 2>&1

            drop_caches

            # Random read
            fio --name=rand-read-${bs} --directory="$dir" --rw=randread --bs="$bs" \
                --size="$SEQ_SIZE" --numjobs=1 --ioengine=libaio --direct=1 --iodepth=32 \
                --runtime="$FIO_RUNTIME" --time_based --group_reporting \
                --output-format=json --output="$RESULTS_DIR/rand_read_${bs}_${target_name}.json" \
                > /dev/null 2>&1

            cleanup_dir "$dir"
        done

        local lw=$(fio_extract "$RESULTS_DIR/rand_write_${bs}_local.json" "write.iops")
        local mw=$(fio_extract "$RESULTS_DIR/rand_write_${bs}_mfs.json" "write.iops")
        local lr=$(fio_extract "$RESULTS_DIR/rand_read_${bs}_local.json" "read.iops")
        local mr=$(fio_extract "$RESULTS_DIR/rand_read_${bs}_mfs.json" "read.iops")

        local lw_lat=$(fio_extract "$RESULTS_DIR/rand_write_${bs}_local.json" "write.lat_ns.mean")
        local mw_lat=$(fio_extract "$RESULTS_DIR/rand_write_${bs}_mfs.json" "write.lat_ns.mean")
        local lr_lat=$(fio_extract "$RESULTS_DIR/rand_read_${bs}_local.json" "read.lat_ns.mean")
        local mr_lat=$(fio_extract "$RESULTS_DIR/rand_read_${bs}_mfs.json" "read.lat_ns.mean")

        result_line "Rand Write IOPS (${bs})" "$lw" "$mw" "IOPS"
        result_line "Rand Read IOPS (${bs})" "$lr" "$mr" "IOPS"
        result_line "Rand Write Lat (${bs})" "$lw_lat" "$mw_lat" "µs"
        result_line "Rand Read Lat (${bs})" "$lr_lat" "$mr_lat" "µs"
    done
}

# ═══════════════════════════════════════════════════════════════════════════════
# 3. Metadata Operations
# ═══════════════════════════════════════════════════════════════════════════════

bench_metadata() {
    section "3. Metadata Operations (1000 files)"
    header_line

    local count=1000
    for target_name in local mfs; do
        if [ "$target_name" = "local" ]; then dir="$LOCAL_DIR/meta"; else dir="$MFS_DIR/meta"; fi
        mkdir -p "$dir"
        cleanup_dir "$dir"

        python3 -c "
import os, time, json

d = '$dir'
n = $count
results = {}

# CREATE files
t0 = time.monotonic()
for i in range(n):
    open(os.path.join(d, f'f{i}'), 'w').close()
results['create'] = time.monotonic() - t0

# STAT files
t0 = time.monotonic()
for i in range(n):
    os.stat(os.path.join(d, f'f{i}'))
results['stat'] = time.monotonic() - t0

# CHMOD files
t0 = time.monotonic()
for i in range(n):
    os.chmod(os.path.join(d, f'f{i}'), 0o644)
results['chmod'] = time.monotonic() - t0

# RENAME files
t0 = time.monotonic()
for i in range(n):
    os.rename(os.path.join(d, f'f{i}'), os.path.join(d, f'r{i}'))
results['rename'] = time.monotonic() - t0

# UNLINK files
t0 = time.monotonic()
for i in range(n):
    os.unlink(os.path.join(d, f'r{i}'))
results['unlink'] = time.monotonic() - t0

# MKDIR + RMDIR
t0 = time.monotonic()
for i in range(n):
    os.mkdir(os.path.join(d, f'd{i}'))
results['mkdir'] = time.monotonic() - t0

t0 = time.monotonic()
for i in range(n):
    os.rmdir(os.path.join(d, f'd{i}'))
results['rmdir'] = time.monotonic() - t0

# Output as JSON
json.dump(results, open('$RESULTS_DIR/meta_${target_name}.json', 'w'))
" 2>/dev/null

        cleanup_dir "$dir"
    done

    for op in create stat chmod rename unlink mkdir rmdir; do
        local lv=$(python3 -c "
import json
d=json.load(open('$RESULTS_DIR/meta_local.json'))
v=d.get('$op',0)
ops=$count/v if v>0 else 0
print(f'{ops:.0f}')
" 2>/dev/null || echo "0")
        local mv=$(python3 -c "
import json
d=json.load(open('$RESULTS_DIR/meta_mfs.json'))
v=d.get('$op',0)
ops=$count/v if v>0 else 0
print(f'{ops:.0f}')
" 2>/dev/null || echo "0")
        result_line "Meta ${op^^}" "$lv" "$mv" "ops/s"
    done
}

# ═══════════════════════════════════════════════════════════════════════════════
# 4. Small File Workload
# ═══════════════════════════════════════════════════════════════════════════════

bench_small_files() {
    section "4. Small File Workload (${SMALL_FILE_COUNT} x ${SMALL_FILE_SIZE})"
    header_line

    for target_name in local mfs; do
        if [ "$target_name" = "local" ]; then dir="$LOCAL_DIR/small"; else dir="$MFS_DIR/small"; fi
        mkdir -p "$dir"
        cleanup_dir "$dir"

        python3 -c "
import os, time, json

d = '$dir'
n = $SMALL_FILE_COUNT
data = b'x' * 4096  # 4K
results = {}

# CREATE + WRITE
t0 = time.monotonic()
for i in range(n):
    with open(os.path.join(d, f'f{i}'), 'wb') as f:
        f.write(data)
results['create_write'] = time.monotonic() - t0

# READ all
t0 = time.monotonic()
for i in range(n):
    with open(os.path.join(d, f'f{i}'), 'rb') as f:
        f.read()
results['read'] = time.monotonic() - t0

# DELETE all
t0 = time.monotonic()
for i in range(n):
    os.unlink(os.path.join(d, f'f{i}'))
results['delete'] = time.monotonic() - t0

json.dump(results, open('$RESULTS_DIR/small_${target_name}.json', 'w'))
" 2>/dev/null

        cleanup_dir "$dir"
    done

    for op in create_write read delete; do
        local lv=$(python3 -c "
import json; d=json.load(open('$RESULTS_DIR/small_local.json'))
v=d.get('$op',0); ops=$SMALL_FILE_COUNT/v if v>0 else 0
print(f'{ops:.0f}')
" 2>/dev/null || echo "0")
        local mv=$(python3 -c "
import json; d=json.load(open('$RESULTS_DIR/small_mfs.json'))
v=d.get('$op',0); ops=$SMALL_FILE_COUNT/v if v>0 else 0
print(f'{ops:.0f}')
" 2>/dev/null || echo "0")
        local label=$(echo "$op" | tr '_' ' ')
        result_line "Small File ${label}" "$lv" "$mv" "files/s"
    done
}

# ═══════════════════════════════════════════════════════════════════════════════
# 5. Large File Workload
# ═══════════════════════════════════════════════════════════════════════════════

bench_large_file() {
    section "5. Large File Workload (single ${LARGE_FILE_SIZE} file)"
    header_line

    for target_name in local mfs; do
        if [ "$target_name" = "local" ]; then dir="$LOCAL_DIR"; else dir="$MFS_DIR"; fi
        drop_caches

        # Write
        local t0=$(date +%s%N)
        dd if=/dev/zero of="$dir/largefile" bs=1M count=$(echo "$LARGE_FILE_SIZE" | sed 's/M//;s/G/*1024/' | bc) \
            conv=fdatasync 2>"$RESULTS_DIR/dd_write_${target_name}.txt"
        local t1=$(date +%s%N)
        local write_ms=$(( (t1 - t0) / 1000000 ))

        drop_caches

        # Read
        t0=$(date +%s%N)
        dd if="$dir/largefile" of=/dev/null bs=1M 2>"$RESULTS_DIR/dd_read_${target_name}.txt"
        t1=$(date +%s%N)
        local read_ms=$(( (t1 - t0) / 1000000 ))

        # Extract throughput from dd
        python3 -c "
import re, json
results = {}
for op in ['write', 'read']:
    with open(f'$RESULTS_DIR/dd_{op}_${target_name}.txt') as f:
        txt = f.read()
    # dd outputs like '1073741824 bytes (1.1 GB, 1.0 GiB) copied, 2.52346 s, 425 MB/s'
    m = re.search(r'([\d.]+)\s+(GB|MB|kB)/s', txt)
    if m:
        val = float(m.group(1))
        if m.group(2) == 'GB': val *= 1024
        elif m.group(2) == 'kB': val /= 1024
        results[op] = val
    else:
        results[op] = 0
json.dump(results, open(f'$RESULTS_DIR/large_${target_name}.json', 'w'))
" 2>/dev/null

        rm -f "$dir/largefile"
    done

    local lw=$(python3 -c "import json; print(f\"{json.load(open('$RESULTS_DIR/large_local.json'))['write']:.1f}\")" 2>/dev/null || echo "0")
    local mw=$(python3 -c "import json; print(f\"{json.load(open('$RESULTS_DIR/large_mfs.json'))['write']:.1f}\")" 2>/dev/null || echo "0")
    local lr=$(python3 -c "import json; print(f\"{json.load(open('$RESULTS_DIR/large_local.json'))['read']:.1f}\")" 2>/dev/null || echo "0")
    local mr=$(python3 -c "import json; print(f\"{json.load(open('$RESULTS_DIR/large_mfs.json'))['read']:.1f}\")" 2>/dev/null || echo "0")

    result_line "Large File Write" "$lw" "$mw" "MB/s"
    result_line "Large File Read" "$lr" "$mr" "MB/s"
}

# ═══════════════════════════════════════════════════════════════════════════════
# 6. Mixed Workload (70% read / 30% write)
# ═══════════════════════════════════════════════════════════════════════════════

bench_mixed() {
    section "6. Mixed Workload (70/30 read/write, 4K random)"
    header_line

    for target_name in local mfs; do
        if [ "$target_name" = "local" ]; then dir="$LOCAL_DIR"; else dir="$MFS_DIR"; fi
        cleanup_dir "$dir"

        fio --name=mixed --directory="$dir" --rw=randrw --rwmixread=70 --bs=4k \
            --size="$SEQ_SIZE" --numjobs=1 --ioengine=libaio --direct=1 --iodepth=32 \
            --runtime="$FIO_RUNTIME" --time_based --group_reporting \
            --output-format=json --output="$RESULTS_DIR/mixed_${target_name}.json" \
            > /dev/null 2>&1

        cleanup_dir "$dir"
    done

    local lr=$(fio_extract "$RESULTS_DIR/mixed_local.json" "read.iops")
    local mr=$(fio_extract "$RESULTS_DIR/mixed_mfs.json" "read.iops")
    local lw=$(fio_extract "$RESULTS_DIR/mixed_local.json" "write.iops")
    local mw=$(fio_extract "$RESULTS_DIR/mixed_mfs.json" "write.iops")
    local lr_lat=$(fio_extract "$RESULTS_DIR/mixed_local.json" "read.lat_ns.mean")
    local mr_lat=$(fio_extract "$RESULTS_DIR/mixed_mfs.json" "read.lat_ns.mean")
    local lw_lat=$(fio_extract "$RESULTS_DIR/mixed_local.json" "write.lat_ns.mean")
    local mw_lat=$(fio_extract "$RESULTS_DIR/mixed_mfs.json" "write.lat_ns.mean")

    result_line "Mixed Read IOPS" "$lr" "$mr" "IOPS"
    result_line "Mixed Write IOPS" "$lw" "$mw" "IOPS"
    result_line "Mixed Read Latency" "$lr_lat" "$mr_lat" "µs"
    result_line "Mixed Write Latency" "$lw_lat" "$mw_lat" "µs"
}

# ═══════════════════════════════════════════════════════════════════════════════
# 7. Fsync / Durability Workload
# ═══════════════════════════════════════════════════════════════════════════════

bench_fsync() {
    section "7. Fsync-Heavy Workload (4K writes with fsync)"
    header_line

    for target_name in local mfs; do
        if [ "$target_name" = "local" ]; then dir="$LOCAL_DIR"; else dir="$MFS_DIR"; fi
        cleanup_dir "$dir"

        fio --name=fsync-write --directory="$dir" --rw=write --bs=4k \
            --size=64M --numjobs=1 --ioengine=sync --fsync=1 \
            --runtime="$FIO_RUNTIME" --time_based --group_reporting \
            --output-format=json --output="$RESULTS_DIR/fsync_${target_name}.json" \
            > /dev/null 2>&1

        cleanup_dir "$dir"
    done

    local lw=$(fio_extract "$RESULTS_DIR/fsync_local.json" "write.iops")
    local mw=$(fio_extract "$RESULTS_DIR/fsync_mfs.json" "write.iops")
    local lw_lat=$(fio_extract "$RESULTS_DIR/fsync_local.json" "write.lat_ns.mean")
    local mw_lat=$(fio_extract "$RESULTS_DIR/fsync_mfs.json" "write.lat_ns.mean")

    result_line "Fsync Write IOPS" "$lw" "$mw" "IOPS"
    result_line "Fsync Write Latency" "$lw_lat" "$mw_lat" "µs"
}

# ═══════════════════════════════════════════════════════════════════════════════
# 8. Directory Scaling
# ═══════════════════════════════════════════════════════════════════════════════

bench_directory_scale() {
    section "8. Directory Scaling (readdir at various sizes)"
    header_line

    for sz in $DIR_SCALE_SIZES; do
        for target_name in local mfs; do
            if [ "$target_name" = "local" ]; then dir="$LOCAL_DIR/dirscale"; else dir="$MFS_DIR/dirscale"; fi
            mkdir -p "$dir"
            cleanup_dir "$dir"

            # Create files
            python3 -c "
import os
d = '$dir'
for i in range($sz):
    open(os.path.join(d, f'f{i}'), 'w').close()
" 2>/dev/null

            # Benchmark readdir
            python3 -c "
import os, time, json
d = '$dir'
# Warm up
os.listdir(d)
# Measure 100 iterations
t0 = time.monotonic()
for _ in range(100):
    os.listdir(d)
elapsed = time.monotonic() - t0
ops_per_sec = 100 / elapsed
json.dump({'readdir_ops': ops_per_sec}, open('$RESULTS_DIR/dirscale_${sz}_${target_name}.json', 'w'))
" 2>/dev/null

            cleanup_dir "$dir"
        done

        local lv=$(python3 -c "import json; print(f\"{json.load(open('$RESULTS_DIR/dirscale_${sz}_local.json'))['readdir_ops']:.0f}\")" 2>/dev/null || echo "0")
        local mv=$(python3 -c "import json; print(f\"{json.load(open('$RESULTS_DIR/dirscale_${sz}_mfs.json'))['readdir_ops']:.0f}\")" 2>/dev/null || echo "0")
        result_line "Readdir (${sz} entries)" "$lv" "$mv" "ops/s"
    done
}

# ═══════════════════════════════════════════════════════════════════════════════
# 9. Concurrent I/O
# ═══════════════════════════════════════════════════════════════════════════════

bench_concurrent() {
    section "9. Concurrent I/O (parallel fio jobs, 4K random)"
    header_line

    for jobs in $CONCURRENT_JOBS; do
        for target_name in local mfs; do
            if [ "$target_name" = "local" ]; then dir="$LOCAL_DIR"; else dir="$MFS_DIR"; fi
            cleanup_dir "$dir"

            fio --name=concurrent --directory="$dir" --rw=randrw --rwmixread=50 --bs=4k \
                --size=64M --numjobs="$jobs" --ioengine=libaio --direct=1 --iodepth=16 \
                --runtime="$FIO_RUNTIME" --time_based --group_reporting \
                --output-format=json --output="$RESULTS_DIR/concurrent_${jobs}_${target_name}.json" \
                > /dev/null 2>&1

            cleanup_dir "$dir"
        done

        # For group_reporting, total IOPS is in jobs[0]
        local lr=$(fio_extract "$RESULTS_DIR/concurrent_${jobs}_local.json" "read.iops")
        local mr=$(fio_extract "$RESULTS_DIR/concurrent_${jobs}_mfs.json" "read.iops")
        local lw=$(fio_extract "$RESULTS_DIR/concurrent_${jobs}_local.json" "write.iops")
        local mw=$(fio_extract "$RESULTS_DIR/concurrent_${jobs}_mfs.json" "write.iops")

        result_line "Concurrent ${jobs}j Read IOPS" "$lr" "$mr" "IOPS"
        result_line "Concurrent ${jobs}j Write IOPS" "$lw" "$mw" "IOPS"
    done
}

# ═══════════════════════════════════════════════════════════════════════════════
# 10. PostgreSQL (pgbench)
# ═══════════════════════════════════════════════════════════════════════════════

bench_postgres() {
    section "10. PostgreSQL (pgbench TPC-B, scale=$PGBENCH_SCALE)"
    header_line

    if ! command -v pg_isready &>/dev/null; then
        echo "  (PostgreSQL not available, skipping)"
        return
    fi

    for target_name in local mfs; do
        if [ "$target_name" = "local" ]; then dir="$LOCAL_DIR/pgdata"; else dir="$MFS_DIR/pgdata"; fi
        rm -rf "$dir" 2>/dev/null || true
        mkdir -p "$dir"

        # Initialize PostgreSQL
        chown postgres:postgres "$dir" 2>/dev/null || true
        su -s /bin/bash postgres -c "initdb -D '$dir'" > /dev/null 2>&1 || {
            # If su fails (no postgres user), try as root with a custom user
            pg_ctl stop -D "$dir" 2>/dev/null || true
            initdb -D "$dir" --username=root 2>/dev/null || {
                echo "  (Cannot init PostgreSQL in $target_name, skipping)"
                continue
            }
        }

        # Start PostgreSQL
        local pg_port=$((5432 + RANDOM % 1000))
        local pg_user=$(whoami)
        pg_ctl -D "$dir" -l "$RESULTS_DIR/pg_${target_name}.log" \
            -o "-p $pg_port -k /tmp" start > /dev/null 2>&1 || {
            echo "  (Cannot start PostgreSQL on $target_name, skipping)"
            continue
        }

        sleep 2

        # Create test database
        createdb -h /tmp -p "$pg_port" benchdb 2>/dev/null || true

        # Initialize pgbench
        pgbench -h /tmp -p "$pg_port" -i -s "$PGBENCH_SCALE" benchdb > /dev/null 2>&1

        # Run pgbench
        pgbench -h /tmp -p "$pg_port" -T "$PGBENCH_TIME" -c 4 -j 2 --no-vacuum \
            benchdb > "$RESULTS_DIR/pgbench_${target_name}.txt" 2>&1

        # Stop PostgreSQL
        pg_ctl -D "$dir" stop > /dev/null 2>&1

        rm -rf "$dir" 2>/dev/null || true
    done

    # Extract TPS
    for target_name in local mfs; do
        if [ -f "$RESULTS_DIR/pgbench_${target_name}.txt" ]; then
            eval "pg_tps_${target_name}=$(python3 -c "
import re
with open('$RESULTS_DIR/pgbench_${target_name}.txt') as f: txt = f.read()
m = re.search(r'tps = ([\d.]+)', txt)
print(f'{float(m.group(1)):.1f}' if m else '0')
" 2>/dev/null || echo "0")"

            eval "pg_lat_${target_name}=$(python3 -c "
import re
with open('$RESULTS_DIR/pgbench_${target_name}.txt') as f: txt = f.read()
m = re.search(r'latency average = ([\d.]+)', txt)
print(f'{float(m.group(1)):.2f}' if m else '0')
" 2>/dev/null || echo "0")"
        else
            eval "pg_tps_${target_name}=N/A"
            eval "pg_lat_${target_name}=N/A"
        fi
    done

    result_line "PostgreSQL TPS (TPC-B)" "${pg_tps_local:-N/A}" "${pg_tps_mfs:-N/A}" "tps"
    result_line "PostgreSQL Avg Latency" "${pg_lat_local:-N/A}" "${pg_lat_mfs:-N/A}" "ms"
}

# ═══════════════════════════════════════════════════════════════════════════════
# 11. SQLite
# ═══════════════════════════════════════════════════════════════════════════════

bench_sqlite() {
    section "11. SQLite (${SQLITE_OPS} ops, journal + WAL)"
    header_line

    if ! command -v sqlite3 &>/dev/null; then
        echo "  (SQLite not available, skipping)"
        return
    fi

    for target_name in local mfs; do
        if [ "$target_name" = "local" ]; then dir="$LOCAL_DIR"; else dir="$MFS_DIR"; fi

        python3 -c "
import sqlite3, time, os, json, random, string

results = {}
n = $SQLITE_OPS

for mode in ['journal', 'wal']:
    db_path = os.path.join('$dir', f'bench_{mode}.db')
    if os.path.exists(db_path):
        os.unlink(db_path)

    conn = sqlite3.connect(db_path)
    if mode == 'wal':
        conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=FULL')
    conn.execute('CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)')
    conn.commit()

    # INSERT
    t0 = time.monotonic()
    for i in range(n):
        conn.execute('INSERT INTO kv VALUES (?, ?)', (f'key{i}', 'x' * 100))
        if i % 100 == 0:
            conn.commit()
    conn.commit()
    results[f'{mode}_insert'] = time.monotonic() - t0

    # SELECT (random)
    t0 = time.monotonic()
    for i in range(n):
        k = f'key{random.randint(0, n-1)}'
        conn.execute('SELECT v FROM kv WHERE k=?', (k,)).fetchone()
    results[f'{mode}_select'] = time.monotonic() - t0

    # UPDATE (random)
    t0 = time.monotonic()
    for i in range(n):
        k = f'key{random.randint(0, n-1)}'
        conn.execute('UPDATE kv SET v=? WHERE k=?', ('y' * 100, k))
        if i % 100 == 0:
            conn.commit()
    conn.commit()
    results[f'{mode}_update'] = time.monotonic() - t0

    conn.close()
    os.unlink(db_path)

json.dump(results, open('$RESULTS_DIR/sqlite_${target_name}.json', 'w'))
" 2>/dev/null
    done

    for mode in journal wal; do
        for op in insert select update; do
            local lv=$(python3 -c "
import json; d=json.load(open('$RESULTS_DIR/sqlite_local.json'))
v=d.get('${mode}_${op}',0); ops=$SQLITE_OPS/v if v>0 else 0
print(f'{ops:.0f}')
" 2>/dev/null || echo "0")
            local mv=$(python3 -c "
import json; d=json.load(open('$RESULTS_DIR/sqlite_mfs.json'))
v=d.get('${mode}_${op}',0); ops=$SQLITE_OPS/v if v>0 else 0
print(f'{ops:.0f}')
" 2>/dev/null || echo "0")
            result_line "SQLite ${mode^^} ${op^^}" "$lv" "$mv" "ops/s"
        done
    done
}

# ═══════════════════════════════════════════════════════════════════════════════
# 12. etcd
# ═══════════════════════════════════════════════════════════════════════════════

bench_etcd() {
    section "12. etcd (${ETCD_OPS} operations)"
    header_line

    if ! command -v etcd &>/dev/null; then
        echo "  (etcd not available, skipping)"
        return
    fi

    for target_name in local mfs; do
        if [ "$target_name" = "local" ]; then dir="$LOCAL_DIR/etcd-data"; else dir="$MFS_DIR/etcd-data"; fi
        rm -rf "$dir" 2>/dev/null || true
        mkdir -p "$dir"

        local etcd_port=$((2379 + RANDOM % 1000))
        local peer_port=$((etcd_port + 1))

        # Start etcd
        etcd --data-dir="$dir" \
            --listen-client-urls="http://127.0.0.1:${etcd_port}" \
            --advertise-client-urls="http://127.0.0.1:${etcd_port}" \
            --listen-peer-urls="http://127.0.0.1:${peer_port}" \
            --initial-advertise-peer-urls="http://127.0.0.1:${peer_port}" \
            --initial-cluster="default=http://127.0.0.1:${peer_port}" \
            > "$RESULTS_DIR/etcd_${target_name}.log" 2>&1 &
        local etcd_pid=$!
        sleep 3

        if ! kill -0 "$etcd_pid" 2>/dev/null; then
            echo "  (etcd failed to start on $target_name, skipping)"
            continue
        fi

        local ep="--endpoints=http://127.0.0.1:${etcd_port}"

        python3 -c "
import subprocess, time, json, random, string

n = $ETCD_OPS
ep = '$ep'
results = {}

def etcdctl(*args):
    cmd = ['etcdctl'] + ep.split('=')[1:] + list(args)
    # Fix: etcdctl wants --endpoints=url
    cmd = ['etcdctl', '$ep'] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=30)

# PUT sequential
t0 = time.monotonic()
for i in range(n):
    etcdctl('put', f'key{i}', 'x' * 100)
results['put'] = time.monotonic() - t0

# GET random
t0 = time.monotonic()
for i in range(n):
    etcdctl('get', f'key{random.randint(0, n-1)}')
results['get'] = time.monotonic() - t0

# RANGE (prefix scan)
t0 = time.monotonic()
for i in range(min(100, n)):
    etcdctl('get', '--prefix', f'key{i}')
results['range'] = time.monotonic() - t0
results['range_count'] = min(100, n)

# TXN (compare-and-swap)
t0 = time.monotonic()
for i in range(min(n, 500)):
    # Simple txn: if key exists, update it
    proc = subprocess.run(
        ['etcdctl', '$ep', 'txn', '--interactive=false'],
        input=f'value(\"key{i}\") = \"\" \n\nput key{i} updated\n\n',
        capture_output=True, text=True, timeout=10
    )
results['txn'] = time.monotonic() - t0
results['txn_count'] = min(n, 500)

json.dump(results, open('$RESULTS_DIR/etcd_${target_name}.json', 'w'))
" 2>/dev/null || true

        # Stop etcd
        kill "$etcd_pid" 2>/dev/null || true
        wait "$etcd_pid" 2>/dev/null || true
        rm -rf "$dir"
    done

    for op in put get; do
        local lv=$(python3 -c "
import json; d=json.load(open('$RESULTS_DIR/etcd_local.json'))
v=d.get('$op',0); ops=$ETCD_OPS/v if v>0 else 0
print(f'{ops:.0f}')
" 2>/dev/null || echo "0")
        local mv=$(python3 -c "
import json; d=json.load(open('$RESULTS_DIR/etcd_mfs.json'))
v=d.get('$op',0); ops=$ETCD_OPS/v if v>0 else 0
print(f'{ops:.0f}')
" 2>/dev/null || echo "0")
        result_line "etcd ${op^^}" "$lv" "$mv" "ops/s"
    done

    # Range ops
    local lr=$(python3 -c "
import json; d=json.load(open('$RESULTS_DIR/etcd_local.json'))
v=d.get('range',0); c=d.get('range_count',100); ops=c/v if v>0 else 0
print(f'{ops:.0f}')
" 2>/dev/null || echo "0")
    local mr=$(python3 -c "
import json; d=json.load(open('$RESULTS_DIR/etcd_mfs.json'))
v=d.get('range',0); c=d.get('range_count',100); ops=c/v if v>0 else 0
print(f'{ops:.0f}')
" 2>/dev/null || echo "0")
    result_line "etcd RANGE (prefix)" "$lr" "$mr" "ops/s"

    # TXN ops
    local lt=$(python3 -c "
import json; d=json.load(open('$RESULTS_DIR/etcd_local.json'))
v=d.get('txn',0); c=d.get('txn_count',500); ops=c/v if v>0 else 0
print(f'{ops:.0f}')
" 2>/dev/null || echo "0")
    local mt=$(python3 -c "
import json; d=json.load(open('$RESULTS_DIR/etcd_mfs.json'))
v=d.get('txn',0); c=d.get('txn_count',500); ops=c/v if v>0 else 0
print(f'{ops:.0f}')
" 2>/dev/null || echo "0")
    result_line "etcd TXN (CAS)" "$lt" "$mt" "ops/s"
}

# ═══════════════════════════════════════════════════════════════════════════════
# Final Summary
# ═══════════════════════════════════════════════════════════════════════════════

print_summary() {
    banner "BENCHMARK COMPLETE"

    echo ""
    echo -e "${BOLD}Full Results:${NC}"
    echo ""
    printf "  ${BOLD}%-40s %12s %12s %8s  %-14s${NC}\n" "Test" "Local" "MooseFS" "Unit" "Ratio"
    printf "  %-40s %12s %12s %8s  %-14s\n" \
        "$(printf '%.0s─' {1..40})" "$(printf '%.0s─' {1..12})" \
        "$(printf '%.0s─' {1..12})" "$(printf '%.0s─' {1..8})" "$(printf '%.0s─' {1..14})"

    # Re-read CSV and print
    tail -n +2 "$RESULTS_CSV" | while IFS=',' read -r test local_val mfs_val unit ratio; do
        printf "  %-40s %12s %12s %8s  %s\n" "$test" "$local_val $unit" "$mfs_val $unit" "" "$ratio"
    done

    echo ""
    echo -e "  Results saved to: ${GREEN}$RESULTS_CSV${NC}"
    echo -e "  Raw data in:      ${GREEN}$RESULTS_DIR/${NC}"
    echo ""

    # Quick summary stats
    python3 -c "
import csv
ratios = []
with open('$RESULTS_CSV') as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            l = float(row['local'])
            m = float(row['moosefs'])
            if l > 0:
                ratios.append(m/l)
        except: pass

if ratios:
    avg = sum(ratios) / len(ratios)
    mn = min(ratios)
    mx = max(ratios)
    med = sorted(ratios)[len(ratios)//2]
    print(f'  Summary across {len(ratios)} metrics:')
    print(f'    Median ratio:  {med:.2f}x')
    print(f'    Mean ratio:    {avg:.2f}x')
    print(f'    Best:          {mx:.2f}x')
    print(f'    Worst:         {mn:.2f}x')
    if med < 0.5:
        print(f'    MooseFS is significantly slower (expected for distributed FS)')
    elif med < 0.9:
        print(f'    MooseFS shows moderate overhead vs local')
    elif med < 1.1:
        print(f'    MooseFS is roughly comparable to local')
    else:
        print(f'    MooseFS outperforms local (likely caching effects)')
" 2>/dev/null || true
}

# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

main() {
    setup

    case "$MODE" in
        all)
            bench_sequential
            bench_random
            bench_metadata
            bench_small_files
            bench_large_file
            bench_mixed
            bench_fsync
            bench_directory_scale
            bench_concurrent
            bench_postgres
            bench_sqlite
            bench_etcd
            ;;
        sequential)   bench_sequential ;;
        random)       bench_random ;;
        metadata)     bench_metadata ;;
        small)        bench_small_files ;;
        large)        bench_large_file ;;
        mixed)        bench_mixed ;;
        fsync)        bench_fsync ;;
        dirscale)     bench_directory_scale ;;
        concurrent)   bench_concurrent ;;
        postgres)     bench_postgres ;;
        sqlite)       bench_sqlite ;;
        etcd)         bench_etcd ;;
        *)
            echo "Usage: $0 [all|quick|sequential|random|metadata|small|large|mixed|fsync|dirscale|concurrent|postgres|sqlite|etcd]"
            exit 1
            ;;
    esac

    print_summary
}

main "$@"
