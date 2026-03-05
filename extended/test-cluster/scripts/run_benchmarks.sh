#!/usr/bin/env bash
# Run the MooseFS benchmark suite
# Usage: ./run_benchmarks.sh [--quick] [category]
#
# Prerequisites: Must be run inside the builder container with
# fio, mfs-bench binary, and database services available.

set -euo pipefail

BENCH_DIR="/workspace/benches"
BINARY="/build/mfs-bench"
LOCAL_DIR="/tmp/bench-local"
MFS_DIR="/mnt/mfs/bench-mfs"
RESULTS_DIR="/build/bench-results"

# Build if binary doesn't exist or source is newer
if [ ! -f "$BINARY" ] || [ "$BENCH_DIR/src/main.rs" -nt "$BINARY" ]; then
    echo "=== Building mfs-bench ==="
    cd "$BENCH_DIR"
    export CARGO_TARGET_DIR=/build/bench-target
    cargo build --release 2>&1 | tail -20
    cp "$CARGO_TARGET_DIR/release/mfs-bench" "$BINARY"
    echo "  Built: $BINARY"
fi

# Ensure local benchmark dir exists
mkdir -p "$LOCAL_DIR"

# Mount MooseFS if not already mounted
if ! mountpoint -q /mnt/mfs 2>/dev/null; then
    echo "=== Mounting MooseFS ==="
    mkdir -p /mnt/mfs
    mfsmount /mnt/mfs -H "${MFS_MASTER:-mfsmaster}" 2>&1 || true
fi
mkdir -p "$MFS_DIR"

# Create results dir
mkdir -p "$RESULTS_DIR"

# Pass-through args
ARGS=("--local-dir" "$LOCAL_DIR" "--mfs-dir" "$MFS_DIR" "--csv" "$RESULTS_DIR/results.csv")

# Add any user args (--quick, category name, etc.)
ARGS+=("$@")

echo "=== Running MooseFS Benchmarks ==="
echo "  Local:   $LOCAL_DIR"
echo "  MooseFS: $MFS_DIR"
echo "  Results: $RESULTS_DIR/results.csv"
echo ""

"$BINARY" "${ARGS[@]}"

echo ""
echo "=== Done. Results in $RESULTS_DIR/results.csv ==="
