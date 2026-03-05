#!/bin/bash
###############################################################################
# test_proto_build.sh – Build-test the kernel protocol library (mfs-proto)
###############################################################################

set -euo pipefail

SRC=/workspace/mfs-proto
BUILD=/build/mfs-proto
PASSED=0
FAILED=0
TOTAL=0

pass() { PASSED=$((PASSED+1)); TOTAL=$((TOTAL+1)); echo "  ✅ $1"; }
fail() { FAILED=$((FAILED+1)); TOTAL=$((TOTAL+1)); echo "  ❌ $1: $2"; }

echo "============================================================"
echo "Kernel Protocol Library (mfs-proto) Build Tests"
echo "============================================================"

# --- Source Completeness ---
echo ""
echo "--- Source Completeness ---"

REQUIRED_FILES=(
    mfs_proto.h
    mfs_proto_encode.c mfs_proto_decode.c
    mfs_proto_cs.c mfs_proto_errno.c
    Kbuild Makefile
)

for f in "${REQUIRED_FILES[@]}"; do
    if [ -f "$SRC/$f" ] && [ -s "$SRC/$f" ]; then
        pass "File present: $f ($(wc -l < "$SRC/$f") lines)"
    else
        fail "File missing/empty: $f" "not found"
    fi
done

# --- Protocol Constants ---
echo ""
echo "--- Protocol Constants in Header ---"

if [ -f "$SRC/mfs_proto.h" ]; then
    for const in "CLTOMA_FUSE_REGISTER" "CLTOMA_FUSE_STATFS" "CLTOMA_FUSE_GETATTR" \
                 "CLTOMA_FUSE_LOOKUP" "CLTOMA_FUSE_READCHUNK" \
                 "CLTOCS_READ" "CLTOCS_WRITE" \
                 "MFS_ROOT_ID" "MFS_STATUS_OK"; do
        if grep -q "$const" "$SRC/mfs_proto.h"; then
            pass "Constant defined: $const"
        else
            fail "Missing constant" "$const not in mfs_proto.h"
        fi
    done
fi

# --- Encoder Functions ---
echo ""
echo "--- Encoder Functions ---"

if [ -f "$SRC/mfs_proto_encode.c" ]; then
    for func in "encode.*register|register.*encode" "encode.*lookup|lookup.*encode" \
                "encode.*getattr|getattr.*encode" "encode.*statfs|statfs.*encode" \
                "encode.*mkdir|mkdir.*encode" "encode.*read_chunk|read_chunk.*encode|encode.*readchunk|readchunk.*encode"; do
        if grep -qiE "$func" "$SRC/mfs_proto_encode.c"; then
            pass "Encoder: $func"
        else
            fail "Missing encoder" "$func"
        fi
    done
fi

# --- Errno Mapping ---
echo ""
echo "--- Errno Mapping ---"

if [ -f "$SRC/mfs_proto_errno.c" ]; then
    for errno in "ENOENT" "EPERM" "EACCES" "EEXIST" "ENOTDIR" "EIO"; do
        if grep -q "$errno" "$SRC/mfs_proto_errno.c"; then
            pass "Maps: $errno"
        else
            fail "Missing errno" "$errno not mapped"
        fi
    done
fi

# --- Kernel Module Build ---
echo ""
echo "--- Kernel Module Build ---"

mkdir -p "$BUILD"
cp -r "$SRC"/* "$BUILD"/ 2>/dev/null || true

KVER=$(ls /lib/modules/ 2>/dev/null | head -1)
if [ -z "$KVER" ]; then
    fail "Kernel headers" "Not found"
else
    echo "  Using kernel headers: $KVER"
    cd "$BUILD"
    if make -C /lib/modules/$KVER/build M="$BUILD" modules 2>/build/proto-build.log; then
        pass "Protocol library module compiled"
        ko_file=$(find "$BUILD" -name "*.ko" | head -1)
        if [ -n "$ko_file" ]; then
            pass "Module produced: $(basename $ko_file)"
        fi
    else
        fail "Build failed" "see /build/proto-build.log"
        tail -15 /build/proto-build.log 2>/dev/null | sed 's/^/    /'
    fi
fi

echo ""
echo "============================================================"
echo "Results: $PASSED/$TOTAL passed, $FAILED failed"
echo "============================================================"
exit $( [ "$FAILED" -eq 0 ] && echo 0 || echo 1 )
