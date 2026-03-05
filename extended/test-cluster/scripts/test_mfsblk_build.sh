#!/bin/bash
###############################################################################
# test_mfsblk_build.sh – Build-test the kernel block device driver (mfsblk)
###############################################################################

set -euo pipefail

SRC=/workspace/mfsblk
BUILD=/build/mfsblk
PASSED=0
FAILED=0
TOTAL=0

pass() { PASSED=$((PASSED+1)); TOTAL=$((TOTAL+1)); echo "  ✅ $1"; }
fail() { FAILED=$((FAILED+1)); TOTAL=$((TOTAL+1)); echo "  ❌ $1: $2"; }

echo "============================================================"
echo "Kernel Block Device (mfsblk) Build Tests"
echo "============================================================"

# --- Source Completeness ---
echo ""
echo "--- Source Completeness ---"

REQUIRED_FILES=(
    mfsblk.h
    mfsblk_main.c mfsblk_dev.c mfsblk_io.c
    mfsblk_conn.c mfsblk_proto.c mfsblk_cache.c
    Kbuild Makefile
)

for f in "${REQUIRED_FILES[@]}"; do
    if [ -f "$SRC/$f" ] && [ -s "$SRC/$f" ]; then
        pass "File present: $f"
    else
        fail "File missing/empty: $f" "not found in $SRC"
    fi
done

# --- Structural Analysis ---
echo ""
echo "--- Structural Analysis ---"

# Check for key kernel APIs being referenced
for api in "blk_mq" "register_blkdev" "gendisk" "sysfs" "kernel_connect"; do
    if grep -rql "$api" "$SRC"/*.c "$SRC"/*.h 2>/dev/null; then
        pass "References $api API"
    else
        fail "Missing $api" "no references in source files"
    fi
done

# --- Kernel Module Build ---
echo ""
echo "--- Kernel Module Build ---"

cp -r "$SRC" "$BUILD" 2>/dev/null || true

KVER=$(ls /lib/modules/ 2>/dev/null | head -1)
if [ -z "$KVER" ]; then
    fail "Kernel headers" "Not found"
    echo "  ⚠️  Skipping kernel build (install linux-headers-$(uname -r))"
else
    echo "  Using kernel headers: $KVER"
    cd "$BUILD"
    if make -C /lib/modules/$KVER/build M="$BUILD" modules 2>/build/mfsblk-build.log; then
        pass "mfsblk.ko compiled"
        if [ -f "$BUILD/mfsblk.ko" ]; then
            pass "mfsblk.ko produced"
            modinfo "$BUILD/mfsblk.ko" 2>/dev/null && pass "modinfo valid" || fail "modinfo" "invalid"
        fi
    else
        fail "Kernel module build" "make failed"
        tail -15 /build/mfsblk-build.log 2>/dev/null | sed 's/^/    /'
    fi
fi

echo ""
echo "============================================================"
echo "Results: $PASSED/$TOTAL passed, $FAILED failed"
echo "============================================================"
exit $( [ "$FAILED" -eq 0 ] && echo 0 || echo 1 )
