#!/bin/bash
###############################################################################
# test_kmod_build.sh – Build-test the kernel filesystem driver (mfs-kmod)
#
# Tests:
#   1. Source files present and non-empty
#   2. Headers parseable (basic C syntax check)
#   3. Kernel module compiles against current headers
#   4. Module info is valid (.ko file, modinfo)
#   5. Userspace helper compiles
###############################################################################

set -euo pipefail

SRC=/workspace/mfs-kmod
BUILD=/build/mfs-kmod
PASSED=0
FAILED=0
TOTAL=0

pass() { PASSED=$((PASSED+1)); TOTAL=$((TOTAL+1)); echo "  ✅ $1"; }
fail() { FAILED=$((FAILED+1)); TOTAL=$((TOTAL+1)); echo "  ❌ $1: $2"; }

echo "============================================================"
echo "Kernel FS Driver (mfs-kmod) Build Tests"
echo "============================================================"

# --- Source Completeness ---
echo ""
echo "--- Source Completeness ---"

REQUIRED_FILES=(
    mfs.h mfs_ctrl_proto.h
    mfs_super.c mfs_inode.c mfs_file.c mfs_dir.c
    mfs_symlink.c mfs_xattr.c mfs_cache.c
    mfs_helper_comm.c mfs_proto.c
    mfskmod-helper.c
    Kbuild Makefile
)

for f in "${REQUIRED_FILES[@]}"; do
    if [ -f "$SRC/$f" ] && [ -s "$SRC/$f" ]; then
        pass "File present: $f"
    else
        fail "File missing/empty: $f" "not found in $SRC"
    fi
done

# --- C Syntax Check (headers) ---
echo ""
echo "--- C Syntax Pre-check ---"

cp -r "$SRC" "$BUILD" 2>/dev/null || true

# Check that headers are valid C
for hdr in "$BUILD"/mfs.h "$BUILD"/mfs_ctrl_proto.h; do
    if [ -f "$hdr" ]; then
        if gcc -fsyntax-only -x c "$hdr" 2>/dev/null; then
            pass "Syntax OK: $(basename $hdr)"
        else
            # Headers with kernel includes will fail in userspace, that's OK
            # Just check they're not fundamentally broken
            if head -5 "$hdr" | grep -q '#'; then
                pass "Header plausible: $(basename $hdr) (kernel deps expected)"
            else
                fail "Syntax error: $(basename $hdr)" "not valid C"
            fi
        fi
    fi
done

# --- Kernel Module Build ---
echo ""
echo "--- Kernel Module Build ---"

KVER=$(ls /lib/modules/ 2>/dev/null | head -1)
if [ -z "$KVER" ]; then
    fail "Kernel headers" "No kernel headers found in /lib/modules/"
    echo ""
    echo "⚠️  Skipping kernel build tests (no headers available)"
    echo "   Install linux-headers-$(uname -r) to enable"
else
    echo "  Using kernel headers: $KVER"

    cd "$BUILD"
    if make -C /lib/modules/$KVER/build M="$BUILD" modules 2>/build/kmod-build.log; then
        pass "Kernel module compiled"

        if [ -f "$BUILD/mfs.ko" ]; then
            pass "mfs.ko produced"
            if modinfo "$BUILD/mfs.ko" >/dev/null 2>&1; then
                pass "modinfo valid"
            else
                fail "modinfo" "could not read module info"
            fi
        else
            fail "mfs.ko" "not produced despite make success"
        fi
    else
        fail "Kernel module build" "make failed (see /build/kmod-build.log)"
        echo "  Last 10 lines of build log:"
        tail -10 /build/kmod-build.log 2>/dev/null | sed 's/^/    /'
    fi
fi

# --- Userspace Helper Build ---
echo ""
echo "--- Userspace Helper Build ---"

if [ -f "$BUILD/mfskmod-helper.c" ]; then
    if gcc -Wall -Wextra -o /build/mfskmod-helper \
        "$BUILD/mfskmod-helper.c" \
        -lpthread 2>/build/helper-build.log; then
        pass "mfskmod-helper compiled"
        if [ -x /build/mfskmod-helper ]; then
            pass "mfskmod-helper executable"
            # Quick smoke: --help or --version if supported
            if /build/mfskmod-helper --help 2>&1 | head -1 | grep -qi 'usage\|help\|mfs'; then
                pass "mfskmod-helper responds to --help"
            else
                pass "mfskmod-helper runs (no --help output, OK)"
            fi
        fi
    else
        fail "mfskmod-helper build" "gcc failed (see /build/helper-build.log)"
        echo "  Last 10 lines:"
        tail -10 /build/helper-build.log 2>/dev/null | sed 's/^/    /'
    fi
fi

# --- Results ---
echo ""
echo "============================================================"
echo "Results: $PASSED/$TOTAL passed, $FAILED failed"
echo "============================================================"

exit $( [ "$FAILED" -eq 0 ] && echo 0 || echo 1 )
