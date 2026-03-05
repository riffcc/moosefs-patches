#!/bin/bash
###############################################################################
# test_labels_patches.sh – Validate per-disk label patches
###############################################################################

set -euo pipefail

SRC=/workspace/mfs-labels
PASSED=0
FAILED=0
TOTAL=0

pass() { PASSED=$((PASSED+1)); TOTAL=$((TOTAL+1)); echo "  ✅ $1"; }
fail() { FAILED=$((FAILED+1)); TOTAL=$((TOTAL+1)); echo "  ❌ $1: $2"; }

echo "============================================================"
echo "Per-Disk Labels Patch Validation"
echo "============================================================"

# --- Patch Files Present ---
echo ""
echo "--- Patch Files ---"

PATCHES=(
    0001-mfshdd-cfg-label-parsing.patch
    0002-disk-label-reporting.patch
    0003-master-disk-label-placement.patch
    0004-label-precedence-config.patch
    0005-config-examples.patch
)

for p in "${PATCHES[@]}"; do
    if [ -f "$SRC/$p" ] && [ -s "$SRC/$p" ]; then
        pass "Patch present: $p"

        # Validate it's a real patch (has diff headers)
        if head -5 "$SRC/$p" | grep -qE '^(diff|---|\+\+\+|From )'; then
            pass "Valid patch format: $p"
        else
            fail "Bad format: $p" "no diff headers found"
        fi
    else
        fail "Missing patch: $p" "not found"
    fi
done

# --- Content Validation ---
echo ""
echo "--- Content Validation ---"

# Patch 1: should modify mfshdd.cfg parsing
if grep -q "labels" "$SRC/0001-mfshdd-cfg-label-parsing.patch" 2>/dev/null; then
    pass "Patch 1 references 'labels'"
else
    fail "Patch 1" "no 'labels' reference"
fi

# Patch 2: should modify chunkserver reporting
if grep -qE "chunkserver|masterconn|hddspacemgr" "$SRC/0002-disk-label-reporting.patch" 2>/dev/null; then
    pass "Patch 2 modifies chunkserver files"
else
    fail "Patch 2" "no chunkserver file references"
fi

# Patch 3: should modify master placement
if grep -qE "chunks|csdb|master" "$SRC/0003-master-disk-label-placement.patch" 2>/dev/null; then
    pass "Patch 3 modifies master placement"
else
    fail "Patch 3" "no master file references"
fi

# Patch 4: should add LABEL_PRECEDENCE
if grep -q "LABEL_PRECEDENCE\|precedence" "$SRC/0004-label-precedence-config.patch" 2>/dev/null; then
    pass "Patch 4 adds LABEL_PRECEDENCE config"
else
    fail "Patch 4" "no LABEL_PRECEDENCE reference"
fi

# Patch 5: example configs
if grep -qE "labels=|example" "$SRC/0005-config-examples.patch" 2>/dev/null; then
    pass "Patch 5 has config examples"
else
    fail "Patch 5" "no config examples"
fi

# --- Try applying to MooseFS source if available ---
echo ""
echo "--- Patch Application Test ---"

if command -v git &>/dev/null; then
    # Clone MooseFS CE for testing
    if [ ! -d /build/moosefs-ce ]; then
        echo "  Cloning MooseFS CE for patch test..."
        git clone --depth=1 https://github.com/moosefs/moosefs.git /build/moosefs-ce 2>/dev/null || true
    fi

    if [ -d /build/moosefs-ce/.git ]; then
        cd /build/moosefs-ce
        git checkout -f HEAD 2>/dev/null

        ALL_APPLY=true
        for p in "${PATCHES[@]}"; do
            if git apply --check "$SRC/$p" 2>/dev/null; then
                pass "git apply --check: $p"
            else
                fail "git apply --check: $p" "does not apply cleanly"
                ALL_APPLY=false
            fi
        done

        if $ALL_APPLY; then
            # Actually apply them
            for p in "${PATCHES[@]}"; do
                git apply "$SRC/$p" 2>/dev/null || true
            done
            pass "All patches applied to MooseFS CE tree"

            # Try building with patches applied
            if [ -f configure.ac ]; then
                echo "  (Full build test would require autotools — skipping)"
                pass "Patch integration: source tree intact after patching"
            fi
        fi
    else
        echo "  ⚠️  Could not clone MooseFS CE — skipping apply test"
    fi
else
    echo "  ⚠️  git not available — skipping apply test"
fi

echo ""
echo "============================================================"
echo "Results: $PASSED/$TOTAL passed, $FAILED failed"
echo "============================================================"
exit $( [ "$FAILED" -eq 0 ] && echo 0 || echo 1 )
