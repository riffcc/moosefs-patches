#!/bin/bash
###############################################################################
# run-tests.sh – Master test runner for Extended MooseFS
#
# Usage:
#   cd extended/test-cluster
#   ./run-tests.sh          # Run everything
#   ./run-tests.sh proto    # Run only protocol tests
#   ./run-tests.sh build    # Run only build tests
#   ./run-tests.sh wire     # Run only wire protocol tests
###############################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

SUITE_PASSED=0
SUITE_FAILED=0
SUITE_TOTAL=0

banner() {
    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║  $1${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

run_suite() {
    local name="$1"
    local cmd="$2"
    SUITE_TOTAL=$((SUITE_TOTAL+1))

    echo -e "${YELLOW}▶ Running: ${name}${NC}"
    if eval "$cmd"; then
        SUITE_PASSED=$((SUITE_PASSED+1))
        echo -e "${GREEN}✅ SUITE PASSED: ${name}${NC}"
    else
        SUITE_FAILED=$((SUITE_FAILED+1))
        echo -e "${RED}❌ SUITE FAILED: ${name}${NC}"
    fi
    echo ""
}

# ─── Cluster Management ──────────────────────────────────────────────

start_cluster() {
    banner "Starting MooseFS Docker Cluster"

    # Clean up any previous failed run
    echo "Cleaning up previous containers..."
    docker compose down -v 2>/dev/null || true

    echo "Pulling images..."
    docker compose pull --quiet 2>/dev/null || true

    echo "Starting master first..."
    docker compose up -d mfsmaster

    echo "Waiting for master to be healthy..."
    local timeout=120
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        local health
        health=$(docker inspect --format='{{.State.Health.Status}}' mfs-master 2>/dev/null || echo "unknown")
        if [ "$health" = "healthy" ]; then
            echo -e "${GREEN}Master is healthy!${NC}"
            break
        fi
        sleep 3
        elapsed=$((elapsed+3))
        echo "  ... waiting ($elapsed/${timeout}s) [status: $health]"
        # Show last log line for debugging
        if [ $((elapsed % 15)) -eq 0 ]; then
            echo "  [last log]: $(docker compose logs --tail=1 mfsmaster 2>/dev/null | tail -1)"
        fi
    done

    if [ $elapsed -ge $timeout ]; then
        echo -e "${RED}Master failed to become healthy in ${timeout}s${NC}"
        echo ""
        echo "--- Master Logs ---"
        docker compose logs mfsmaster 2>/dev/null | tail -30
        echo ""
        echo "--- Container State ---"
        docker inspect --format='{{json .State}}' mfs-master 2>/dev/null | python3 -m json.tool 2>/dev/null || true
        exit 1
    fi

    echo "Starting remaining services..."
    docker compose up -d

    # Wait for chunkservers to register and report storage
    echo "Waiting for chunkservers to register and scan disks..."
    sleep 15
    echo -e "${GREEN}Cluster ready!${NC}"

    # Show cluster status
    echo ""
    echo "--- Cluster Status (via mfscli) ---"
    docker compose exec -T builder mfscli -SCS -H 172.29.99.2 2>/dev/null || echo "(mfscli check failed, but master is healthy)"
    echo ""
}

stop_cluster() {
    banner "Stopping MooseFS Docker Cluster"
    docker compose down -v 2>/dev/null || true
}

wait_for_builder() {
    echo "Waiting for builder container to be ready..."
    local timeout=300
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        # Check that gcc, python3, git, AND mfscli are all installed
        if docker compose exec -T builder bash -c 'which gcc && which python3 && which git && which mfscli && test -d /workspace/mfs-proto' 2>/dev/null; then
            echo -e "${GREEN}Builder is ready! (gcc + python3 + git + mfscli + workspace mounted)${NC}"
            return 0
        fi
        sleep 5
        elapsed=$((elapsed+5))
        # Show what's happening inside the builder
        local status
        status=$(docker compose exec -T builder bash -c 'echo "gcc=$(which gcc 2>/dev/null || echo NO) python3=$(which python3 2>/dev/null || echo NO) git=$(which git 2>/dev/null || echo NO) workspace=$(ls /workspace/mfs-proto/*.h 2>/dev/null | wc -l) headers"' 2>/dev/null || echo "container not ready")
        echo "  ... ($elapsed/${timeout}s) $status"
    done
    echo -e "${RED}Builder timed out after ${timeout}s!${NC}"
    echo "Builder logs:"
    docker compose logs --tail=20 builder 2>/dev/null
    return 1
}

# ─── Test Execution ──────────────────────────────────────────────────

run_wire_tests() {
    run_suite "Wire Protocol Tests" \
        "docker compose exec -T builder python3 /workspace/scripts/test_proto_wire.py"
}

run_proto_build() {
    run_suite "Protocol Library Build" \
        "docker compose exec -T builder bash /workspace/scripts/test_proto_build.sh"
}

run_kmod_build() {
    run_suite "Kernel FS Driver Build" \
        "docker compose exec -T builder bash /workspace/scripts/test_kmod_build.sh"
}

run_mfsblk_build() {
    run_suite "Block Device Build" \
        "docker compose exec -T builder bash /workspace/scripts/test_mfsblk_build.sh"
}

run_qemu_build() {
    run_suite "QEMU Plugin Build" \
        "docker compose exec -T builder bash /workspace/scripts/test_qemu_build.sh"
}

run_labels_test() {
    run_suite "Per-Disk Labels Patches" \
        "docker compose exec -T builder bash /workspace/scripts/test_labels_patches.sh"
}

run_fuse_baseline() {
    # Check if client container is actually running first
    local client_state
    client_state=$(docker inspect --format='{{.State.Status}}' mfs-client 2>/dev/null || echo "missing")
    if [ "$client_state" != "running" ]; then
        echo -e "${YELLOW}⚠ FUSE client container is $client_state — waiting 15s for it to stabilize...${NC}"
        sleep 15
        client_state=$(docker inspect --format='{{.State.Status}}' mfs-client 2>/dev/null || echo "missing")
        if [ "$client_state" != "running" ]; then
            echo -e "${YELLOW}⚠ FUSE client still $client_state — skipping FUSE baseline test${NC}"
            echo "  Client logs:"
            docker compose logs --tail=10 mfsclient 2>/dev/null || true
            SUITE_TOTAL=$((SUITE_TOTAL+1))
            SUITE_FAILED=$((SUITE_FAILED+1))
            echo -e "${RED}❌ SUITE FAILED: FUSE Client Baseline (container not running)${NC}"
            return
        fi
    fi
    # Wait for chunkservers to register storage with master
    echo -e "${YELLOW}  Waiting for chunkservers to register storage...${NC}"
    for attempt in $(seq 1 30); do
        avail=$(docker compose exec -T mfsclient sh -c 'df /mnt/mfs 2>/dev/null | tail -1 | awk "{print \$4}"' 2>/dev/null || echo "0")
        if [ "$avail" != "0" ] && [ -n "$avail" ] && [ "$avail" != "-" ]; then
            echo "  Chunkserver storage available after ${attempt}s"
            break
        fi
        sleep 1
    done

    # Show chunkserver logs for diagnostics
    echo "  --- Chunkserver 1 logs (last 5) ---"
    docker compose logs --tail=5 mfschunkserver1 2>/dev/null || true
    echo ""

    run_suite "FUSE Client Baseline" \
        "docker compose exec -T mfsclient sh -c '
            echo \"--- FUSE mount check ---\" &&
            df -h /mnt/mfs &&
            echo \"--- Write test ---\" &&
            dd if=/dev/urandom of=/mnt/mfs/test_file bs=1M count=10 2>&1 &&
            echo \"--- Read test ---\" &&
            md5sum /mnt/mfs/test_file &&
            cp /mnt/mfs/test_file /mnt/mfs/test_file_copy &&
            md5sum /mnt/mfs/test_file_copy &&
            echo \"--- Directory test ---\" &&
            mkdir -p /mnt/mfs/test_dir/sub1/sub2 &&
            touch /mnt/mfs/test_dir/sub1/file1.txt &&
            ls -laR /mnt/mfs/test_dir/ &&
            echo \"--- Cleanup ---\" &&
            rm -rf /mnt/mfs/test_file /mnt/mfs/test_file_copy /mnt/mfs/test_dir &&
            echo \"✅ FUSE baseline passed\"
        '"
}

# ─── Main ─────────────────────────────────────────────────────────────

MODE="${1:-all}"

case "$MODE" in
    all)
        start_cluster
        if ! wait_for_builder; then
            echo -e "${RED}Cannot run tests — builder not ready${NC}"
            exit 1
        fi

        banner "Running All Test Suites"
        run_fuse_baseline
        run_wire_tests
        run_proto_build
        run_kmod_build
        run_mfsblk_build
        run_qemu_build
        run_labels_test
        ;;
    wire)
        start_cluster
        run_wire_tests
        ;;
    build)
        start_cluster
        wait_for_builder
        run_proto_build
        run_kmod_build
        run_mfsblk_build
        run_qemu_build
        ;;
    proto)
        start_cluster
        wait_for_builder
        run_wire_tests
        run_proto_build
        ;;
    labels)
        start_cluster
        wait_for_builder
        run_labels_test
        ;;
    fuse)
        start_cluster
        run_fuse_baseline
        ;;
    stop|down)
        stop_cluster
        exit 0
        ;;
    *)
        echo "Usage: $0 [all|wire|build|proto|labels|fuse|bench|bench-quick|stop]"
        exit 1
        ;;
esac

# ─── Summary ──────────────────────────────────────────────────────────

banner "FINAL RESULTS"
echo -e "Suites passed: ${GREEN}${SUITE_PASSED}${NC}"
echo -e "Suites failed: ${RED}${SUITE_FAILED}${NC}"
echo -e "Total suites:  ${SUITE_TOTAL}"
echo ""

if [ "$SUITE_FAILED" -eq 0 ]; then
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║              ALL TEST SUITES PASSED                     ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"
else
    echo -e "${RED}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║              SOME TEST SUITES FAILED                    ║${NC}"
    echo -e "${RED}╚══════════════════════════════════════════════════════════╝${NC}"
fi

echo ""
echo "To teardown: $0 stop"

exit $( [ "$SUITE_FAILED" -eq 0 ] && echo 0 || echo 1 )
