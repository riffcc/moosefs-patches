#!/usr/bin/env bash
set -euo pipefail

VERSION=""
WORKDIR=${WORKDIR:-/root/qemu-mfs-build}
MFS_SRC=${MFS_SRC:-$(cd "$(dirname "$0")/.." && pwd)}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)
            VERSION=$2
            shift 2
            ;;
        --workdir)
            WORKDIR=$2
            shift 2
            ;;
        --mfs-src)
            MFS_SRC=$2
            shift 2
            ;;
        *)
            echo "unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

mkdir -p "$WORKDIR"
cd "$WORKDIR"

export DEBIAN_FRONTEND=noninteractive

SRC_LIST=/etc/apt/sources.list.d/pve-no-subscription-src.list
if [[ ! -f "$SRC_LIST" ]] || ! grep -q '^deb-src .*download\.proxmox\.com/debian/pve .*pve-no-subscription' "$SRC_LIST"; then
    cat > "$SRC_LIST" <<'EOF'
deb-src http://download.proxmox.com/debian/pve trixie pve-no-subscription
EOF
fi

apt-get update
apt-get install -y dpkg-dev devscripts build-essential meson ninja-build fakeroot pkgconf ca-certificates git
apt-get build-dep -y pve-qemu-kvm

rm -rf "$WORKDIR"/pve-qemu-kvm-* "$WORKDIR"/*.dsc "$WORKDIR"/*.orig.tar.* "$WORKDIR"/*.debian.tar.*

if [[ -n "$VERSION" ]]; then
    apt-get source "pve-qemu-kvm=$VERSION"
else
    apt-get source pve-qemu-kvm
fi

SRC_DIR=$(find "$WORKDIR" -maxdepth 1 -mindepth 1 -type d -name 'pve-qemu-kvm-*' | sort | tail -n 1)
if [[ -z "$SRC_DIR" ]]; then
    echo "failed to locate unpacked pve-qemu-kvm source tree in $WORKDIR" >&2
    exit 1
fi

"$MFS_SRC/scripts/integrate-qemu-tree.sh" "$SRC_DIR" "$MFS_SRC"

export DEBFULLNAME="OpenAI Codex"
export DEBEMAIL="codex@local"
dch --chdir "$SRC_DIR" --local +mfs1 "Enable qemu-mfs MooseFS block driver"

(
    cd "$SRC_DIR"
    dpkg-buildpackage -b -uc -us
)

find "$WORKDIR" -maxdepth 1 -type f -name 'pve-qemu-kvm_*+mfs1_amd64.deb' -print
