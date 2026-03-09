#!/usr/bin/env bash
set -euo pipefail

VERSION=""
WORKDIR=${WORKDIR:-/root/qemu-mfs-build}
MFS_SRC=${MFS_SRC:-$(cd "$(dirname "$0")/.." && pwd)}
CODENAME=${CODENAME:-}
SKIP_BUILD_DEPS=0
QEMU_GIT_URL=${QEMU_GIT_URL:-git://git.proxmox.com/git/pve-qemu.git}
QEMU_GIT_REF=${QEMU_GIT_REF:-}
DISABLE_DOCS=0

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
        --codename)
            CODENAME=$2
            shift 2
            ;;
        --git-url)
            QEMU_GIT_URL=$2
            shift 2
            ;;
        --git-ref)
            QEMU_GIT_REF=$2
            shift 2
            ;;
        --skip-build-deps)
            SKIP_BUILD_DEPS=1
            shift
            ;;
        --disable-docs)
            DISABLE_DOCS=1
            shift
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

if [[ -z "$CODENAME" ]]; then
    CODENAME=$(. /etc/os-release && printf '%s' "${VERSION_CODENAME:-}")
fi

if [[ -z "$CODENAME" ]]; then
    echo "failed to detect Debian codename" >&2
    exit 1
fi

SRC_FILE=/etc/apt/sources.list.d/qemu-mfs-build-src.sources
rm -f /etc/apt/sources.list.d/pve-no-subscription-src.list
rm -f /etc/apt/sources.list.d/qemu-mfs-build-src.list
cat > "$SRC_FILE" <<EOF
Types: deb-src
URIs: http://deb.debian.org/debian
Suites: ${CODENAME} ${CODENAME}-updates
Components: main contrib non-free non-free-firmware

Types: deb-src
URIs: http://deb.debian.org/debian-security
Suites: ${CODENAME}-security
Components: main contrib non-free non-free-firmware
EOF

apt-get update
apt-get install -y dpkg-dev devscripts build-essential meson ninja-build fakeroot pkgconf ca-certificates git

if [[ "$SKIP_BUILD_DEPS" -eq 0 ]]; then
    apt-get build-dep -y pve-qemu-kvm || true
fi

SRC_DIR="$WORKDIR/pve-qemu"
if [[ -d "$SRC_DIR/.git" ]]; then
    EXISTING_URL=$(git -C "$SRC_DIR" remote get-url origin 2>/dev/null || true)
    if [[ "$EXISTING_URL" != "$QEMU_GIT_URL" ]]; then
        rm -rf "$SRC_DIR"
        git clone "$QEMU_GIT_URL" "$SRC_DIR"
    else
        git -C "$SRC_DIR" fetch --tags origin
    fi
else
    rm -rf "$SRC_DIR"
    git clone "$QEMU_GIT_URL" "$SRC_DIR"
fi

if [[ -n "$QEMU_GIT_REF" ]]; then
    git -C "$SRC_DIR" checkout "$QEMU_GIT_REF"
elif [[ -n "$VERSION" ]]; then
    if git -C "$SRC_DIR" rev-parse -q --verify "refs/tags/v$VERSION" >/dev/null; then
        git -C "$SRC_DIR" checkout "v$VERSION"
    elif git -C "$SRC_DIR" rev-parse -q --verify "refs/tags/$VERSION" >/dev/null; then
        git -C "$SRC_DIR" checkout "$VERSION"
    else
        echo "warning: no matching git ref for version ${VERSION}, using repository default branch" >&2
    fi
fi

git -C "$SRC_DIR" reset --hard HEAD
git -C "$SRC_DIR" clean -fdx
git -C "$SRC_DIR" submodule update --init --recursive

QEMU_TREE="$SRC_DIR"
if [[ -d "$SRC_DIR/qemu" ]]; then
    QEMU_TREE="$SRC_DIR/qemu"
fi

(
    cd "$QEMU_TREE"
    meson subprojects download
)

"$MFS_SRC/scripts/integrate-qemu-tree.sh" "$QEMU_TREE" "$MFS_SRC"

if [[ "$DISABLE_DOCS" -eq 1 ]]; then
    sed -i 's/--enable-docs/--disable-docs/' "$SRC_DIR/debian/rules"
fi

export DEBFULLNAME="OpenAI Codex"
export DEBEMAIL="codex@local"
(
    cd "$SRC_DIR"
    dch --local +mfs1 "Enable qemu-mfs MooseFS block driver"
)

(
    cd "$SRC_DIR"
    make deb
)

find "$WORKDIR" -maxdepth 1 -type f -name 'pve-qemu-kvm_*+mfs1_amd64.deb' -print
