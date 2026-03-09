#!/usr/bin/env bash
set -euo pipefail

HOST=${HOST:-root@10.7.1.210}
REMOTE_DIR=${REMOTE_DIR:-/root/qemu-mfs}
QEMU_VERSION=${QEMU_VERSION:-10.1.2-7}
LOCAL_DIR=$(cd "$(dirname "$0")/.." && pwd)

echo "Syncing qemu-mfs tree to $HOST:$REMOTE_DIR"
rsync -avz --delete \
    -e "ssh -o BatchMode=yes" \
    "$LOCAL_DIR"/ "$HOST:$REMOTE_DIR"/

echo "Building patched pve-qemu-kvm package on $HOST"
ssh -o BatchMode=yes "$HOST" \
    "bash '$REMOTE_DIR/scripts/build-pve-qemu-package.sh' --version '$QEMU_VERSION' --mfs-src '$REMOTE_DIR'"

echo "Installing patched qemu package and updating MooseFS plugin on $HOST"
ssh -o BatchMode=yes "$HOST" <<EOF
set -euo pipefail
DEB=\$(find /root/qemu-mfs-build -maxdepth 1 -type f -name 'pve-qemu-kvm_*+mfs1_amd64.deb' | sort | tail -n 1)
if [[ -z "\$DEB" ]]; then
    echo "patched pve-qemu-kvm package not found" >&2
    exit 1
fi
dpkg -i "\$DEB"
python3 '$REMOTE_DIR/scripts/patch-proxmox-moosefs-plugin.py'
systemctl restart pvedaemon.service pvestatd.service pveproxy.service
EOF

echo "Installation complete on $HOST"
