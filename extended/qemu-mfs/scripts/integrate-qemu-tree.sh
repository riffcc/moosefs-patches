#!/usr/bin/env bash
set -euo pipefail

QEMU_SRC=${1:?usage: integrate-qemu-tree.sh <qemu-source-root> [qemu-mfs-root]}
MFS_SRC=${2:-$(cd "$(dirname "$0")/.." && pwd)}

for f in block/mfs.c block/mfs-proto.c block/mfs-proto.h block/mfs-conn.c block/mfs-conn.h; do
    install -D -m 0644 "$MFS_SRC/$f" "$QEMU_SRC/$f"
done

MESON_FILE="$QEMU_SRC/block/meson.build"

python3 - "$MESON_FILE" <<'PY'
import pathlib
import sys

meson = pathlib.Path(sys.argv[1])
text = meson.read_text()
marker = "block_ss.add(files(\n  'mfs.c',\n  'mfs-proto.c',\n  'mfs-conn.c',\n))\n"

if "'mfs.c'" in text and "'mfs-proto.c'" in text and "'mfs-conn.c'" in text:
    sys.exit(0)

if not text.endswith("\n"):
    text += "\n"

text += "\n# MooseFS block driver (qemu-mfs)\n" + marker
meson.write_text(text)
PY

echo "Integrated qemu-mfs into $QEMU_SRC"
