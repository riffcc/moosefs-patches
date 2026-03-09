#!/usr/bin/env bash
set -euo pipefail

MODULE=/root/moosefs-patches/extended/mfs-kmod/mfs.ko
HELPER=/usr/local/sbin/mfskmod-helper-riff
CTRL=/dev/mfs_ctrl
TARGET=/mnt/proxmox-mfs-kmod
LOG=/var/log/moosefs-kmod/mfskmod-helper-riff.log
PIDFILE=/run/mfskmod-helper-riff.pid
VERSION=2026-03-09-evict-inode-v1
SOURCE=mfsmaster.per.riff.cc:9421/proxmox
OPTS=master=mfsmaster.per.riff.cc,port=9421,subdir=/proxmox,password=untaxed-mortally-ungraded-simple-arrogance-deletion

mkdir -p "$(dirname "$TARGET")" /var/log/moosefs-kmod
umount -l "$TARGET" >/dev/null 2>&1 || true
mkdir -p "$TARGET"

if [ -f "$PIDFILE" ]; then
  old_pid="$(cat "$PIDFILE" 2>/dev/null || true)"
  if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
    kill "$old_pid" 2>/dev/null || true
    sleep 1
    kill -9 "$old_pid" 2>/dev/null || true
  fi
  rm -f "$PIDFILE"
fi

if [ -r /sys/module/mfs/version ] && [ "$(cat /sys/module/mfs/version 2>/dev/null || true)" != "$VERSION" ]; then
  modprobe -r mfs >/dev/null 2>&1 || true
  rm -f "$CTRL"
fi

if [ ! -c "$CTRL" ]; then
  modprobe -r mfs >/dev/null 2>&1 || true
  rm -f "$CTRL"
  insmod "$MODULE"
  sleep 1
fi

nohup "$HELPER" -f -v -d "$CTRL" >"$LOG" 2>&1 </dev/null &
helper_pid=$!
echo "$helper_pid" > "$PIDFILE"
sleep 1
if ! kill -0 "$helper_pid" 2>/dev/null; then
  echo "helper failed to stay up" >&2
  rm -f "$PIDFILE"
  exit 1
fi

mount -i -t mfs "$SOURCE" "$TARGET" -o "$OPTS"
echo "---MODULE VERSION---"
cat /sys/module/mfs/version 2>/dev/null || true
echo "---MOUNT---"
mount | grep " $TARGET " || true
echo "---DF---"
df -h "$TARGET" || true
echo "---ROOT LIST---"
timeout 10 sh -c "ls -lah \"$TARGET\" | sed -n '1,40p'" || true
