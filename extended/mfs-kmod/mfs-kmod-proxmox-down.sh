#!/usr/bin/env bash
set -euo pipefail

TARGET=/mnt/proxmox-mfs-kmod
PIDFILE=/run/mfskmod-helper-riff.pid

if mountpoint -q "$TARGET"; then
  umount -l "$TARGET" >/dev/null 2>&1 || true
fi

if [ -f "$PIDFILE" ]; then
  helper_pid="$(cat "$PIDFILE" 2>/dev/null || true)"
  if [ -n "$helper_pid" ] && kill -0 "$helper_pid" 2>/dev/null; then
    kill "$helper_pid" 2>/dev/null || true
    for _ in 1 2 3 4 5; do
      if ! kill -0 "$helper_pid" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    if kill -0 "$helper_pid" 2>/dev/null; then
      kill -9 "$helper_pid" 2>/dev/null || true
    fi
  fi
  rm -f "$PIDFILE"
fi

pkill -x mfskmod-helper-riff >/dev/null 2>&1 || true
