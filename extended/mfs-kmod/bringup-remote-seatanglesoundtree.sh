#!/usr/bin/env bash
set -euo pipefail

host="${1:-root@10.7.1.122}"

ssh -o BatchMode=yes "$host" 'bash -s' <<'REMOTE'
set -euo pipefail

mkdir -p /var/log/moosefs /mnt/mfs /mnt/mfsdata/cs1 /mnt/mfsdata/cs2 /mnt/mfsdata/cs3
chown mfs:mfs /var/log/moosefs /var/lib/mfs /mnt/mfsdata/cs1 /mnt/mfsdata/cs2 /mnt/mfsdata/cs3

cat >/etc/mfs/mfsmaster.cfg <<'"'"'EOF'"'"'
WORKING_USER = mfs
WORKING_GROUP = mfs
SYSLOG_IDENT = mfsmaster
DATA_PATH = /var/lib/mfs
EXPORTS_FILENAME = /etc/mfs/mfsexports.cfg
BACK_LOGS = 50
METADATA_SAVE_FREQ = 1
MATOCS_LISTEN_PORT = 9420
MATOML_LISTEN_PORT = 9419
MATOCL_LISTEN_PORT = 9421
EOF

cat >/etc/mfs/mfschunkserver.cfg <<'"'"'EOF'"'"'
WORKING_USER = mfs
WORKING_GROUP = mfs
SYSLOG_IDENT = mfschunkserver
LOCK_MEMORY = 0
DATA_PATH = /var/lib/mfs
MASTER_HOST = 10.7.1.122
MASTER_PORT = 9420
CSSERV_LISTEN_PORT = 9422
HDD_CONF_FILENAME = /etc/mfs/mfshdd.cfg
HDD_LEAVE_SPACE_DEFAULT = 1G
EOF

cat >/etc/mfs/mfsexports.cfg <<'"'"'EOF'"'"'
* / rw,alldirs,maproot=0:0
EOF

cat >/etc/mfs/mfshdd.cfg <<'"'"'EOF'"'"'
/mnt/mfsdata/cs1
/mnt/mfsdata/cs2
/mnt/mfsdata/cs3
EOF

pkill -f "/root/moosefs-patches/mfsmaster/mfsmaster" || true
pkill -f "/root/moosefs-patches/mfschunkserver/mfschunkserver" || true
killall -q mfskmod-helper || true
umount /mnt/mfs >/dev/null 2>&1 || umount -l /mnt/mfs >/dev/null 2>&1 || true

modprobe -r mfs >/dev/null 2>&1 || true
rm -f /dev/mfs_ctrl
insmod /root/moosefs-patches/extended/mfs-kmod/mfs.ko
sleep 1

nohup /root/moosefs-patches/mfsmaster/mfsmaster -f -d -c /etc/mfs/mfsmaster.cfg \
	>/var/log/moosefs/mfsmaster.log 2>&1 </dev/null &
sleep 2

nohup /root/moosefs-patches/mfschunkserver/mfschunkserver -f -d -c /etc/mfs/mfschunkserver.cfg \
	>/var/log/moosefs/mfschunkserver.log 2>&1 </dev/null &
sleep 2

nohup /usr/sbin/mfskmod-helper -f -v -d /dev/mfs_ctrl \
	>/tmp/mfskmod-helper.log 2>&1 </dev/null &
sleep 1

MFS_MODULE_PATH=/root/moosefs-patches/extended/mfs-kmod/mfs.ko \
MFS_MODULE_VERSION=2026-03-09-evict-inode-v1 \
mount -i -t mfs none /mnt/mfs -o master=10.7.1.122,port=9421

echo "---PORTS---"
ss -ltnp | egrep ":9419|:9420|:9421|:9422" || true
echo "---MODULE VERSION---"
cat /sys/module/mfs/version 2>/dev/null || true
echo "---MOUNT---"
mount | grep " /mnt/mfs " || true
echo "---DF---"
df -h /mnt/mfs || true
echo "---LS---"
ls -lah /mnt/mfs | sed -n "1,80p" || true
echo "---HELPER LOG---"
sed -n "1,120p" /tmp/mfskmod-helper.log || true
REMOTE
