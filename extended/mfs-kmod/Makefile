ifneq ($(KERNELRELEASE),)
include Kbuild
else

PWD := $(shell pwd)
KDIR ?= /lib/modules/$(shell uname -r)/build
CC ?= cc
CFLAGS_HELPER ?= -O2 -Wall -Wextra -Wpedantic -std=gnu11 -pthread

.PHONY: all module helper clean install

all: module helper

module:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

helper: mfskmod-helper

mfskmod-helper: mfskmod-helper.c mfs_ctrl_proto.h ../../mfscommon/md5.c ../../mfscommon/md5.h
	$(CC) $(CFLAGS_HELPER) -I../../mfscommon -o $@ mfskmod-helper.c ../../mfscommon/md5.c

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
	$(RM) mfskmod-helper

install: all
	install -Dm0755 mfskmod-helper /usr/sbin/mfskmod-helper
	install -Dm0755 mount.mfs /sbin/mount.mfs
	install -Dm0644 mfs.ko /lib/modules/$(shell uname -r)/extra/mfs.ko

endif
