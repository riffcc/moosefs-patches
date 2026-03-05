ifneq ($(KERNELRELEASE),)
obj-m += mfs_proto.o
mfs_proto-y := mfs_proto_encode.o mfs_proto_decode.o mfs_proto_cs.o mfs_proto_errno.o
else
KDIR ?= /lib/modules/$(shell uname -r)/build
PWD := $(shell pwd)

all:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
endif
