ifneq ($(KERNELRELEASE),)
include Kbuild
else

PWD := $(shell pwd)
KDIR ?= /lib/modules/$(shell uname -r)/build

.PHONY: all module clean test

all: module

module:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
	$(MAKE) -C $(PWD) test-clean

test:
	cargo test --manifest-path $(PWD)/Cargo.toml

test-clean:
	cargo clean --manifest-path $(PWD)/Cargo.toml

endif
