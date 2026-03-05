# QEMU block driver integration fragment for MooseFS protocol driver.
#
# Usage from QEMU's block/meson.build:
#   subdir('../extended/qemu-mfs')
#
# This fragment expects `block_ss` to exist in the parent scope.

block_ss.add(files(
  'block/mfs.c',
  'block/mfs-proto.c',
  'block/mfs-conn.c',
))
