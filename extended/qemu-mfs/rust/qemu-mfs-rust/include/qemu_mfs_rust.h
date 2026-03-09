#ifndef QEMU_MFS_RUST_H
#define QEMU_MFS_RUST_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct QemuMfsRustClientHandle QemuMfsRustClientHandle;
typedef struct QemuMfsRustFileHandle QemuMfsRustFileHandle;

enum {
    QEMU_MFS_RUST_OPEN_CREATE = 1u << 0,
    QEMU_MFS_RUST_OPEN_READ_WRITE = 1u << 1,
};

QemuMfsRustClientHandle *qemu_mfs_rust_client_connect(const char *master_addr,
                                                      const char *subdir,
                                                      const char *password,
                                                      const char *password_md5_hex);
void qemu_mfs_rust_client_destroy(QemuMfsRustClientHandle *handle);
const char *qemu_mfs_rust_client_last_error(const QemuMfsRustClientHandle *handle);

QemuMfsRustFileHandle *qemu_mfs_rust_client_open_file(QemuMfsRustClientHandle *client,
                                                      const char *path,
                                                      uint32_t flags,
                                                      uint64_t size_hint);
void qemu_mfs_rust_file_destroy(QemuMfsRustFileHandle *handle);
int qemu_mfs_rust_file_size(const QemuMfsRustFileHandle *handle, uint64_t *out_size);

int qemu_mfs_rust_client_pread(QemuMfsRustClientHandle *client,
                               const QemuMfsRustFileHandle *file,
                               uint64_t offset,
                               uint8_t *out_data,
                               size_t out_len);
int qemu_mfs_rust_client_pwrite(QemuMfsRustClientHandle *client,
                                const QemuMfsRustFileHandle *file,
                                uint64_t offset,
                                const uint8_t *data,
                                size_t data_len);

#ifdef __cplusplus
}
#endif

#endif

