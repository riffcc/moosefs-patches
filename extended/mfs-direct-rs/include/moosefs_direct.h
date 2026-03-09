#ifndef MOOSEFS_DIRECT_H
#define MOOSEFS_DIRECT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct MfsClientHandle MfsClientHandle;
typedef struct MfsOpenFileHandle MfsOpenFileHandle;

MfsClientHandle *mfs_client_connect(const char *master_addr,
                                    const char *subdir,
                                    const char *password);

void mfs_client_destroy(MfsClientHandle *handle);
void mfs_file_destroy(MfsOpenFileHandle *handle);

const char *mfs_client_last_error(const MfsClientHandle *handle);

MfsOpenFileHandle *mfs_client_open_file(MfsClientHandle *handle,
                                        const char *path);
MfsOpenFileHandle *mfs_client_ensure_file_len(MfsClientHandle *handle,
                                              const char *path,
                                              uint64_t size);

int mfs_client_write_all(MfsClientHandle *handle,
                         const char *path,
                         const uint8_t *data,
                         size_t data_len);

int mfs_file_size(const MfsOpenFileHandle *handle, uint64_t *out_size);

int mfs_client_pread(MfsClientHandle *client,
                     const MfsOpenFileHandle *file,
                     uint64_t offset,
                     uint8_t *out_data,
                     size_t out_len);

int mfs_client_pwrite(MfsClientHandle *client,
                      const MfsOpenFileHandle *file,
                      uint64_t offset,
                      const uint8_t *data,
                      size_t data_len);

int mfs_client_mkdir_all(MfsClientHandle *handle, const char *path);

int mfs_client_read_all(MfsClientHandle *handle,
                        const char *path,
                        uint8_t **out_data,
                        size_t *out_len);

int mfs_client_exists(MfsClientHandle *handle,
                      const char *path,
                      int *out_exists);

int mfs_client_file_size(MfsClientHandle *handle,
                         const char *path,
                         int *out_exists,
                         size_t *out_size);

int mfs_client_unlink(MfsClientHandle *handle, const char *path);
int mfs_client_rmdir(MfsClientHandle *handle, const char *path);
int mfs_client_rename(MfsClientHandle *handle,
                      const char *old_path,
                      const char *new_path);

void mfs_client_free_buffer(uint8_t *data, size_t len);

#ifdef __cplusplus
}
#endif

#endif
