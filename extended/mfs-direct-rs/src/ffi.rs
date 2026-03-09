use std::ffi::{c_char, c_int, c_uchar, CStr, CString};
use std::net::TcpStream;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;

use crate::{Client, ConnectOptions, Error};

pub struct MfsClientHandle {
    client: Client<TcpStream>,
    last_error: Option<CString>,
}

pub struct MfsOpenFileHandle {
    file: crate::OpenFile,
}

#[repr(C)]
pub struct MfsDirEntry {
    pub name: *mut c_char,
    pub inode: u32,
    pub file_type: u8,
    pub _reserved: [u8; 3],
    pub size: u64,
}

impl MfsClientHandle {
    fn new(client: Client<TcpStream>) -> Self {
        Self {
            client,
            last_error: None,
        }
    }

    fn set_error_message(&mut self, message: String) -> c_int {
        self.last_error = Some(sanitize_cstring(message));
        -1
    }

    fn set_error(&mut self, error: Error) -> c_int {
        self.last_error = Some(sanitize_cstring(error.to_string()));
        -error.errno_like()
    }

    fn clear_error(&mut self) {
        self.last_error = None;
    }
}

fn sanitize_cstring(message: String) -> CString {
    let bytes: Vec<u8> = message.into_bytes().into_iter().filter(|b| *b != 0).collect();
    CString::new(bytes).expect("interior nul bytes removed")
}

fn cstr_arg<'a>(ptr: *const c_char, name: &str) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err(format!("{name} must not be null"));
    }
    let cstr = unsafe { CStr::from_ptr(ptr) };
    cstr.to_str()
        .map_err(|_| format!("{name} must be valid UTF-8"))
}

fn optional_cstr_arg<'a>(ptr: *const c_char) -> Result<Option<&'a str>, String> {
    if ptr.is_null() {
        return Ok(None);
    }
    let cstr = unsafe { CStr::from_ptr(ptr) };
    cstr.to_str()
        .map(Some)
        .map_err(|_| "password must be valid UTF-8".to_string())
}

fn ffi_guard(default: c_int, f: impl FnOnce() -> c_int) -> c_int {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(code) => code,
        Err(_) => default,
    }
}

#[no_mangle]
pub extern "C" fn mfs_client_connect(
    master_addr: *const c_char,
    subdir: *const c_char,
    password: *const c_char,
) -> *mut MfsClientHandle {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let master_addr = cstr_arg(master_addr, "master_addr").ok()?;
        let subdir = cstr_arg(subdir, "subdir").ok()?;
        let password = optional_cstr_arg(password).ok().flatten();

        let mut options = ConnectOptions::default();
        options.subdir = subdir.to_string();
        if let Some(password) = password {
            options = options.with_password(password);
        }

        let client = Client::<TcpStream>::connect_registered(master_addr, options).ok()?;
        Some(Box::into_raw(Box::new(MfsClientHandle::new(client))))
    }));

    match result {
        Ok(Some(handle)) => handle,
        _ => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn mfs_client_destroy(handle: *mut MfsClientHandle) {
    if handle.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(handle) };
}

#[no_mangle]
pub extern "C" fn mfs_file_destroy(handle: *mut MfsOpenFileHandle) {
    if handle.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(handle) };
}

#[no_mangle]
pub extern "C" fn mfs_dir_entries_free(
    entries: *mut MfsDirEntry,
    count: usize,
) {
    if entries.is_null() {
        return;
    }
    let mut entries = unsafe { Vec::from_raw_parts(entries, count, count) };
    for entry in &mut entries {
        if !entry.name.is_null() {
            let _ = unsafe { CString::from_raw(entry.name) };
            entry.name = ptr::null_mut();
        }
    }
}

#[no_mangle]
pub extern "C" fn mfs_client_last_error(handle: *const MfsClientHandle) -> *const c_char {
    if handle.is_null() {
        return ptr::null();
    }
    let handle = unsafe { &*handle };
    handle
        .last_error
        .as_ref()
        .map_or(ptr::null(), |msg| msg.as_ptr())
}

#[no_mangle]
pub extern "C" fn mfs_client_list_dir(
    handle: *mut MfsClientHandle,
    path: *const c_char,
    out_entries: *mut *mut MfsDirEntry,
    out_count: *mut usize,
) -> c_int {
    ffi_guard(-71, || {
        if handle.is_null() || out_entries.is_null() || out_count.is_null() {
            return -22;
        }
        let handle = unsafe { &mut *handle };
        let path = match cstr_arg(path, "path") {
            Ok(path) => path,
            Err(err) => return handle.set_error_message(err),
        };

        match handle.client.list_dir(path) {
            Ok(entries) => {
                let mut out = Vec::with_capacity(entries.len());
                for entry in entries {
                    let name = sanitize_cstring(entry.name);
                    out.push(MfsDirEntry {
                        name: name.into_raw(),
                        inode: entry.inode,
                        file_type: entry.file_type,
                        _reserved: [0; 3],
                        size: entry.size,
                    });
                }
                let mut boxed = out.into_boxed_slice();
                let count = boxed.len();
                let ptr = boxed.as_mut_ptr();
                std::mem::forget(boxed);
                unsafe {
                    *out_entries = ptr;
                    *out_count = count;
                }
                handle.clear_error();
                0
            }
            Err(err) => handle.set_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn mfs_client_open_file(
    handle: *mut MfsClientHandle,
    path: *const c_char,
) -> *mut MfsOpenFileHandle {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() {
            return None;
        }
        let handle = unsafe { &mut *handle };
        let path = match cstr_arg(path, "path") {
            Ok(path) => path,
            Err(err) => {
                handle.set_error_message(err);
                return None;
            }
        };

        match handle.client.open_file(path) {
            Ok(file) => {
                handle.clear_error();
                Some(Box::into_raw(Box::new(MfsOpenFileHandle { file })))
            }
            Err(err) => {
                handle.set_error(err);
                None
            }
        }
    }));

    match result {
        Ok(Some(file)) => file,
        _ => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn mfs_client_ensure_file_len(
    handle: *mut MfsClientHandle,
    path: *const c_char,
    size: u64,
) -> *mut MfsOpenFileHandle {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() {
            return None;
        }
        let handle = unsafe { &mut *handle };
        let path = match cstr_arg(path, "path") {
            Ok(path) => path,
            Err(err) => {
                handle.set_error_message(err);
                return None;
            }
        };

        match handle.client.ensure_file_len(path, size) {
            Ok(file) => {
                handle.clear_error();
                Some(Box::into_raw(Box::new(MfsOpenFileHandle { file })))
            }
            Err(err) => {
                handle.set_error(err);
                None
            }
        }
    }));

    match result {
        Ok(Some(file)) => file,
        _ => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn mfs_client_write_all(
    handle: *mut MfsClientHandle,
    path: *const c_char,
    data: *const c_uchar,
    data_len: usize,
) -> c_int {
    ffi_guard(-71, || {
        if handle.is_null() {
            return -22;
        }
        let handle = unsafe { &mut *handle };
        let path = match cstr_arg(path, "path") {
            Ok(path) => path,
            Err(err) => return handle.set_error_message(err),
        };
        let bytes = if data_len == 0 {
            &[]
        } else if data.is_null() {
            return handle.set_error_message("data must not be null when data_len > 0".to_string());
        } else {
            unsafe { slice::from_raw_parts(data, data_len) }
        };

        match handle.client.write_all(path, bytes) {
            Ok(()) => {
                handle.clear_error();
                0
            }
            Err(err) => handle.set_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn mfs_file_size(handle: *const MfsOpenFileHandle, out_size: *mut u64) -> c_int {
    ffi_guard(-71, || {
        if handle.is_null() || out_size.is_null() {
            return -22;
        }
        let handle = unsafe { &*handle };
        unsafe {
            *out_size = handle.file.size;
        }
        0
    })
}

#[no_mangle]
pub extern "C" fn mfs_client_pread(
    client: *mut MfsClientHandle,
    file: *const MfsOpenFileHandle,
    offset: u64,
    out_data: *mut c_uchar,
    out_len: usize,
) -> c_int {
    ffi_guard(-71, || {
        if client.is_null() {
            return -22;
        }
        let client = unsafe { &mut *client };
        if file.is_null() {
            return client.set_error_message("file handle must not be null".to_string());
        }
        let file = unsafe { &*file };
        let out = if out_len == 0 {
            &mut []
        } else if out_data.is_null() {
            return client
                .set_error_message("out_data must not be null when out_len > 0".to_string());
        } else {
            unsafe { slice::from_raw_parts_mut(out_data, out_len) }
        };

        match client.client.read_at(&file.file, offset, out) {
            Ok(()) => {
                client.clear_error();
                0
            }
            Err(err) => client.set_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn mfs_client_pwrite(
    client: *mut MfsClientHandle,
    file: *const MfsOpenFileHandle,
    offset: u64,
    data: *const c_uchar,
    data_len: usize,
) -> c_int {
    ffi_guard(-71, || {
        if client.is_null() {
            return -22;
        }
        let client = unsafe { &mut *client };
        if file.is_null() {
            return client.set_error_message("file handle must not be null".to_string());
        }
        let file = unsafe { &*file };
        let bytes = if data_len == 0 {
            &[]
        } else if data.is_null() {
            return client.set_error_message("data must not be null when data_len > 0".to_string());
        } else {
            unsafe { slice::from_raw_parts(data, data_len) }
        };

        match client.client.write_at(&file.file, offset, bytes) {
            Ok(()) => {
                client.clear_error();
                0
            }
            Err(err) => client.set_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn mfs_client_mkdir_all(handle: *mut MfsClientHandle, path: *const c_char) -> c_int {
    ffi_guard(-71, || {
        if handle.is_null() {
            return -22;
        }
        let handle = unsafe { &mut *handle };
        let path = match cstr_arg(path, "path") {
            Ok(path) => path,
            Err(err) => return handle.set_error_message(err),
        };

        match handle.client.ensure_dir_all(path) {
            Ok(_) => {
                handle.clear_error();
                0
            }
            Err(err) => handle.set_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn mfs_client_read_all(
    handle: *mut MfsClientHandle,
    path: *const c_char,
    out_data: *mut *mut c_uchar,
    out_len: *mut usize,
) -> c_int {
    ffi_guard(-71, || {
        if handle.is_null() || out_data.is_null() || out_len.is_null() {
            return -22;
        }
        let handle = unsafe { &mut *handle };
        let path = match cstr_arg(path, "path") {
            Ok(path) => path,
            Err(err) => return handle.set_error_message(err),
        };

        match handle.client.read_all(path) {
            Ok(data) => {
                let mut boxed = data.into_boxed_slice();
                let len = boxed.len();
                let ptr = boxed.as_mut_ptr();
                std::mem::forget(boxed);
                unsafe {
                    *out_data = ptr;
                    *out_len = len;
                }
                handle.clear_error();
                0
            }
            Err(err) => handle.set_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn mfs_client_exists(
    handle: *mut MfsClientHandle,
    path: *const c_char,
    out_exists: *mut c_int,
) -> c_int {
    ffi_guard(-71, || {
        if handle.is_null() || out_exists.is_null() {
            return -22;
        }
        let handle = unsafe { &mut *handle };
        let path = match cstr_arg(path, "path") {
            Ok(path) => path,
            Err(err) => return handle.set_error_message(err),
        };

        match handle.client.path_exists(path) {
            Ok(exists) => {
                unsafe {
                    *out_exists = if exists { 1 } else { 0 };
                }
                handle.clear_error();
                0
            }
            Err(err) => handle.set_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn mfs_client_file_size(
    handle: *mut MfsClientHandle,
    path: *const c_char,
    out_exists: *mut c_int,
    out_size: *mut usize,
) -> c_int {
    ffi_guard(-71, || {
        if handle.is_null() || out_exists.is_null() || out_size.is_null() {
            return -22;
        }
        let handle = unsafe { &mut *handle };
        let path = match cstr_arg(path, "path") {
            Ok(path) => path,
            Err(err) => return handle.set_error_message(err),
        };

        match handle.client.stat_path(path) {
            Ok(Some((_, size, _))) => {
                unsafe {
                    *out_exists = 1;
                    *out_size = size as usize;
                }
                handle.clear_error();
                0
            }
            Ok(None) => {
                unsafe {
                    *out_exists = 0;
                    *out_size = 0;
                }
                handle.clear_error();
                0
            }
            Err(err) => handle.set_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn mfs_client_unlink(handle: *mut MfsClientHandle, path: *const c_char) -> c_int {
    ffi_guard(-71, || {
        if handle.is_null() {
            return -22;
        }
        let handle = unsafe { &mut *handle };
        let path = match cstr_arg(path, "path") {
            Ok(path) => path,
            Err(err) => return handle.set_error_message(err),
        };

        match handle.client.unlink_path(path) {
            Ok(()) => {
                handle.clear_error();
                0
            }
            Err(err) => handle.set_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn mfs_client_rmdir(handle: *mut MfsClientHandle, path: *const c_char) -> c_int {
    ffi_guard(-71, || {
        if handle.is_null() {
            return -22;
        }
        let handle = unsafe { &mut *handle };
        let path = match cstr_arg(path, "path") {
            Ok(path) => path,
            Err(err) => return handle.set_error_message(err),
        };

        match handle.client.remove_dir_path(path) {
            Ok(()) => {
                handle.clear_error();
                0
            }
            Err(err) => handle.set_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn mfs_client_rename(
    handle: *mut MfsClientHandle,
    old_path: *const c_char,
    new_path: *const c_char,
) -> c_int {
    ffi_guard(-71, || {
        if handle.is_null() {
            return -22;
        }
        let handle = unsafe { &mut *handle };
        let old_path = match cstr_arg(old_path, "old_path") {
            Ok(path) => path,
            Err(err) => return handle.set_error_message(err),
        };
        let new_path = match cstr_arg(new_path, "new_path") {
            Ok(path) => path,
            Err(err) => return handle.set_error_message(err),
        };

        match handle.client.rename_path(old_path, new_path) {
            Ok(()) => {
                handle.clear_error();
                0
            }
            Err(err) => handle.set_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn mfs_client_free_buffer(data: *mut c_uchar, len: usize) {
    if data.is_null() {
        return;
    }
    let _ = unsafe { Vec::from_raw_parts(data, len, len) };
}

#[cfg(test)]
mod tests {
    use super::sanitize_cstring;

    #[test]
    fn sanitize_removes_nuls() {
        let c = sanitize_cstring("a\0b".to_string());
        assert_eq!(c.to_str().unwrap(), "ab");
    }
}
