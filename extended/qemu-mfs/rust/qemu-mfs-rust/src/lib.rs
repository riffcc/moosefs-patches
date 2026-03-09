use std::ffi::{c_char, c_int, c_uchar, CStr, CString};
use std::net::TcpStream;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;

use moosefs_direct::{Client, ConnectOptions, Error, OpenFile};

pub struct QemuMfsRustClientHandle {
    client: Client<TcpStream>,
    last_error: Option<CString>,
}

pub struct QemuMfsRustFileHandle {
    file: OpenFile,
}

impl QemuMfsRustClientHandle {
    fn new(client: Client<TcpStream>) -> Self {
        Self {
            client,
            last_error: None,
        }
    }

    fn set_error_message(&mut self, message: impl Into<String>) -> c_int {
        self.last_error = Some(sanitize_cstring(message.into()));
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

fn ffi_guard(default: c_int, f: impl FnOnce() -> c_int) -> c_int {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(code) => code,
        Err(_) => default,
    }
}

fn cstr_arg<'a>(ptr: *const c_char, name: &str) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err(format!("{name} must not be null"));
    }
    let cstr = unsafe { CStr::from_ptr(ptr) };
    cstr.to_str()
        .map_err(|_| format!("{name} must be valid UTF-8"))
}

fn optional_cstr_arg<'a>(ptr: *const c_char, name: &str) -> Result<Option<&'a str>, String> {
    if ptr.is_null() {
        return Ok(None);
    }
    let cstr = unsafe { CStr::from_ptr(ptr) };
    cstr.to_str()
        .map(Some)
        .map_err(|_| format!("{name} must be valid UTF-8"))
}

fn parse_md5_hex(hex: &str) -> Result<[u8; 16], String> {
    if hex.len() != 32 {
        return Err("password_md5_hex must be exactly 32 hex characters".to_string());
    }

    let mut out = [0u8; 16];
    for (index, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(chunk).map_err(|_| "password_md5_hex must be ASCII hex".to_string())?;
        out[index] = u8::from_str_radix(pair, 16)
            .map_err(|_| "password_md5_hex contains non-hex characters".to_string())?;
    }
    Ok(out)
}

fn open_with_flags(
    client: &mut Client<TcpStream>,
    path: &str,
    flags: u32,
    size_hint: u64,
) -> Result<OpenFile, Error> {
    if flags & QEMU_MFS_RUST_OPEN_CREATE != 0 {
        client.ensure_file_len(path, size_hint)
    } else {
        client.open_file(path)
    }
}

const QEMU_MFS_RUST_OPEN_CREATE: u32 = 1u32 << 0;
#[allow(dead_code)]
const QEMU_MFS_RUST_OPEN_READ_WRITE: u32 = 1u32 << 1;

#[no_mangle]
pub extern "C" fn qemu_mfs_rust_client_connect(
    master_addr: *const c_char,
    subdir: *const c_char,
    password: *const c_char,
    password_md5_hex: *const c_char,
) -> *mut QemuMfsRustClientHandle {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let master_addr = cstr_arg(master_addr, "master_addr").ok()?;
        let subdir = cstr_arg(subdir, "subdir").ok()?;
        let password = optional_cstr_arg(password, "password").ok().flatten();
        let password_md5_hex = optional_cstr_arg(password_md5_hex, "password_md5_hex").ok().flatten();

        if password.is_some() && password_md5_hex.is_some() {
            return None;
        }

        let mut options = ConnectOptions::default().with_subdir(subdir);
        if let Some(password) = password {
            options = options.with_password(password);
        } else if let Some(password_md5_hex) = password_md5_hex {
            options.password_md5 = Some(parse_md5_hex(password_md5_hex).ok()?);
        }

        let client = Client::<TcpStream>::connect_registered(master_addr, options).ok()?;
        Some(Box::into_raw(Box::new(QemuMfsRustClientHandle::new(client))))
    }));

    match result {
        Ok(Some(handle)) => handle,
        _ => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn qemu_mfs_rust_client_destroy(handle: *mut QemuMfsRustClientHandle) {
    if handle.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(handle) };
}

#[no_mangle]
pub extern "C" fn qemu_mfs_rust_client_last_error(
    handle: *const QemuMfsRustClientHandle,
) -> *const c_char {
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
pub extern "C" fn qemu_mfs_rust_client_open_file(
    client: *mut QemuMfsRustClientHandle,
    path: *const c_char,
    flags: u32,
    size_hint: u64,
) -> *mut QemuMfsRustFileHandle {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if client.is_null() {
            return None;
        }
        let client = unsafe { &mut *client };
        let path = match cstr_arg(path, "path") {
            Ok(path) => path,
            Err(err) => {
                client.set_error_message(err);
                return None;
            }
        };

        match open_with_flags(&mut client.client, path, flags, size_hint) {
            Ok(file) => {
                client.clear_error();
                Some(Box::into_raw(Box::new(QemuMfsRustFileHandle { file })))
            }
            Err(err) => {
                client.set_error(err);
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
pub extern "C" fn qemu_mfs_rust_file_destroy(handle: *mut QemuMfsRustFileHandle) {
    if handle.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(handle) };
}

#[no_mangle]
pub extern "C" fn qemu_mfs_rust_file_size(
    handle: *const QemuMfsRustFileHandle,
    out_size: *mut u64,
) -> c_int {
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
pub extern "C" fn qemu_mfs_rust_client_pread(
    client: *mut QemuMfsRustClientHandle,
    file: *const QemuMfsRustFileHandle,
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
            return client.set_error_message("file handle must not be null");
        }
        let file = unsafe { &*file };
        let out = if out_len == 0 {
            &mut []
        } else if out_data.is_null() {
            return client.set_error_message("out_data must not be null when out_len > 0");
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
pub extern "C" fn qemu_mfs_rust_client_pwrite(
    client: *mut QemuMfsRustClientHandle,
    file: *const QemuMfsRustFileHandle,
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
            return client.set_error_message("file handle must not be null");
        }
        let file = unsafe { &*file };
        let bytes = if data_len == 0 {
            &[]
        } else if data.is_null() {
            return client.set_error_message("data must not be null when data_len > 0");
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

#[cfg(test)]
mod tests {
    use super::parse_md5_hex;

    #[test]
    fn parses_lowercase_md5() {
        assert_eq!(
            parse_md5_hex("0123456789abcdef0123456789abcdef").unwrap(),
            [
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
            ]
        );
    }

    #[test]
    fn rejects_bad_md5_length() {
        assert!(parse_md5_hex("abcd").is_err());
    }
}

