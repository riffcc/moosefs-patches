use crate::types::{BenchConfig, Target};
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

/// Get the directory for a given target
pub fn target_dir(config: &BenchConfig, target: Target, sub: &str) -> PathBuf {
    let base = match target {
        Target::Local => &config.local_dir,
        Target::MooseFS => &config.mfs_dir,
    };
    let dir = base.join(sub);
    fs::create_dir_all(&dir).ok();
    dir
}

/// Clean up a directory's contents
pub fn cleanup(dir: &Path) {
    if dir.exists() {
        fs::remove_dir_all(dir).ok();
        fs::create_dir_all(dir).ok();
    }
}

/// Drop kernel page caches (best effort)
pub fn drop_caches() {
    // sync first
    unsafe { libc::sync(); }
    fs::write("/proc/sys/vm/drop_caches", "3").ok();
}

/// Run fio with JSON output and return the parsed JSON
pub fn run_fio(args: &[&str]) -> Option<serde_json::Value> {
    let output = Command::new("fio")
        .args(args)
        .arg("--output-format=json")
        .output()
        .ok()?;

    if !output.status.success() {
        eprintln!("  fio failed: {}", String::from_utf8_lossy(&output.stderr));
        return None;
    }

    serde_json::from_slice(&output.stdout).ok()
}

/// Extract a nested value from fio JSON
/// e.g., fio_value(&json, "jobs.0.write.bw_bytes")
pub fn fio_value(json: &serde_json::Value, path: &str) -> f64 {
    let mut v = json;
    for key in path.split('.') {
        if let Ok(idx) = key.parse::<usize>() {
            v = &v[idx];
        } else {
            v = &v[key];
        }
    }
    v.as_f64().unwrap_or(0.0)
}

/// Measure how long a closure takes, returning (duration, result)
pub fn timed<F, R>(f: F) -> (Duration, R)
where
    F: FnOnce() -> R,
{
    let start = Instant::now();
    let result = f();
    (start.elapsed(), result)
}

/// Write `count` bytes of zeros to a file, returning bytes/sec
pub fn write_zeros(path: &Path, total_bytes: u64) -> f64 {
    let chunk = vec![0u8; 1024 * 1024]; // 1MB chunks
    let mut remaining = total_bytes;
    let start = Instant::now();

    let mut f = fs::File::create(path).expect("create file");
    while remaining > 0 {
        let n = remaining.min(chunk.len() as u64) as usize;
        f.write_all(&chunk[..n]).expect("write");
        remaining -= n as u64;
    }
    f.sync_all().expect("sync");

    let elapsed = start.elapsed().as_secs_f64();
    total_bytes as f64 / elapsed
}

/// Read an entire file, returning bytes/sec
pub fn read_file(path: &Path) -> f64 {
    let meta = fs::metadata(path).expect("stat");
    let total = meta.len();
    let mut buf = vec![0u8; 1024 * 1024];
    let start = Instant::now();

    let mut f = fs::File::open(path).expect("open");
    loop {
        match f.read(&mut buf) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => panic!("read error: {e}"),
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    total as f64 / elapsed
}
