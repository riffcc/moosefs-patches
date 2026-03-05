use serde::Serialize;
use std::path::PathBuf;

/// Global benchmark configuration
pub struct BenchConfig {
    pub local_dir: PathBuf,
    pub mfs_dir: PathBuf,
    pub quick: bool,
    pub csv_path: PathBuf,
}

impl BenchConfig {
    /// Sizing parameters adjusted for quick mode
    pub fn seq_size_bytes(&self) -> u64 {
        if self.quick { 128 * 1024 * 1024 } else { 512 * 1024 * 1024 }
    }

    pub fn large_file_bytes(&self) -> u64 {
        if self.quick { 256 * 1024 * 1024 } else { 1024 * 1024 * 1024 }
    }

    pub fn small_file_count(&self) -> usize {
        if self.quick { 1_000 } else { 10_000 }
    }

    pub fn fio_runtime_secs(&self) -> u32 {
        if self.quick { 10 } else { 30 }
    }

    pub fn dir_scale_sizes(&self) -> Vec<usize> {
        if self.quick {
            vec![100, 1_000]
        } else {
            vec![100, 1_000, 10_000]
        }
    }

    pub fn concurrent_jobs(&self) -> Vec<usize> {
        if self.quick { vec![4, 8] } else { vec![4, 8, 16] }
    }

    pub fn sqlite_ops(&self) -> usize {
        if self.quick { 2_000 } else { 10_000 }
    }

    pub fn pgbench_scale(&self) -> u32 {
        if self.quick { 5 } else { 10 }
    }

    pub fn pgbench_time_secs(&self) -> u32 {
        if self.quick { 20 } else { 60 }
    }

    pub fn etcd_ops(&self) -> usize {
        if self.quick { 1_000 } else { 5_000 }
    }
}

/// A single benchmark result: local value vs MooseFS value
#[derive(Debug, Clone, Serialize)]
pub struct BenchResult {
    pub name: String,
    pub local_value: f64,
    pub mfs_value: f64,
    pub unit: String,
    /// Higher is better? (true = throughput/IOPS, false = latency)
    pub higher_is_better: bool,
}

impl BenchResult {
    pub fn new(name: &str, local: f64, mfs: f64, unit: &str, higher_is_better: bool) -> Self {
        Self {
            name: name.to_string(),
            local_value: local,
            mfs_value: mfs,
            unit: unit.to_string(),
            higher_is_better,
        }
    }

    pub fn ratio(&self) -> Option<f64> {
        if self.local_value > 0.0 {
            Some(self.mfs_value / self.local_value)
        } else {
            None
        }
    }
}

/// Which target are we benchmarking?
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Target {
    Local,
    MooseFS,
}

impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Target::Local => write!(f, "local"),
            Target::MooseFS => write!(f, "moosefs"),
        }
    }
}
