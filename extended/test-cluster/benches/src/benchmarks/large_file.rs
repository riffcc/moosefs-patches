use crate::report;
use crate::types::{BenchConfig, BenchResult, Target};
use super::util;

pub fn run(config: &BenchConfig) -> Vec<BenchResult> {
    let total = config.large_file_bytes();
    let total_mb = total / (1024 * 1024);
    report::section(&format!("5. Large File I/O ({total_mb} MB single file)"));

    let mut local_vals = [0f64; 2]; // write, read  (bytes/sec)
    let mut mfs_vals = [0f64; 2];

    for target in [Target::Local, Target::MooseFS] {
        let dir = util::target_dir(config, target, "largefile");
        util::cleanup(&dir);
        let file_path = dir.join("bigfile.dat");

        // Write
        util::drop_caches();
        let write_bps = util::write_zeros(&file_path, total);

        // Read
        util::drop_caches();
        let read_bps = util::read_file(&file_path);

        match target {
            Target::Local => {
                local_vals[0] = write_bps / (1024.0 * 1024.0);
                local_vals[1] = read_bps / (1024.0 * 1024.0);
            }
            Target::MooseFS => {
                mfs_vals[0] = write_bps / (1024.0 * 1024.0);
                mfs_vals[1] = read_bps / (1024.0 * 1024.0);
            }
        }

        util::cleanup(&dir);
    }

    vec![
        BenchResult::new("Large File Write", local_vals[0], mfs_vals[0], "MB/s", true),
        BenchResult::new("Large File Read", local_vals[1], mfs_vals[1], "MB/s", true),
    ]
}
