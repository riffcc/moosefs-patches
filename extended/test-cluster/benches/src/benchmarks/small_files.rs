use crate::report;
use crate::types::{BenchConfig, BenchResult, Target};
use super::util;

use std::fs;
use std::io::Write;
use std::time::Instant;

pub fn run(config: &BenchConfig) -> Vec<BenchResult> {
    let count = config.small_file_count();
    let file_size = 4096; // 4K files
    report::section(&format!("4. Small Files ({count} × 4K)"));

    let data = vec![0xABu8; file_size];
    let mut local_vals = [0f64; 3]; // create, read, delete
    let mut mfs_vals = [0f64; 3];

    for target in [Target::Local, Target::MooseFS] {
        let dir = util::target_dir(config, target, "smallfiles");
        util::cleanup(&dir);

        // Create
        let start = Instant::now();
        for i in 0..count {
            let p = dir.join(format!("sf{i}"));
            let mut f = fs::File::create(&p).unwrap();
            f.write_all(&data).ok();
        }
        let create_rate = count as f64 / start.elapsed().as_secs_f64();

        // Read
        util::drop_caches();
        let start = Instant::now();
        for i in 0..count {
            let p = dir.join(format!("sf{i}"));
            fs::read(&p).ok();
        }
        let read_rate = count as f64 / start.elapsed().as_secs_f64();

        // Delete
        let start = Instant::now();
        for i in 0..count {
            let p = dir.join(format!("sf{i}"));
            fs::remove_file(&p).ok();
        }
        let delete_rate = count as f64 / start.elapsed().as_secs_f64();

        match target {
            Target::Local => {
                local_vals[0] = create_rate;
                local_vals[1] = read_rate;
                local_vals[2] = delete_rate;
            }
            Target::MooseFS => {
                mfs_vals[0] = create_rate;
                mfs_vals[1] = read_rate;
                mfs_vals[2] = delete_rate;
            }
        }

        util::cleanup(&dir);
    }

    vec![
        BenchResult::new("Small File Create", local_vals[0], mfs_vals[0], "files/s", true),
        BenchResult::new("Small File Read", local_vals[1], mfs_vals[1], "files/s", true),
        BenchResult::new("Small File Delete", local_vals[2], mfs_vals[2], "files/s", true),
    ]
}
