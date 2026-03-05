use crate::report;
use crate::types::{BenchConfig, BenchResult, Target};
use super::util;

use std::fs;
use std::time::Instant;

pub fn run(config: &BenchConfig) -> Vec<BenchResult> {
    let sizes = config.dir_scale_sizes();
    report::section(&format!("8. Directory Scalability (readdir at {:?} entries)", sizes));

    let mut results = Vec::new();

    for &n in &sizes {
        let mut local_time = 0f64;
        let mut mfs_time = 0f64;

        for target in [Target::Local, Target::MooseFS] {
            let dir = util::target_dir(config, target, &format!("dirscale-{n}"));
            util::cleanup(&dir);

            // Populate directory
            for i in 0..n {
                let p = dir.join(format!("entry_{i:08}"));
                fs::File::create(&p).ok();
            }

            // Time readdir
            util::drop_caches();
            let start = Instant::now();
            let entries: Vec<_> = fs::read_dir(&dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            let elapsed = start.elapsed().as_secs_f64();
            let _ = entries.len(); // force iteration

            match target {
                Target::Local => local_time = elapsed * 1000.0, // ms
                Target::MooseFS => mfs_time = elapsed * 1000.0,
            }

            util::cleanup(&dir);
        }

        results.push(BenchResult::new(
            &format!("Readdir {n} entries"),
            local_time, mfs_time, "ms", false,
        ));
    }

    results
}
