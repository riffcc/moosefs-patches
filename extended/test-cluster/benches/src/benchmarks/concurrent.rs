use crate::report;
use crate::types::{BenchConfig, BenchResult, Target};
use super::util;

pub fn run(config: &BenchConfig) -> Vec<BenchResult> {
    let job_counts = config.concurrent_jobs();
    report::section(&format!("9. Concurrent I/O (random 4K, iodepth=8, numjobs={:?})", job_counts));

    let runtime = config.fio_runtime_secs();
    let size = format!("{}M", 64); // 64M per job
    let mut results = Vec::new();

    for &jobs in &job_counts {
        let mut local_iops = 0f64;
        let mut mfs_iops = 0f64;

        for target in [Target::Local, Target::MooseFS] {
            let dir = util::target_dir(config, target, &format!("concurrent-{jobs}"));
            util::cleanup(&dir);
            let dir_str = dir.to_str().unwrap();

            util::drop_caches();
            if let Some(json) = util::run_fio(&[
                &format!("--name=concurrent-{jobs}j"),
                &format!("--directory={dir_str}"),
                "--rw=randrw", "--rwmixread=50", "--bs=4k",
                &format!("--size={size}"),
                &format!("--numjobs={jobs}"),
                "--ioengine=posixaio", "--direct=0", "--iodepth=8",
                &format!("--runtime={runtime}"), "--time_based", "--group_reporting",
            ]) {
                let r_iops = util::fio_value(&json, "jobs.0.read.iops");
                let w_iops = util::fio_value(&json, "jobs.0.write.iops");
                let total = r_iops + w_iops;

                match target {
                    Target::Local => local_iops = total,
                    Target::MooseFS => mfs_iops = total,
                }
            }

            util::cleanup(&dir);
        }

        results.push(BenchResult::new(
            &format!("Concurrent {jobs}j Total IOPS"),
            local_iops, mfs_iops, "IOPS", true,
        ));
    }

    results
}
