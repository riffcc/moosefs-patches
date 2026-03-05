use crate::report;
use crate::types::{BenchConfig, BenchResult, Target};
use super::util;

pub fn run(config: &BenchConfig) -> Vec<BenchResult> {
    report::section("7. Fsync-Heavy Writes (4K sequential, fsync after each write)");

    let runtime = config.fio_runtime_secs();
    let size = format!("{}M", config.seq_size_bytes() / (1024 * 1024));
    let mut local_vals = [0f64; 2]; // iops, lat
    let mut mfs_vals = [0f64; 2];

    for target in [Target::Local, Target::MooseFS] {
        let dir = util::target_dir(config, target, "fsync");
        util::cleanup(&dir);
        let dir_str = dir.to_str().unwrap();

        util::drop_caches();
        if let Some(json) = util::run_fio(&[
            "--name=fsync-write", &format!("--directory={dir_str}"),
            "--rw=write", "--bs=4k", &format!("--size={size}"),
            "--numjobs=1", "--ioengine=sync", "--fsync=1",
            &format!("--runtime={runtime}"), "--time_based", "--group_reporting",
        ]) {
            let iops = util::fio_value(&json, "jobs.0.write.iops");
            let lat = util::fio_value(&json, "jobs.0.write.lat_ns.mean") / 1000.0;

            match target {
                Target::Local => { local_vals[0] = iops; local_vals[1] = lat; }
                Target::MooseFS => { mfs_vals[0] = iops; mfs_vals[1] = lat; }
            }
        }

        util::cleanup(&dir);
    }

    vec![
        BenchResult::new("Fsync Write IOPS", local_vals[0], mfs_vals[0], "IOPS", true),
        BenchResult::new("Fsync Write Lat", local_vals[1], mfs_vals[1], "µs", false),
    ]
}
